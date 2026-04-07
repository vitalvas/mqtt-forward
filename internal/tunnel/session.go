package tunnel

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrSessionNotFound  = errors.New("session not found")
	ErrSessionExists    = errors.New("session already exists")
	ErrReorderTimeout   = errors.New("reorder buffer timeout")
	ErrBufferFull       = errors.New("reorder buffer full")
	ErrFlowControlBlock = errors.New("flow control: sender blocked")
)

type Session interface {
	ID() string
	Mode() string
	Start(ctx context.Context) error
	Close() error
	HandleData(seq uint32, payload []byte)
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]Session
	lastSeen map[string]time.Time
	logger   *slog.Logger
}

func NewSessionManager(logger *slog.Logger) *SessionManager {
	return &SessionManager{
		sessions: make(map[string]Session),
		lastSeen: make(map[string]time.Time),
		logger:   logger,
	}
}

func (sm *SessionManager) Add(s Session) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.sessions[s.ID()]; exists {
		return ErrSessionExists
	}

	sm.sessions[s.ID()] = s
	sm.lastSeen[s.ID()] = time.Now()

	return nil
}

func (sm *SessionManager) Get(id string) (Session, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	s, ok := sm.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}

	return s, nil
}

func (sm *SessionManager) Remove(id string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.sessions, id)
	delete(sm.lastSeen, id)
}

func (sm *SessionManager) Touch(id string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.sessions[id]; ok {
		sm.lastSeen[id] = time.Now()
	}
}

func (sm *SessionManager) CloseAll() {
	sm.mu.Lock()
	sessions := make(map[string]Session, len(sm.sessions))
	for k, v := range sm.sessions {
		sessions[k] = v
	}
	sm.mu.Unlock()

	for id, s := range sessions {
		if err := s.Close(); err != nil {
			sm.logger.Error("error closing session", "session_id", id, "error", err)
		}
	}

	sm.mu.Lock()
	sm.sessions = make(map[string]Session)
	sm.lastSeen = make(map[string]time.Time)
	sm.mu.Unlock()
}

func (sm *SessionManager) Count() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return len(sm.sessions)
}

func (sm *SessionManager) RunStaleCleanup(ctx context.Context, timeout time.Duration) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sm.cleanupStale(timeout)
		}
	}
}

func (sm *SessionManager) cleanupStale(timeout time.Duration) {
	sm.mu.Lock()

	now := time.Now()
	var stale []string

	for id, lastSeen := range sm.lastSeen {
		if now.Sub(lastSeen) > timeout {
			stale = append(stale, id)
		}
	}

	staleSessions := make(map[string]Session, len(stale))
	for _, id := range stale {
		staleSessions[id] = sm.sessions[id]
		delete(sm.sessions, id)
		delete(sm.lastSeen, id)
	}

	sm.mu.Unlock()

	for id, s := range staleSessions {
		sm.logger.Info("cleaning up stale session", "session_id", id)

		if err := s.Close(); err != nil {
			sm.logger.Error("error closing stale session", "session_id", id, "error", err)
		}
	}
}

type ReorderBuffer struct {
	mu       sync.Mutex
	buf      map[uint32][]byte
	nextSeq  uint32
	size     int
	dataCh   chan []byte
	closeCh  chan struct{}
	closeOne sync.Once
}

func NewReorderBuffer(size int) *ReorderBuffer {
	return &ReorderBuffer{
		buf:     make(map[uint32][]byte),
		size:    size,
		dataCh:  make(chan []byte, size),
		closeCh: make(chan struct{}),
	}
}

func (rb *ReorderBuffer) Insert(seq uint32, data []byte) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if seq < rb.nextSeq {
		return nil // duplicate, ignore
	}

	if int(seq-rb.nextSeq) >= rb.size {
		return ErrBufferFull
	}

	buf := make([]byte, len(data))
	copy(buf, data)
	rb.buf[seq] = buf

	for {
		payload, ok := rb.buf[rb.nextSeq]
		if !ok {
			break
		}

		delete(rb.buf, rb.nextSeq)
		rb.nextSeq++

		select {
		case rb.dataCh <- payload:
		case <-rb.closeCh:
			return nil
		}
	}

	return nil
}

func (rb *ReorderBuffer) DataCh() <-chan []byte {
	return rb.dataCh
}

func (rb *ReorderBuffer) Close() {
	rb.closeOne.Do(func() {
		close(rb.closeCh)
	})
}

type FlowControl struct {
	mu            sync.Mutex
	sentBytes     uint64
	ackedBytes    uint64
	windowSize    uint64
	blockedNotify chan struct{}
}

func NewFlowControl(windowSize uint64) *FlowControl {
	return &FlowControl{
		windowSize:    windowSize,
		blockedNotify: make(chan struct{}, 1),
	}
}

func (fc *FlowControl) CanSend(n uint64) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	return (fc.sentBytes - fc.ackedBytes + n) <= fc.windowSize
}

func (fc *FlowControl) AddSent(n uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.sentBytes += n
}

func (fc *FlowControl) UpdateAck(acked uint64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if acked > fc.ackedBytes {
		fc.ackedBytes = acked
	}

	select {
	case fc.blockedNotify <- struct{}{}:
	default:
	}
}

func (fc *FlowControl) WaitForWindow(ctx context.Context, n uint64) error {
	for {
		if fc.CanSend(n) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fc.blockedNotify:
		}
	}
}

func (fc *FlowControl) SentBytes() uint64 {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	return fc.sentBytes
}

type SequenceWriter struct {
	mu        sync.Mutex
	seq       uint32
	transport Transport
	topic     string
	fc        *FlowControl
}

func NewSequenceWriter(transport Transport, topic string, fc *FlowControl) *SequenceWriter {
	return &SequenceWriter{
		transport: transport,
		topic:     topic,
		fc:        fc,
	}
}

func (sw *SequenceWriter) Write(ctx context.Context, data []byte) error {
	for len(data) > 0 {
		chunk := data
		if len(chunk) > MaxPayloadSize {
			chunk = data[:MaxPayloadSize]
		}

		if err := sw.fc.WaitForWindow(ctx, uint64(len(chunk))); err != nil {
			return err
		}

		sw.mu.Lock()
		frame := EncodeDataFrame(sw.seq, chunk)
		sw.seq++
		sw.mu.Unlock()

		sw.fc.AddSent(uint64(len(chunk)))

		if err := sw.transport.Publish(sw.topic, frame); err != nil {
			return err
		}

		data = data[len(chunk):]
	}

	return nil
}
