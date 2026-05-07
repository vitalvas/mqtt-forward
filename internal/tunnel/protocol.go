package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	MessageTypeOpen    = "open"
	MessageTypeOpenAck = "open_ack"
	MessageTypeClose   = "close"
	MessageTypeResize  = "resize"
	MessageTypePing    = "ping"
	MessageTypePong    = "pong"

	SessionModeTCP   = "tcp"
	SessionModeShell = "shell"
	SessionModeExec  = "exec"

	DataHeaderSize = 4
	MaxPayloadSize = 64 * 1024
	ReorderBufSize = 64

	FlowControlWindow = 256 * 1024
	StaleTimeout      = 5 * 60 // seconds

	topicPrefix  = "tunnel"
	topicIn      = "in"
	topicOut     = "out"
	topicControl = "control"
	topicData    = "data"
	topicShared  = "__shared__"
)

// SharedPingTopic returns the broadcast topic for status pings.
func SharedPingTopic() string {
	return strings.Join([]string{topicPrefix, topicShared, MessageTypePing}, "/")
}

// AllOutControlFilter returns a wildcard filter for control messages from all devices.
func AllOutControlFilter() string {
	return strings.Join([]string{topicPrefix, "+", topicOut, topicControl}, "/")
}

type ControlMessage struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Mode      string `json:"mode,omitempty"`
	Target    string `json:"target,omitempty"`
	Command   string `json:"command,omitempty"`
	Cols      uint16 `json:"cols,omitempty"`
	Rows      uint16 `json:"rows,omitempty"`
	Success   bool   `json:"success,omitempty"`
	Error     string `json:"error,omitempty"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	AckBytes  uint64 `json:"ack_bytes,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Timeout   int    `json:"timeout,omitempty"`
}

type ParsedTopic struct {
	DeviceID  string
	IsControl bool
	IsData    bool
	SessionID string
}

// InControlTopic returns the topic for messages going to the device.
func InControlTopic(deviceID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicIn, topicControl}, "/")
}

// InDataTopic returns the data topic for messages going to the device.
func InDataTopic(deviceID, sessionID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicIn, topicData, sessionID}, "/")
}

// OutControlTopic returns the topic for messages coming from the device.
func OutControlTopic(deviceID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicOut, topicControl}, "/")
}

// OutDataTopic returns the data topic for messages coming from the device.
func OutDataTopic(deviceID, sessionID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicOut, topicData, sessionID}, "/")
}

// InControlFilter returns the subscription filter for incoming control messages.
func InControlFilter(deviceID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicIn, topicControl}, "/")
}

// InDataFilter returns the subscription filter for incoming data messages.
func InDataFilter(deviceID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicIn, topicData, "+"}, "/")
}

// OutControlFilter returns the subscription filter for outgoing control messages.
func OutControlFilter(deviceID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicOut, topicControl}, "/")
}

// OutDataFilter returns the subscription filter for outgoing data messages.
func OutDataFilter(deviceID string) string {
	return strings.Join([]string{topicPrefix, deviceID, topicOut, topicData, "+"}, "/")
}

func ParseTopic(topic string) (ParsedTopic, error) {
	parts := strings.Split(topic, "/")

	if len(parts) < 4 {
		return ParsedTopic{}, errors.New("topic too short")
	}

	if parts[0] != topicPrefix {
		return ParsedTopic{}, fmt.Errorf("invalid topic prefix: %s", parts[0])
	}

	dir := parts[2]
	if dir != topicIn && dir != topicOut {
		return ParsedTopic{}, fmt.Errorf("invalid direction: %s", dir)
	}

	parsed := ParsedTopic{
		DeviceID: parts[1],
	}

	switch parts[3] {
	case topicControl:
		if len(parts) != 4 {
			return ParsedTopic{}, errors.New("control topic must have exactly 4 segments")
		}

		parsed.IsControl = true

	case topicData:
		if len(parts) != 5 {
			return ParsedTopic{}, errors.New("data topic must have exactly 5 segments")
		}

		parsed.IsData = true
		parsed.SessionID = parts[4]

	default:
		return ParsedTopic{}, fmt.Errorf("unknown topic type: %s", parts[3])
	}

	return parsed, nil
}

func EncodeDataFrame(seq uint32, payload []byte) []byte {
	frame := make([]byte, DataHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[:DataHeaderSize], seq)
	copy(frame[DataHeaderSize:], payload)

	return frame
}

func DecodeDataFrame(frame []byte) (uint32, []byte, error) {
	if len(frame) < DataHeaderSize {
		return 0, nil, errors.New("frame too short")
	}

	seq := binary.BigEndian.Uint32(frame[:DataHeaderSize])

	return seq, frame[DataHeaderSize:], nil
}
