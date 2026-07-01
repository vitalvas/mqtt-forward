package health

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"
)

const (
	shutdownTimeout = 5 * time.Second
	readTimeout     = 5 * time.Second
	// writeTimeout must accommodate pprof CPU profile and trace endpoints,
	// which stream for up to ?seconds=N (default 30s) before responding.
	writeTimeout = 2 * time.Minute
	idleTimeout  = 30 * time.Second
)

type Server struct {
	listen  string
	healthy func() bool
	logger  *slog.Logger
	server  *http.Server
}

func New(listen string, healthy func() bool, logger *slog.Logger) *Server {
	return &Server{
		listen:  listen,
		healthy: healthy,
		logger:  logger,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	registerPprof(mux)

	return mux
}

func registerPprof(mux *http.ServeMux) {
	mux.Handle("/debug/pprof/", loopbackOnly(http.HandlerFunc(pprof.Index)))
	mux.Handle("/debug/pprof/cmdline", loopbackOnly(http.HandlerFunc(pprof.Cmdline)))
	mux.Handle("/debug/pprof/profile", loopbackOnly(http.HandlerFunc(pprof.Profile)))
	mux.Handle("/debug/pprof/symbol", loopbackOnly(http.HandlerFunc(pprof.Symbol)))
	mux.Handle("/debug/pprof/trace", loopbackOnly(http.HandlerFunc(pprof.Trace)))
}

func loopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopback(r.RemoteAddr) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	return ip.IsLoopback()
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")

	if s.healthy == nil || !s.healthy() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("unavailable\n"))

		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// listen creates the health server listener. The address is treated as a unix
// domain socket when it is prefixed with "unix:" or is a filesystem path (starts
// with "/"); otherwise it is a TCP address. For unix sockets the parent directory
// must already exist -- no directory is created here. A stale socket file left by
// an unclean shutdown is removed before binding.
func listen(addr string) (net.Listener, error) {
	if network, path, ok := unixAddr(addr); ok {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return nil, err
		}

		return net.Listen(network, path)
	}

	return net.Listen("tcp", addr)
}

// unixAddr reports whether addr denotes a unix domain socket and returns the
// network ("unix") and socket path to bind.
func unixAddr(addr string) (string, string, bool) {
	if path, ok := strings.CutPrefix(addr, "unix:"); ok {
		return "unix", path, true
	}

	if strings.HasPrefix(addr, "/") {
		return "unix", addr, true
	}

	return "", "", false
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := listen(s.listen)
	if err != nil {
		return err
	}

	s.server = &http.Server{
		Handler:      s.Handler(),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	s.logger.Debug("health server listening", "addr", listener.Addr().String())

	errCh := make(chan error, 1)

	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.logger.Debug("health server shutdown", "error", err)
		}

		<-errCh

		return nil

	case err := <-errCh:
		return err
	}
}
