package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Server owns a dedicated listener and mux; no debug endpoints are registered.
type Server struct {
	listener net.Listener
	http     *http.Server
}

// Listen binds synchronously so an unavailable metrics port fails before capture starts.
func Listen(addr string, handler http.Handler) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)
	return &Server{listener: listener, http: &http.Server{
		Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
	}}, nil
}

func (s *Server) Addr() net.Addr { return s.listener.Addr() }

// Run drains in-flight scrapes on cancellation and joins the serving goroutine.
func (s *Server) Run(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- s.http.Serve(s.listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err := s.http.Shutdown(shutdownCtx)
		if err != nil {
			err = errors.Join(err, s.http.Close())
		}
		serveErr := <-done
		if !errors.Is(serveErr, http.ErrServerClosed) {
			err = errors.Join(err, serveErr)
		}
		return err
	}
}
