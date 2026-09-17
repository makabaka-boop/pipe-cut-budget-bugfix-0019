// Command server runs the raincut HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raincut/internal/api"
)

// drainTimeout bounds how long the server waits for in-flight requests to
// finish after a termination signal. Requests still running when it expires
// are cut off and the process exits with a failure status. Keep it below
// the orchestrator's kill grace period (docker-compose sets 15s).
const drainTimeout = 10 * time.Second

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           api.New(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("raincut: listening on :%s", port)
	if err := serve(ctx, srv, drainTimeout); err != nil {
		log.Fatalf("raincut: %v", err)
	}
}

// serve runs srv until the listener fails or ctx signals termination.
//
// ListenAndServe returns as soon as Shutdown closes the listeners, while
// in-flight computations are still running; returning at that point would
// exit the process and truncate their responses. So on termination serve
// blocks until Shutdown has drained every in-flight request, and reports an
// error — a non-zero process exit — when the drain does not finish within
// drainTimeout.
func serve(ctx context.Context, srv *http.Server, drainTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		// The listener stopped on its own, e.g. the port is already taken.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listen: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	log.Printf("raincut: termination signal received, draining in-flight requests (timeout %s)", drainTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("in-flight requests not drained within %s: %w", drainTimeout, err)
	}
	<-serveErr // Shutdown closed the listener; ListenAndServe has returned.
	log.Printf("raincut: all in-flight requests drained, stopped")
	return nil
}
