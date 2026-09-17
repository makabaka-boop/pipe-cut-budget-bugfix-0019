package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// slowHandler simulates a computation that stays in flight for d before
// responding.
func slowHandler(d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(d)
		_, _ = io.WriteString(w, "done")
	})
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}
	return addr
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server on %s did not start listening", addr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A termination signal while a request is in flight must not truncate the
// response: serve waits for the handler to finish and then returns nil, so
// the process exits 0 only after every in-flight computation completed.
func TestServeDrainsInFlightRequestBeforeReturning(t *testing.T) {
	srv := &http.Server{Addr: freeAddr(t), Handler: slowHandler(300 * time.Millisecond)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() { serveDone <- serve(ctx, srv, 5*time.Second) }()
	waitListening(t, srv.Addr)

	type result struct {
		status int
		body   string
		err    error
	}
	respCh := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + srv.Addr + "/")
		if err != nil {
			respCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		respCh <- result{status: resp.StatusCode, body: string(body), err: err}
	}()

	// The request is in flight when the termination signal arrives.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case r := <-respCh:
		if r.err != nil {
			t.Fatalf("in-flight request failed instead of being drained: %v", r.err)
		}
		if r.status != http.StatusOK || r.body != "done" {
			t.Fatalf("in-flight response = %d %q, want 200 %q", r.status, r.body, "done")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request was truncated instead of drained")
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve returned %v after a complete drain, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after draining")
	}
}

// When in-flight requests outlive the drain timeout, serve must report an
// error so the process exits with a failure status instead of silently
// truncating the responses with exit code 0.
func TestServeReportsDrainTimeout(t *testing.T) {
	srv := &http.Server{Addr: freeAddr(t), Handler: slowHandler(2 * time.Second)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() { serveDone <- serve(ctx, srv, 100*time.Millisecond) }()
	waitListening(t, srv.Addr)

	go func() {
		resp, err := http.Get("http://" + srv.Addr + "/")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	// The request is in flight when the termination signal arrives.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-serveDone:
		if err == nil {
			t.Fatal("serve returned nil although the drain timed out; " +
				"the process must exit with a failure status")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after the drain timeout")
	}
}
