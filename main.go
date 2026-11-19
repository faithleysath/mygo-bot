package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := start(); err != nil {
		log.Fatal(err)
	}
}

func start() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := openStore("mygo-bot.sqlite")
	if err != nil {
		return err
	}
	defer db.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		return err
	}
	log.Printf("listening on http://%s", listener.Addr())
	return serve(ctx, listener, newAgent(db), 5*time.Second)
}

func serve(ctx context.Context, listener net.Listener, a *agent, grace time.Duration) error {
	server := &http.Server{Handler: a.routes(), ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-stopped:
	}

	// Shutdown 会等同步 /messages 返回，因此先通知所有工作循环收尾。
	a.mu.Lock()
	a.closing = true
	for _, j := range a.active {
		j.requestPause(reasonShutdown, false)
	}
	a.mu.Unlock()
	deadline, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	force := context.AfterFunc(deadline, func() {
		a.mu.Lock()
		for _, j := range a.active {
			j.cancel()
		}
		a.mu.Unlock()
		server.Close()
	})
	defer force()
	err := server.Shutdown(deadline)
	a.workers.Wait()
	a.handlers.Wait()
	a.client.CloseIdleConnections()
	if errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	return errors.Join(serveErr, err)
}
