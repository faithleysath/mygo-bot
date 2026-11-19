package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A 一直等到 B 已经落库才返回。分别在正常响应和断流后检查下一次请求。
func TestParallelToolBarrier(t *testing.T) {
	for _, eof := range []bool{false, true} {
		t.Run(fmt.Sprintf("EOF=%t", eof), func(t *testing.T) {
			release := make(chan struct{})
			bStarted := make(chan struct{})
			var requests atomic.Int32
			f := setup(t, func(w http.ResponseWriter, r *http.Request) {
				input := readRequest(t, r)
				w.Header().Set("Content-Type", "text/event-stream")
				if requests.Add(1) == 1 {
					item(w, call)
					item(w, strings.ReplaceAll(strings.ReplaceAll(call, "clock-1", "clock-2"), "current_time", "fast"))
					if !eof {
						completed(w)
					}
					return
				}
				a, b := strings.Index(input, "slow result"), strings.Index(input, "Tool error: fast failed")
				if a < 0 || b < a {
					t.Errorf("wrong tool result order: %s", input)
				}
				answer(w, "ok")
				completed(w)
			})
			f.tool = func(ctx context.Context, name, _ string) (string, error) {
				if name == "fast" {
					close(bStarted)
					return "", errors.New("fast failed")
				}
				<-release
				return "slow result", nil
			}
			done := make(chan string, 1)
			go func() { done <- f.request("POST", "/sessions/1/messages", `{"text":"clock"}`, 200) }()
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			await(t, bStarted)
			deadline := time.Now().Add(2 * time.Second)
			for !strings.Contains(f.request("GET", "/sessions/1/items", "", 200), "fast failed") {
				if time.Now().After(deadline) {
					t.Fatal("fast result not saved")
				}
				time.Sleep(time.Millisecond)
			}
			if strings.Contains(f.request("GET", "/sessions/1/context", "", 200), "function_call") || requests.Load() != 1 {
				t.Fatal("advanced before slow tool completed")
			}
			close(release)
			await(t, done)
			log := f.request("GET", "/sessions/1/items", "", 200)
			if strings.Index(log, "fast failed") > strings.Index(log, "slow result") {
				t.Fatal(log)
			}
		})
	}
}

func TestRecoverToolBatch(t *testing.T) {
	for _, tc := range []struct{ reason, want string }{{reasonUser, "User paused"}, {reasonRestart, "Daemon restarted"}, {reasonShutdown, "Daemon shut down"}, {"internal details", "Execution interrupted"}} {
		t.Run(tc.reason, func(t *testing.T) {
			f := setup(t, func(w http.ResponseWriter, r *http.Request) {
				in := readRequest(t, r)
				a, b := strings.Index(in, tc.want), strings.Index(in, "saved result")
				if a < 0 || b < a || strings.Contains(in, "internal details") || strings.Count(in, "function_call_output") != 2 {
					t.Error(in)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				answer(w, "ok")
				completed(w)
			})
			err := transaction(f.db, func(tx *sql.Tx) error {
				if _, err := tx.Exec(`UPDATE sessions SET status='paused',last_error=?`, tc.reason); err != nil {
					return err
				}
				if _, err := addItem(tx, 1, "function_call", []byte(call), nil); err != nil {
					return err
				}
				b, err := addItem(tx, 1, "function_call", []byte(strings.ReplaceAll(call, "clock-1", "clock-2")), nil)
				if err != nil {
					return err
				}
				_, err = saveToolResult(tx, 1, b, "clock-2", "saved result")
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			// 只有日志中的调用，没有已发布的上下文。空恢复也应修好这批结果。
			f.request("POST", "/sessions/1/resume", "", 200)
			if got := f.request("GET", "/sessions/1", "", 200); !strings.Contains(got, `"last_error":""`) {
				t.Fatal(got)
			}
		})
	}
}

func TestGatewayTimeoutsAndRetryLimit(t *testing.T) {
	for _, mode := range []string{"headers", "partial frame", "complete items then EOF"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var tries atomic.Int32
			releaseGateway := make(chan struct{})
			f := setup(t, func(w http.ResponseWriter, r *http.Request) {
				readRequest(t, r)
				n := tries.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				if n > maxRetries+1 {
					answer(w, "resumed")
					completed(w)
					return
				}
				switch mode {
				case "headers":
					select {
					case <-releaseGateway:
					case <-r.Context().Done():
					}
				case "complete items then EOF":
					answer(w, "received")
				case "partial frame":
					// 持续收到字节，但没有一帧是完整的。
					for {
						fmt.Fprint(w, "data: ")
						w.(http.Flusher).Flush()
						select {
						case <-r.Context().Done():
							return
						case <-time.After(10 * time.Millisecond):
						}
					}
				}
			})
			defer close(releaseGateway)
			f.client.Transport.(*http.Transport).ResponseHeaderTimeout = 100 * time.Millisecond
			f.frameTimeout = 100 * time.Millisecond
			f.request("POST", "/sessions/1/messages", `{"text":"clock"}`, 502)
			state := f.request("GET", "/sessions/1", "", 200)
			if tries.Load() != maxRetries+1 || !strings.Contains(state, `"paused"`) || !strings.Contains(state, "retry limit reached") {
				t.Fatal(tries.Load(), state)
			}
			if mode == "headers" && !strings.Contains(state, "timeout awaiting response headers") {
				t.Fatal(state)
			}
			if mode == "partial frame" && !strings.Contains(state, "idle timeout") {
				t.Fatal(state)
			}
			f.request("POST", "/sessions/1/resume", "", 200)
		})
	}
}

func TestHeartbeatAndRetryReset(t *testing.T) {
	var tries atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		readRequest(t, r)
		w.Header().Set("Content-Type", "text/event-stream")
		switch tries.Add(1) {
		case 1, 2, 4, 5:
			w.WriteHeader(502)
		case 3:
			item(w, call)
			completed(w)
		default:
			for range 8 {
				fmt.Fprint(w, ": heartbeat\n\n")
				w.(http.Flusher).Flush()
				time.Sleep(25 * time.Millisecond)
			}
			answer(w, "ok")
			completed(w)
		}
	})
	f.frameTimeout = 120 * time.Millisecond
	if got := f.request("POST", "/sessions/1/messages", `{"text":"clock"}`, 200); tries.Load() != 6 || !strings.Contains(got, "ok") {
		t.Fatal(got, tries.Load())
	}
}

func TestShutdownDrainsTools(t *testing.T) {
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		readRequest(t, r)
		w.Header().Set("Content-Type", "text/event-stream")
		item(w, call)
		completed(w)
	})
	f.tool = func(ctx context.Context, _, _ string) (string, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return "", ctx.Err()
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan error, 1)
	go func() { exited <- serve(ctx, listener, f.agent, 50*time.Millisecond) }()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		r, e := http.Post("http://"+listener.Addr().String()+"/sessions/1/messages", "application/json", strings.NewReader(`{"text":"clock"}`))
		if e == nil {
			r.Body.Close()
		}
	}()
	await(t, started)
	cancel()
	await(t, cancelled)
	f.request("POST", "/sessions/1/resume", "", 503)
	select {
	case <-exited:
		t.Fatal("shutdown left a tool behind")
	default:
	}
	close(release)
	if err := await(t, exited); err != nil {
		t.Fatal(err)
	}
	await(t, clientDone)
	var status, reason string
	if err := f.db.QueryRow(`SELECT status,last_error FROM sessions`).Scan(&status, &reason); err != nil || status != "paused" || reason != reasonShutdown {
		t.Fatal(status, reason, err)
	}
}
