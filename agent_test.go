package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	*testing.T
	*agent
	api http.Handler
}

func setup(t *testing.T, model http.HandlerFunc) fixture {
	t.Helper()
	server := httptest.NewServer(model)
	t.Cleanup(server.Close)
	db, err := openStore(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	a := newAgent(db)
	f := fixture{t, a, a.routes()}
	f.request("PUT", "/config", fmt.Sprintf(`{"base_url":%q,"model":"test","default_reasoning_effort":"low"}`, server.URL), 204)
	f.request("POST", "/sessions", "", 201)
	return f
}
func (f fixture) request(method, path, body string, status int) string {
	f.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	f.api.ServeHTTP(w, r)
	if w.Code != status {
		f.Errorf("%s %s: got %d, want %d: %s", method, path, w.Code, status, w.Body.String())
	}
	return w.Body.String()
}

const call = `{"type":"function_call","call_id":"clock-1","name":"current_time","arguments":"{}"}`

func item(w http.ResponseWriter, payload string) {
	fmt.Fprintf(w, "data: {\"type\":\"response.output_item.done\",\"item\":%s}\n\n", payload)
}
func answer(w http.ResponseWriter, text string) {
	item(w, fmt.Sprintf(`{"type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}`, text))
}
func completed(w http.ResponseWriter) { fmt.Fprint(w, "data: {\"type\":\"response.completed\"}\n\n") }
func readRequest(t *testing.T, r *http.Request) string {
	t.Helper()
	var req struct {
		Input     json.RawMessage
		Stream    bool
		Store     bool
		Reasoning struct{ Effort string }
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Error(err)
	}
	if r.URL.Path != "/responses" || !req.Stream || req.Store || req.Reasoning.Effort != "low" {
		t.Errorf("unexpected request %s %+v", r.URL, req)
	}
	return string(req.Input)
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(4 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

func TestRetryAndQueue(t *testing.T) {
	var attempts atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		in := readRequest(t, r)
		w.Header().Set("Content-Type", "text/event-stream")
		if strings.Contains(in, "withdraw-me") {
			t.Error("withdrawn message sent")
		}
		switch attempts.Add(1) {
		case 1:
			answer(w, "checking")
			item(w, call)
			completed(w)
		case 2:
			if !strings.Contains(in, "function_call_output") {
				t.Error("missing clock result")
			}
			answer(w, "first answer") // EOF before response.completed
		case 3:
			if strings.Count(in, "first answer") != 1 || !strings.Contains(in, "checking") {
				t.Errorf("bad replay: %s", in)
			}
			answer(w, "confirmed")
			completed(w)
		default:
			http.Error(w, "unexpected retry", 400)
		}
	})
	f.request("POST", "/sessions/1/pause", "", 200)
	f.request("POST", "/sessions/1/messages", `{"text":"read clock"}`, 202)
	f.request("POST", "/sessions/1/messages", `{"text":"withdraw-me"}`, 202)
	for range 2 {
		f.request("DELETE", "/sessions/1/queue/2", "", 204)
	}
	if q := f.request("GET", "/sessions/1/queue", "", 200); strings.Contains(q, "withdraw-me") || !strings.Contains(q, "read clock") {
		t.Fatal(q)
	}
	f.request("POST", "/sessions/1/resume", `{"text":"continue"}`, 400)
	r := httptest.NewRequest("POST", "/sessions/1/resume", nil)
	r.Header.Set("Accept", "text/event-stream")
	w := httptest.NewRecorder()
	f.api.ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), "event: attempt.retry") || !strings.Contains(w.Body.String(), "event: turn.completed") || !strings.Contains(w.Body.String(), "confirmed") {
		t.Fatal(w.Body.String())
	}
	log := f.request("GET", "/sessions/1/items", "", 200)
	if !strings.Contains(log, "first answer") || !strings.Contains(log, "withdrawal") {
		t.Fatal(log)
	}
	var history []struct {
		Payload struct {
			Type string
			Role string
		}
	}
	if err := json.Unmarshal([]byte(f.request("GET", "/sessions/1/context", "", 200)), &history); err != nil {
		t.Fatal(err)
	}
	var seq []string
	for _, e := range history {
		seq = append(seq, e.Payload.Role+e.Payload.Type)
	}
	if !reflect.DeepEqual(seq, []string{"user", "assistantmessage", "function_call", "function_call_output", "assistantmessage", "assistantmessage"}) {
		t.Fatal(seq)
	}
	f.request("DELETE", "/sessions/1/queue/1", "", 409)
	if !strings.Contains(f.request("GET", "/sessions/1", "", 200), `"idle"`) {
		t.Fatal("expected idle")
	}
}

func TestSoftPauseAndConcurrentSession(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var n atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		in := readRequest(t, r)
		w.Header().Set("Content-Type", "text/event-stream")
		if strings.Contains(in, "separate") {
			answer(w, "independent")
			completed(w)
			return
		}
		if n.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				t.Error("soft pause cancelled stream")
				return
			}
			item(w, call)
		} else {
			answer(w, "resumed")
		}
		completed(w)
	})
	result := make(chan string, 1)
	go func() { result <- f.request("POST", "/sessions/1/messages", `{"text":"clock"}`, 200) }()
	await(t, started)
	f.request("POST", "/sessions/1/pause", "", 200)
	f.request("POST", "/sessions/1/messages", `{"text":"queued"}`, 202)
	f.request("POST", "/sessions", "", 201)
	if !strings.Contains(f.request("POST", "/sessions/2/messages", `{"text":"separate"}`, 200), "independent") {
		t.Fatal("second session")
	}
	close(release)
	if !strings.Contains(await(t, result), "paused") {
		t.Fatal("expected soft pause")
	}
	if q := f.request("GET", "/sessions/1/queue", "", 200); !strings.Contains(q, "queued") {
		t.Fatal(q)
	}
	if !strings.Contains(f.request("POST", "/sessions/1/resume", "", 200), "resumed") {
		t.Fatal("resume")
	}
}

func TestHardPauseTool(t *testing.T) {
	var n atomic.Int32
	entered := make(chan struct{})
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		in := readRequest(t, r)
		w.Header().Set("Content-Type", "text/event-stream")
		if n.Add(1) == 1 {
			item(w, call)
		} else {
			if !strings.Contains(in, "result unknown") {
				t.Error(in)
			}
			answer(w, "ok")
		}
		completed(w)
	})
	f.tool = func(ctx context.Context, _, _ string) (string, error) {
		close(entered)
		<-ctx.Done()
		return "", ctx.Err()
	}
	result := make(chan string, 1)
	go func() { result <- f.request("POST", "/sessions/1/messages", `{"text":"clock"}`, 200) }()
	await(t, entered)
	f.request("POST", "/sessions/1/pause", `{"mode":"hard"}`, 200)
	await(t, result)
	if strings.Contains(f.request("GET", "/sessions/1/context", "", 200), "function_call") {
		t.Fatal("unpaired call visible")
	}
	f.request("POST", "/sessions/1/resume", "", 200)
}

func TestRestartAndSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mygo-bot.sqlite")
	db, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO sessions(status) VALUES('running'); INSERT INTO items(session_id,kind,payload) VALUES(1,'assistant_message','{}');INSERT INTO context_items(session_id,item_id) VALUES(1,1)`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	var status string
	var visible, logs int
	if err = db.QueryRow(`SELECT status,(SELECT count(*) FROM context_items),(SELECT count(*) FROM items) FROM sessions`).Scan(&status, &visible, &logs); err != nil || status != "paused" || visible != 1 || logs != 1 {
		t.Fatalf("recovery %s %d %d %v", status, visible, logs, err)
	}
	if _, err = db.Exec(`ALTER TABLE sessions ADD COLUMN extra TEXT`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err = openStore(path); err == nil {
		db.Close()
		t.Fatal("accepted schema mismatch")
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatal(files)
	}
}

func TestSoftToHardWhileWaitingOnNetwork(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		readRequest(t, r)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"half\"}\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(cancelled)
	})
	result := make(chan string, 1)
	go func() { result <- f.request("POST", "/sessions/1/messages", `{"text":"hello"}`, 200) }()
	await(t, started)
	f.request("POST", "/sessions/1/pause", "", 200)
	f.request("POST", "/sessions/1/pause", `{"mode":"hard"}`, 200)
	await(t, cancelled)
	await(t, result)
	if strings.Contains(f.request("GET", "/sessions/1/items", "", 200), "half") {
		t.Fatal("partial item persisted")
	}
}

func TestClientDisconnect(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		readRequest(t, r)
		close(started)
		<-release
		w.Header().Set("Content-Type", "text/event-stream")
		answer(w, "saved after disconnect")
		completed(w)
	})
	server := httptest.NewServer(f.api)
	defer server.Close()
	req, _ := http.NewRequest("POST", server.URL+"/sessions/1/messages", strings.NewReader(`{"text":"hello"}`))
	req.Header.Set("Accept", "text/event-stream")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	await(t, started)
	res.Body.Close()
	f.mu.Lock()
	worker := f.active[1]
	f.mu.Unlock()
	close(release)
	await(t, worker.ready)
	if worker.result.Text != "saved after disconnect" {
		t.Fatal(worker.result)
	}
}

func TestPermanentModelError(t *testing.T) {
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		http.Error(w, "invalid context", 400)
	})
	if s := f.request("POST", "/sessions/1/messages", `{"text":"hello"}`, 502); !strings.Contains(s, "400") {
		t.Fatal(s)
	}
	var st string
	if err := f.db.QueryRow(`SELECT status FROM sessions`).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "paused" {
		t.Fatal(st)
	}
}
