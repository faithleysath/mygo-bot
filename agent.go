package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	reasonUser     = "paused by user"
	reasonShutdown = "daemon shutting down"
	reasonRestart  = "daemon restarted"
	maxRetries     = 3
)

type agent struct {
	db     *sql.DB
	client *http.Client
	tool   func(context.Context, string, string) (string, error)
	// 创建和退出循环时，同时修改会话状态。等模型或工具期间不持锁。
	mu                sync.Mutex
	active            map[int64]*job
	closing           bool
	workers, handlers sync.WaitGroup
	frameTimeout      time.Duration
}

func newAgent(db *sql.DB) *agent {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &agent{db: db, client: &http.Client{Transport: transport}, tool: currentTime, active: make(map[int64]*job), frameTimeout: 60 * time.Second}
}

type notice struct {
	Type    string `json:"type"`
	Attempt string `json:"attempt_id"`
	Data    string `json:"data"`
}
type reply struct {
	ItemID int64  `json:"item_id,omitempty"`
	Status string `json:"status"`
	Text   string `json:"text,omitempty"`
	Error  string `json:"error,omitempty"`
}
type job struct {
	sid      int64
	ctx      context.Context
	cancel   context.CancelFunc
	pause    chan struct{}
	reason   string // 由 agent.mu 保护
	listener context.Context
	events   chan notice
	ready    chan struct{}
	result   reply
}

func (j *job) notify(event notice) {
	if j.events == nil {
		return
	}
	select {
	case j.events <- event:
	case <-j.listener.Done():
	case <-j.ready:
	case <-j.ctx.Done():
	}
}
func (j *job) answer(result reply) {
	select {
	case <-j.ready:
	default:
		j.result = result
		close(j.ready)
	}
}
func (j *job) pausing() bool {
	select {
	case <-j.pause:
		return true
	default:
		return false
	}
}

// 调用时已持有 mu，数据库里的 running 状态也已经提交。
func (a *agent) launch(sid int64, listener context.Context, sse bool) *job {
	ctx, cancel := context.WithCancel(context.Background())
	j := &job{sid: sid, ctx: ctx, cancel: cancel, pause: make(chan struct{}), listener: listener, ready: make(chan struct{})}
	if sse {
		j.events = make(chan notice)
	}
	a.active[sid] = j
	a.workers.Go(func() { a.run(j) })
	return j
}

func (a *agent) stop(j *job, cause error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	reason := j.reason
	if cause != nil && !j.pausing() {
		reason = cause.Error()
	}
	_, err := a.db.Exec(`UPDATE sessions SET status='paused',last_error=? WHERE id=?`, reason, j.sid)
	if err != nil {
		reason = err.Error()
	}
	status := "paused"
	if err != nil || (cause != nil && !j.pausing()) {
		status = "failed"
	}
	delete(a.active, j.sid)
	j.cancel()
	j.answer(reply{Status: status, Error: reason})
}

// 外层循环负责工具往返和断线重试；stream 内层循环只负责一次模型请求。
func (a *agent) run(j *job) {
	var cfg config
	first := true
	retries := 0
	for {
		a.mu.Lock()
		if j.pausing() {
			a.mu.Unlock()
			a.stop(j, nil)
			return
		}
		err := transaction(a.db, func(tx *sql.Tx) error {
			added, err := includePending(tx, j.sid)
			if err != nil {
				return err
			}
			if first || added {
				return tx.QueryRow(`SELECT c.base_url,c.api_key,c.model,c.instructions,s.reasoning_effort FROM config c,sessions s WHERE s.id=?`, j.sid).Scan(&cfg.BaseURL, &cfg.APIKey, &cfg.Model, &cfg.Instructions, &cfg.Effort)
			}
			return nil
		})
		a.mu.Unlock()
		first = false
		if err != nil {
			a.stop(j, err)
			return
		}

		step := attempt{id: rand.Text()}
		err = a.stream(j.ctx, j.sid, cfg, func(event modelEvent) error {
			switch event.Type {
			case "response.output_text.delta":
				j.notify(notice{"text.delta", step.id, event.Delta})
			case "response.output_item.done":
				return a.accept(j, &step, event.Item)
			}
			return nil
		})
		// 即使流已断开或取消，也先等全部工具退出。工具独立于单次网络请求。
		if toolErr := a.waitTools(j, &step); toolErr != nil {
			a.stop(j, toolErr)
			return
		}
		var transient retryable
		if errors.As(err, &transient) && j.ctx.Err() == nil {
			if j.pausing() {
				a.stop(j, nil)
				return
			}
			if retries == maxRetries {
				a.stop(j, fmt.Errorf("retry limit reached (%d): %w", maxRetries, err))
				return
			}
			delay := (250 * time.Millisecond) << retries
			retries++
			j.notify(notice{"attempt.retry", step.id, "retrying from current context"})
			select {
			case <-j.pause:
			case <-time.After(delay):
			}
			continue
		}
		if err == nil && len(step.tools) == 0 && !step.hasAnswer {
			err = errors.New("response completed without an answer or tool call")
		}
		if err != nil {
			a.stop(j, err)
			return
		}
		finished, err := a.complete(j, &step)
		if err != nil {
			a.stop(j, err)
			return
		}
		if finished {
			return
		}
		retries = 0
	}
}

type attempt struct {
	id        string
	tools     []<-chan error
	hasAnswer bool
	text      strings.Builder
}

func (a *agent) accept(j *job, s *attempt, raw json.RawMessage) error {
	var item struct {
		Type, Name, Arguments, Role string
		CallID                      string `json:"call_id"`
		Content                     []struct{ Type, Text, Refusal string }
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		return err
	}
	if item.Type == "" {
		return errors.New("missing item type")
	}
	if item.Type == "function_call" {
		if item.CallID == "" {
			return errors.New("missing call_id")
		}
		// 先记录调用。若执行途中过早退出，恢复时补一个「结果未知」，避免重跑。
		var id int64
		err := transaction(a.db, func(tx *sql.Tx) error {
			var err error
			id, err = addItem(tx, j.sid, item.Type, raw, nil)
			return err
		})
		if err != nil {
			return err
		}
		j.notify(notice{"tool.called", s.id, item.Name})
		if err := j.ctx.Err(); err != nil {
			return err
		}
		done := make(chan error, 1)
		s.tools = append(s.tools, done)
		go func() { done <- a.runTool(j, id, item.CallID, item.Name, item.Arguments) }()
		return nil
	}
	kind := item.Type
	if kind == "message" && item.Role == "assistant" {
		kind = "assistant_message"
		for _, part := range item.Content {
			switch part.Type {
			case "output_text":
				s.hasAnswer = true
				s.text.WriteString(part.Text)
			case "refusal":
				s.hasAnswer = true
				s.text.WriteString(part.Refusal)
			}
		}
	}
	return transaction(a.db, func(tx *sql.Tx) error {
		id, err := addItem(tx, j.sid, kind, raw, nil)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO context_items(session_id,item_id) VALUES(?,?)`, j.sid, id)
		return err
	})
}

func (a *agent) runTool(j *job, id int64, callID, name, args string) error {
	output, err := a.tool(j.ctx, name, args)
	if err != nil {
		if j.ctx.Err() != nil {
			return nil // 恢复时用 last_error 解释缺失的结果。
		}
		output = "Tool error: " + err.Error()
	}
	return transaction(a.db, func(tx *sql.Tx) error {
		_, err := saveToolResult(tx, j.sid, id, callID, output)
		return err
	})
}

func (a *agent) waitTools(j *job, s *attempt) error {
	var err error
	for _, done := range s.tools {
		err = errors.Join(err, <-done) // 即使其中一个写库失败，也要等其余任务退出。
	}
	if err = errors.Join(err, j.ctx.Err()); err != nil || len(s.tools) == 0 {
		return err
	}
	return transaction(a.db, func(tx *sql.Tx) error { return publishTools(tx, j.sid, false, "") })
}

func (a *agent) complete(j *job, s *attempt) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if j.ctx.Err() != nil {
		return false, j.ctx.Err()
	}
	state := "running"
	err := transaction(a.db, func(tx *sql.Tx) error {
		if j.pausing() {
			state = "paused"
		} else if len(s.tools) == 0 {
			var queued bool
			if err := tx.QueryRow(`SELECT EXISTS(SELECT 1`+pending+`)`, j.sid).Scan(&queued); err != nil {
				return err
			}
			if !queued {
				state = "idle"
			}
		}
		_, err := tx.Exec(`UPDATE sessions SET status=?,last_error=? WHERE id=?`, state, j.reason, j.sid)
		return err
	})
	if err != nil {
		return false, err
	}
	if len(s.tools) == 0 {
		j.answer(reply{Status: "completed", Text: s.text.String()})
	}
	if state != "running" {
		delete(a.active, j.sid)
		j.cancel()
		j.answer(reply{Status: state})
		return true, nil
	}
	return false, nil
}

func (a *agent) pause(sid int64, hard bool) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	j := a.active[sid]
	state := "paused"
	if j != nil {
		state = "pausing"
	}
	if a.closing {
		return "", httpError{503, reasonShutdown}
	}
	r, err := a.db.Exec(`UPDATE sessions SET status=?,last_error=CASE WHEN status='paused' THEN last_error ELSE ? END WHERE id=?`, state, reasonUser, sid)
	if err != nil {
		return "", err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return "", sql.ErrNoRows
	}
	if j != nil {
		j.requestPause(reasonUser, hard)
	}
	return state, nil
}

// 调用时持有 agent.mu。重复请求可以把软暂停升级成硬暂停。
func (j *job) requestPause(reason string, hard bool) {
	if !j.pausing() {
		j.reason = reason
		close(j.pause)
	}
	if hard {
		j.cancel()
	}
}

func (a *agent) submit(sid int64, message *string, listener context.Context, sse bool) (int64, *job, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closing {
		return 0, nil, httpError{503, reasonShutdown}
	}
	var id int64
	var start bool
	err := transaction(a.db, func(tx *sql.Tx) error {
		var status, reason string
		if err := tx.QueryRow(`SELECT status,last_error FROM sessions WHERE id=?`, sid).Scan(&status, &reason); err != nil {
			return err
		}
		if message != nil {
			payload, _ := json.Marshal(map[string]string{"role": "user", "content": *message})
			var err error
			id, err = addItem(tx, sid, "user_message", payload, nil)
			if err != nil {
				return err
			}
		}
		start = a.active[sid] == nil && (message == nil || status == "idle")
		if !start {
			return nil
		}
		var configured bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM config)`).Scan(&configured); err != nil {
			return err
		}
		if !configured {
			return httpError{503, "configure the model first"}
		}
		if err := publishTools(tx, sid, true, reason); err != nil {
			return err
		}
		if message == nil {
			var hasInput bool
			if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM context_items WHERE session_id=?) OR EXISTS(SELECT 1`+pending+`)`, sid, sid).Scan(&hasInput); err != nil {
				return err
			}
			if !hasInput {
				return httpError{409, "empty session"}
			}
		}
		_, err := tx.Exec(`UPDATE sessions SET status='running',last_error='' WHERE id=?`, sid)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	if !start {
		return id, nil, nil
	}
	return id, a.launch(sid, listener, sse), nil
}

type httpError struct {
	code    int
	message string
}

func (e httpError) Error() string { return e.message }
