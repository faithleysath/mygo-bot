package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

type agent struct {
	db     *sql.DB
	client *http.Client
	tool   func(context.Context, string, string) (string, error)
	// 创建和退出循环时，同时修改会话状态。等模型或工具期间不持锁。
	mu     sync.Mutex
	active map[int64]*job
}

func newAgent(db *sql.DB) *agent {
	return &agent{db: db, client: http.DefaultClient, tool: currentTime, active: make(map[int64]*job)}
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
	go a.run(j)
	return j
}

func (a *agent) stop(j *job, cause error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	reason := "paused by user"
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
	delay := 250 * time.Millisecond
	for {
		a.mu.Lock()
		if j.pausing() {
			a.mu.Unlock()
			a.stop(j, nil)
			return
		}
		err := transaction(a.db, func(tx *sql.Tx) error {
			if first {
				if err := recoverTools(tx, j.sid); err != nil {
					return err
				}
			}
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
		var transient retryable
		if errors.As(err, &transient) && j.ctx.Err() == nil {
			j.notify(notice{"attempt.retry", step.id, "retrying from current context"})
			select {
			case <-j.pause:
			case <-time.After(delay):
			}
			delay = min(2*delay, 5*time.Second)
			continue
		}
		if err == nil && !step.usedTool && !step.hasAnswer {
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
		delay = 250 * time.Millisecond
	}
}

type attempt struct {
	id                  string
	usedTool, hasAnswer bool
	text                strings.Builder
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
		s.usedTool = true
		// 先记录调用。若执行途中过早退出，恢复时补一个「结果未知」，避免重跑。
		var id int64
		err := transaction(a.db, func(tx *sql.Tx) error {
			var err error
			id, err = addItem(tx, j.sid, item.Type, raw, s.id, nil)
			return err
		})
		if err != nil {
			return err
		}
		j.notify(notice{"tool.called", s.id, item.Name})
		if err := j.ctx.Err(); err != nil {
			return err
		}
		output, err := a.tool(j.ctx, item.Name, item.Arguments)
		if err != nil {
			return err
		}
		return transaction(a.db, func(tx *sql.Tx) error { return finishTool(tx, j.sid, id, item.CallID, output, s.id) })
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
		id, err := addItem(tx, j.sid, kind, raw, s.id, nil)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO context_items(session_id,item_id) VALUES(?,?)`, j.sid, id)
		return err
	})
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
		} else if !s.usedTool {
			var queued bool
			if err := tx.QueryRow(`SELECT EXISTS(SELECT 1`+pending+`)`, j.sid).Scan(&queued); err != nil {
				return err
			}
			if !queued {
				state = "idle"
			}
		}
		_, err := tx.Exec(`UPDATE sessions SET status=?,last_error=CASE WHEN ?='paused' THEN 'paused by user' ELSE '' END WHERE id=?`, state, state, j.sid)
		return err
	})
	if err != nil {
		return false, err
	}
	if !s.usedTool {
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
	r, err := a.db.Exec(`UPDATE sessions SET status=?,last_error='paused by user' WHERE id=?`, state, sid)
	if err != nil {
		return "", err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return "", sql.ErrNoRows
	}
	if j != nil {
		if !j.pausing() {
			close(j.pause)
		}
		if hard {
			j.cancel()
		}
	}
	return state, nil
}

func (a *agent) submit(sid int64, message *string, listener context.Context, sse bool) (int64, *job, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var id int64
	var start bool
	err := transaction(a.db, func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRow(`SELECT status FROM sessions WHERE id=?`, sid).Scan(&status); err != nil {
			return err
		}
		if message != nil {
			payload, _ := json.Marshal(map[string]string{"role": "user", "content": *message})
			var err error
			id, err = addItem(tx, sid, "user_message", payload, "", nil)
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
