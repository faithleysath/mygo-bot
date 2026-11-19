package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type endpoint func(http.ResponseWriter, *http.Request) error

func (a *agent) routes() http.Handler {
	mux := http.NewServeMux()
	on := func(pattern string, fn endpoint) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if err := fn(w, r); err != nil {
				status := 500
				var problem httpError
				switch {
				case errors.As(err, &problem):
					status = problem.code
				case errors.Is(err, sql.ErrNoRows):
					status = 404
				default:
					log.Print(err)
				}
				writeJSON(w, status, map[string]string{"error": err.Error()})
			}
		})
	}
	on("PUT /config", a.setConfig)
	on("GET /config", a.getConfig)
	on("POST /sessions", a.createSession)
	on("GET /sessions", a.list)
	on("GET /sessions/{sid}", a.session)
	on("PATCH /sessions/{sid}", a.session)
	for _, path := range []string{"items", "context", "queue"} {
		on("GET /sessions/{sid}/"+path, a.list)
	}
	on("DELETE /sessions/{sid}/queue/{item}", a.withdraw)
	on("POST /sessions/{sid}/messages", a.send)
	on("POST /sessions/{sid}/resume", a.send)
	on("POST /sessions/{sid}/pause", a.pauseHTTP)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		if a.closing {
			a.mu.Unlock()
			writeJSON(w, 503, map[string]string{"error": reasonShutdown})
			return
		}
		a.handlers.Add(1)
		a.mu.Unlock()
		defer a.handlers.Done()
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func decode(r *http.Request, dst any, optional bool) error {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		if optional && err == io.EOF {
			return nil
		}
		return httpError{400, "invalid JSON: " + err.Error()}
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return httpError{400, "expected one JSON object"}
	}
	return nil
}
func pathID(r *http.Request, key string) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue(key), 10, 64)
	if err != nil || id <= 0 {
		return 0, httpError{400, "invalid " + key}
	}
	return id, nil
}
func validEffort(e *string) bool { return e == nil || *e == "low" || *e == "high" || *e == "max" }

func (a *agent) setConfig(w http.ResponseWriter, r *http.Request) error {
	var c config
	if err := decode(r, &c, false); err != nil {
		return err
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || strings.TrimSpace(c.Model) == "" || !validEffort(c.Effort) {
		return httpError{400, "invalid base_url, model or default_reasoning_effort"}
	}
	_, err = a.db.Exec(`INSERT OR REPLACE INTO config VALUES(1,?,?,?,?,?)`, c.BaseURL, c.APIKey, c.Model, c.Instructions, c.Effort)
	if err == nil {
		w.WriteHeader(204)
	}
	return err
}
func (a *agent) getConfig(w http.ResponseWriter, r *http.Request) error {
	var c config
	var hasKey bool
	err := a.db.QueryRow(`SELECT base_url,api_key!='',model,instructions,default_reasoning_effort FROM config`).Scan(&c.BaseURL, &hasKey, &c.Model, &c.Instructions, &c.Effort)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 200, map[string]bool{"configured": false})
		return nil
	}
	if err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"configured": true, "base_url": c.BaseURL, "has_api_key": hasKey, "model": c.Model, "instructions": c.Instructions, "default_reasoning_effort": c.Effort})
	return nil
}

// 创建时，省略字段表示继承默认值，显式 null 表示让网关决定。
func readEffort(r *http.Request, create bool) (*string, bool, error) {
	var input struct {
		Effort json.RawMessage `json:"reasoning_effort"`
	}
	if err := decode(r, &input, create); err != nil {
		return nil, false, err
	}
	if input.Effort == nil && create {
		return nil, true, nil
	}
	var e *string
	if json.Unmarshal(input.Effort, &e) != nil || !validEffort(e) {
		return nil, false, httpError{400, "expected reasoning_effort: low, high, max or null"}
	}
	return e, false, nil
}
func (a *agent) createSession(w http.ResponseWriter, r *http.Request) error {
	e, inherit, err := readEffort(r, true)
	if err != nil {
		return err
	}
	res, err := a.db.Exec(`INSERT INTO sessions(reasoning_effort) VALUES(CASE WHEN ? THEN (SELECT default_reasoning_effort FROM config) ELSE ? END)`, inherit, e)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err == nil {
		writeJSON(w, 201, map[string]any{"id": id, "status": "idle"})
	}
	return err
}

const sessionJSON = `json_object('id',id,'status',status,'reasoning_effort',reasoning_effort,'user_queue_cursor',user_queue_cursor,'last_error',last_error)`

func (a *agent) session(w http.ResponseWriter, r *http.Request) error {
	sid, err := pathID(r, "sid")
	if err != nil {
		return err
	}
	if r.Method == "PATCH" {
		e, _, err := readEffort(r, false)
		if err != nil {
			return err
		}
		if _, err = a.db.Exec(`UPDATE sessions SET reasoning_effort=? WHERE id=?`, e, sid); err != nil {
			return err
		}
	}
	var row []byte
	if err := a.db.QueryRow(`SELECT `+sessionJSON+` FROM sessions WHERE id=?`, sid).Scan(&row); err != nil {
		return err
	}
	writeJSON(w, 200, json.RawMessage(row))
	return nil
}

func (a *agent) list(w http.ResponseWriter, r *http.Request) error {
	var after int64
	limit := 50
	var err error
	q := r.URL.Query()
	if q.Has("after") {
		after, err = strconv.ParseInt(q.Get("after"), 10, 64)
		if err != nil || after < 0 {
			return httpError{400, "invalid after"}
		}
	}
	if q.Has("limit") {
		limit, err = strconv.Atoi(q.Get("limit"))
		if err != nil || limit < 1 || limit > 200 {
			return httpError{400, "limit must be 1 to 200"}
		}
	}
	query := `SELECT ` + sessionJSON + ` FROM sessions WHERE id>? ORDER BY id LIMIT ?`
	args := []any{after, limit}
	if r.PathValue("sid") != "" {
		sid, err := pathID(r, "sid")
		if err != nil {
			return err
		}
		var exists int
		if err = a.db.QueryRow(`SELECT 1 FROM sessions WHERE id=?`, sid).Scan(&exists); err != nil {
			return err
		}
		args = []any{sid, after, limit}
		switch {
		case strings.HasSuffix(r.URL.Path, "/items"):
			query = `SELECT json_object('id',id,'kind',kind,'payload',json(payload),'target_id',target_id) FROM items WHERE session_id=? AND id>? ORDER BY id LIMIT ?`
		case strings.HasSuffix(r.URL.Path, "/context"):
			query = `SELECT json_object('position',c.position,'item_id',i.id,'payload',json(i.payload)) FROM context_items c JOIN items i ON i.id=c.item_id WHERE c.session_id=? AND c.position>? ORDER BY c.position LIMIT ?`
		default:
			query = `SELECT json_object('id',i.id,'text',json_extract(i.payload,'$.content'))` + pending + ` AND i.id>? ORDER BY i.id LIMIT ?`
		}
	}
	rows, err := a.db.QueryContext(r.Context(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	page := []json.RawMessage{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		page = append(page, raw)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	writeJSON(w, 200, page)
	return nil
}

func (a *agent) withdraw(w http.ResponseWriter, r *http.Request) error {
	sid, err := pathID(r, "sid")
	if err != nil {
		return err
	}
	id, err := pathID(r, "item")
	if err != nil {
		return err
	}
	err = transaction(a.db, func(tx *sql.Tx) error {
		var cursor int64
		var withdrawn bool
		err := tx.QueryRow(`SELECT s.user_queue_cursor,EXISTS(SELECT 1 FROM items WHERE kind='withdrawal' AND target_id=i.id) FROM items i JOIN sessions s ON s.id=i.session_id WHERE s.id=? AND i.id=? AND i.kind='user_message'`, sid, id).Scan(&cursor, &withdrawn)
		if err != nil || withdrawn {
			return err
		}
		if id <= cursor {
			return httpError{409, "message already included in context"}
		}
		_, err = addItem(tx, sid, "withdrawal", json.RawMessage(`{}`), id)
		return err
	})
	if err == nil {
		w.WriteHeader(204)
	}
	return err
}

func (a *agent) pauseHTTP(w http.ResponseWriter, r *http.Request) error {
	sid, err := pathID(r, "sid")
	if err != nil {
		return err
	}
	in := struct {
		Mode string `json:"mode"`
	}{"soft"}
	if err = decode(r, &in, true); err != nil {
		return err
	}
	if in.Mode != "soft" && in.Mode != "hard" {
		return httpError{400, "expected soft or hard"}
	}
	status, err := a.pause(sid, in.Mode == "hard")
	if err == nil {
		writeJSON(w, 200, map[string]string{"status": status})
	}
	return err
}

func (a *agent) send(w http.ResponseWriter, r *http.Request) error {
	sid, err := pathID(r, "sid")
	if err != nil {
		return err
	}
	var message *string
	if strings.HasSuffix(r.URL.Path, "/messages") {
		var in struct {
			Text string `json:"text"`
		}
		if err := decode(r, &in, false); err != nil {
			return err
		}
		if strings.TrimSpace(in.Text) == "" {
			return httpError{400, "text is required"}
		}
		message = &in.Text
	} else {
		var unused any
		if err := json.NewDecoder(r.Body).Decode(&unused); err != io.EOF {
			return httpError{400, "resume expects an empty body"}
		}
	}
	sse := strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	id, j, err := a.submit(sid, message, r.Context(), sse)
	if err != nil {
		return err
	}
	if j == nil {
		if message == nil {
			w.WriteHeader(http.StatusAccepted)
			return nil
		}
		writeJSON(w, 202, map[string]any{"item_id": id})
		return nil
	}
	if !sse {
		select {
		case <-j.ready:
			status := 200
			if j.result.Status == "failed" {
				status = 502
			}
			result := j.result
			result.ItemID = id
			writeJSON(w, status, result)
		case <-r.Context().Done():
		}
		return nil
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ctrl := http.NewResponseController(w)
	if ctrl.Flush() != nil {
		return nil
	}
	for {
		var event notice
		var data any
		var name string
		last := false
		select {
		case <-r.Context().Done():
			return nil
		case event = <-j.events:
			name = event.Type
			data = event
		case <-j.ready:
			last = true
			name = "turn." + j.result.Status
			result := j.result
			result.ItemID = id
			data = result
		}
		b, _ := json.Marshal(data)
		_ = ctrl.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b); err != nil {
			return nil
		}
		if ctrl.Flush() != nil || last {
			return nil
		}
	}
}
