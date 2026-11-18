package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type config struct {
	BaseURL      string  `json:"base_url"`
	APIKey       string  `json:"api_key"`
	Model        string  `json:"model"`
	Instructions string  `json:"instructions"`
	Effort       *string `json:"default_reasoning_effort"`
}

var clockTool = json.RawMessage(`{
	"type": "function", "name": "current_time",
	"description": "Read the current local time, including the UTC offset.",
	"parameters": {"type": "object", "properties": {}, "required": [], "additionalProperties": false}
}`)

func currentTime(ctx context.Context, name, args string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var params map[string]json.RawMessage
	if name != "current_time" || json.Unmarshal([]byte(args), &params) != nil || params == nil || len(params) != 0 {
		return "Tool error: expected current_time with empty arguments {}.", nil
	}
	return time.Now().Format(time.RFC3339), nil
}

type modelEvent struct {
	Type     string          `json:"type"`
	Delta    string          `json:"delta"`
	Item     json.RawMessage `json:"item"`
	Message  string          `json:"message"`
	Response struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"response"`
}

type retryable struct{ error }

// 每次只取一个条目。慢网关读取请求体时，数据库连接可供其他会话使用。
func (a *agent) writeInput(ctx context.Context, w io.Writer, sid int64, cfg config) error {
	header := map[string]any{"model": cfg.Model, "instructions": cfg.Instructions, "tools": []any{clockTool}, "store": false, "stream": true}
	if cfg.Effort != nil {
		header["reasoning"] = map[string]string{"effort": *cfg.Effort}
	}
	b, err := json.Marshal(header)
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintf(w, `%s,"input":[`, b[:len(b)-1]); err != nil {
		return err
	}
	var pos int64
	for {
		var raw string
		var next int64
		err := a.db.QueryRowContext(ctx, `SELECT c.position,i.payload FROM context_items c JOIN items i ON i.id=c.item_id WHERE c.session_id=? AND c.position>? AND c.draft=0 ORDER BY c.position LIMIT 1`, sid, pos).Scan(&next, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if pos != 0 {
			if _, err = io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if _, err = io.WriteString(w, raw); err != nil {
			return err
		}
		pos = next
	}
	_, err = io.WriteString(w, "]}")
	return err
}

func (a *agent) stream(ctx context.Context, sid int64, cfg config, handle func(modelEvent) error) error {
	body, writer := io.Pipe()
	defer body.Close()
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(cfg.BaseURL, "/")+"/responses", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	go func() { writer.CloseWithError(a.writeInput(ctx, writer, sid, cfg)) }()
	res, err := a.client.Do(req)
	if err != nil {
		return retryable{err}
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		detail, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		err := fmt.Errorf("model HTTP %d: %s", res.StatusCode, detail)
		if res.StatusCode == 408 || res.StatusCode == 429 || res.StatusCode >= 500 {
			return retryable{err}
		}
		return err
	}
	if !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		return errors.New("expected a Responses event stream")
	}
	reader := bufio.NewReader(res.Body)
	var data []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return retryable{err}
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			continue
		}
		if line != "" || len(data) == 0 {
			continue
		}
		payload := strings.Join(data, "\n")
		data = nil
		if payload == "[DONE]" {
			return retryable{io.ErrUnexpectedEOF}
		}
		var event modelEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return fmt.Errorf("invalid Responses event: %w", err)
		}
		switch event.Type {
		case "response.completed":
			// 完整条目已在 output_item.done 时落库，这里只确认本次响应结束。
			return nil
		case "response.failed", "response.incomplete", "error":
			if event.Response.Error != nil {
				event.Message = event.Response.Error.Message
			}
			return fmt.Errorf("%s: %s", event.Type, event.Message)
		default:
			if err := handle(event); err != nil {
				return err
			}
		}
	}
}
