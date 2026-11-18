package main

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schema string

func openStore(path string) (*sql.DB, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	fresh := err == nil
	if fresh {
		err = file.Close()
	}
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw&_journal_mode=MEMORY&_locking_mode=EXCLUSIVE&_foreign_keys=on&_txlock=immediate"}
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return nil, err
	}
	// SQLite 的连接级设置在这个连接上一直有效。网络请求期间释放连接。
	db.SetMaxOpenConns(1)
	err = transaction(db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`PRAGMA temp_store=MEMORY`); err != nil {
			return err
		}
		if fresh {
			if _, err := tx.Exec(schema); err != nil {
				return err
			}
		} else if err := validateSchema(tx); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE sessions SET status='paused',last_error='daemon restarted' WHERE status IN ('running','pausing')`)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func validateSchema(tx *sql.Tx) error {
	ref, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}
	defer ref.Close()
	if _, err = ref.Exec(schema); err != nil {
		return err
	}
	const definitions = `SELECT name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`
	want, err := ref.Query(definitions)
	if err != nil {
		return err
	}
	defer want.Close()
	got, err := tx.Query(definitions)
	if err != nil {
		return err
	}
	defer got.Close()
	for want.Next() {
		var name, ddl, actual, actualDDL string
		if err := want.Scan(&name, &ddl); err != nil {
			return err
		}
		if !got.Next() {
			return fmt.Errorf("missing schema object %s", name)
		}
		if err := got.Scan(&actual, &actualDDL); err != nil {
			return err
		}
		if name != actual || ddl != actualDDL {
			return fmt.Errorf("schema mismatch: expected %s, found %s", name, actual)
		}
	}
	if got.Next() {
		var name, ddl string
		if err := got.Scan(&name, &ddl); err != nil {
			return err
		}
		return fmt.Errorf("unexpected schema object %s", name)
	}
	return errors.Join(want.Err(), got.Err())
}

func transaction(db *sql.DB, f func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// 查看队列和消费队列共用这条筛选条件。撤回记录永远留在原始日志。
const pending = ` FROM items i JOIN sessions s ON s.id=i.session_id
 WHERE s.id=? AND i.kind='user_message' AND i.id>s.user_queue_cursor
 AND NOT EXISTS (SELECT 1 FROM items w WHERE w.kind='withdrawal' AND w.target_id=i.id)`

func includePending(tx *sql.Tx, sid int64) (bool, error) {
	result, err := tx.Exec(`INSERT INTO context_items(session_id,item_id) SELECT i.session_id,i.id`+pending+` ORDER BY i.id`, sid)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(`UPDATE sessions SET user_queue_cursor=coalesce((SELECT max(id) FROM items WHERE session_id=? AND kind='user_message'),0) WHERE id=?`, sid, sid)
	count, _ := result.RowsAffected()
	return count > 0, err
}

func addItem(tx *sql.Tx, sid int64, kind string, payload json.RawMessage, attempt string, target any) (int64, error) {
	r, err := tx.Exec(`INSERT INTO items(session_id,kind,payload,attempt_id,target_id) VALUES(?,?,?,nullif(?,''),?)`, sid, kind, string(payload), attempt, target)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

func finishTool(tx *sql.Tx, sid, callItem int64, callID, output, attempt string) error {
	payload, _ := json.Marshal(map[string]string{"type": "function_call_output", "call_id": callID, "output": output})
	id, err := addItem(tx, sid, "function_call_output", payload, attempt, callItem)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO context_items(session_id,item_id) VALUES(?,?),(?,?)`, sid, callItem, sid, id)
	return err
}

func recoverTools(tx *sql.Tx, sid int64) error {
	for {
		var id int64
		var callID, attempt string
		err := tx.QueryRow(`SELECT id, json_extract(payload,'$.call_id'), attempt_id FROM items c
			WHERE session_id=? AND kind='function_call' AND NOT EXISTS (
				SELECT 1 FROM items r WHERE r.target_id=c.id AND r.kind='function_call_output'
			) ORDER BY id LIMIT 1`, sid).Scan(&id, &callID, &attempt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := finishTool(tx, sid, id, callID, "Execution interrupted; result unknown.", attempt); err != nil {
			return err
		}
	}
}
