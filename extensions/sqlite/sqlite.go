// Package sqlite implements the same documents table as @hocuspocus/extension-sqlite.
package sqlite

import (
	"context"
	"database/sql"
	"errors"

	hp "github.com/l4iniwakura/HocusPocus-Go"
	"github.com/l4iniwakura/HocusPocus-Go/extensions/database"
	_ "modernc.org/sqlite"
)

const Schema = `CREATE TABLE IF NOT EXISTS documents (name varchar(255) NOT NULL, data blob NOT NULL, UNIQUE(name))`

type SQLite struct {
	db       *sql.DB
	database database.Database
}

func New(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(Schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &SQLite{db: db}
	s.database.Storage = s
	return s, nil
}
func (*SQLite) Priority() int            { return 100 }
func (s *SQLite) Handles(h hp.Hook) bool { return h == hp.OnDestroy || s.database.Handles(h) }
func (s *SQLite) Handle(ctx context.Context, h hp.Hook, p *hp.Payload) error {
	if h == hp.OnDestroy {
		return s.Close()
	}
	return s.database.Handle(ctx, h, p)
}
func (s *SQLite) Fetch(ctx context.Context, name string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, "SELECT data FROM documents WHERE name = ? ORDER BY rowid DESC", name).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return data, err
}
func (s *SQLite) Store(ctx context.Context, name string, state []byte) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO documents(name,data) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET data=excluded.data", name, state)
	return err
}
func (s *SQLite) Close() error { return s.db.Close() }
