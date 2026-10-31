// Package logger provides structured lifecycle logging without document contents.
package logger

import (
	"context"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	"log/slog"
)

type Logger struct{ Log *slog.Logger }

func (*Logger) Priority() int          { return 100 }
func (*Logger) Handles(h hp.Hook) bool { return h != hp.OnAuthenticate }
func (l *Logger) Handle(ctx context.Context, h hp.Hook, p *hp.Payload) error {
	log := l.Log
	if log == nil {
		log = slog.Default()
	}
	log.DebugContext(ctx, "hocuspocus lifecycle", "hook", h, "document", p.DocumentName)
	return nil
}
