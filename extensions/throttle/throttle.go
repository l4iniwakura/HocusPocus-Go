// Package throttle limits new connections per client address.
package throttle

import (
	"context"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	"net"
	"net/http"
	"sync"
	"time"
)

type entry struct {
	attempts    []time.Time
	bannedUntil time.Time
}
type Throttle struct {
	mu              sync.Mutex
	entries         map[string]entry
	Limit           int
	Window, BanTime time.Duration
	ClientIP        func(*http.Request) string
}

func New() *Throttle                     { return &Throttle{Limit: 15, Window: time.Minute, BanTime: 5 * time.Minute} }
func (*Throttle) Priority() int          { return 1000 }
func (*Throttle) Handles(h hp.Hook) bool { return h == hp.OnConnect }
func (t *Throttle) Handle(_ context.Context, h hp.Hook, p *hp.Payload) error {
	if h != hp.OnConnect || t.Limit <= 0 {
		return nil
	}
	host, _, err := net.SplitHostPort(p.Request.RemoteAddr)
	if err != nil {
		host = p.Request.RemoteAddr
	}
	if t.ClientIP != nil {
		host = t.ClientIP(p.Request)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries == nil {
		t.entries = map[string]entry{}
	}
	window := t.Window
	if window <= 0 {
		window = time.Minute
	}
	ban := t.BanTime
	if ban <= 0 {
		ban = 5 * time.Minute
	}
	now := time.Now()
	v := t.entries[host]
	if now.Before(v.bannedUntil) {
		return hp.ErrForbidden
	}
	attempts := v.attempts[:0]
	for _, at := range v.attempts {
		if now.Sub(at) < window {
			attempts = append(attempts, at)
		}
	}
	v.attempts = append(attempts, now)
	if len(v.attempts) > t.Limit {
		v.bannedUntil = now.Add(ban)
		t.entries[host] = v
		return hp.ErrForbidden
	}
	t.entries[host] = v
	// Bound expired-IP retention without one background goroutine per limiter.
	if len(t.entries) > 4096 {
		for key, old := range t.entries {
			if !now.Before(old.bannedUntil) && (len(old.attempts) == 0 || now.Sub(old.attempts[len(old.attempts)-1]) >= window) {
				delete(t.entries, key)
			}
		}
	}
	return nil
}
