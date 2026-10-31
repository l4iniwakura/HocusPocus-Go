// Package webhook implements the upstream event envelope and HMAC signature.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
	"github.com/l4iniwakura/HocusPocus-Go/transformer"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type pending struct {
	started time.Time
	timer   *time.Timer
	body    []byte
}
type Webhook struct {
	URL               string
	Secret            []byte
	Client            *http.Client
	Events            map[hp.Hook]bool
	Debounce, MaxWait time.Duration
	Transformer       transformer.Prosemirror
	mu                sync.Mutex
	pending           map[string]*pending
}

func (*Webhook) Priority() int { return 100 }
func (w *Webhook) enabled(h hp.Hook) bool {
	if w.Events == nil {
		return h == hp.OnChange
	}
	return w.Events[h]
}
func (w *Webhook) Handles(h hp.Hook) bool {
	return h == hp.OnConfigure || h == hp.OnDestroy || w.enabled(h)
}
func New(url string, secret []byte) *Webhook {
	return &Webhook{URL: url, Secret: secret, Debounce: 2 * time.Second, MaxWait: 10 * time.Second}
}
func (w *Webhook) Handle(ctx context.Context, h hp.Hook, p *hp.Payload) error {
	if h == hp.OnConfigure {
		if w.URL == "" {
			return fmt.Errorf("webhook URL required")
		}
		w.pending = map[string]*pending{}
		return nil
	}
	if h == hp.OnDestroy {
		return w.flush(ctx)
	}
	if !w.enabled(h) {
		return nil
	}
	events := map[hp.Hook]string{hp.OnChange: "change", hp.OnConnect: "connect", hp.OnLoadDocument: "create", hp.OnDisconnect: "disconnect"}
	event, ok := events[h]
	if !ok {
		return fmt.Errorf("unsupported webhook event %s", h)
	}
	payload := map[string]any{"documentName": p.DocumentName, "requestHeaders": map[string]any{}, "requestParameters": map[string]string{}}
	if p.Request != nil {
		parameters := map[string]string{}
		for key, values := range p.Request.URL.Query() {
			if len(values) > 0 {
				parameters[key] = values[len(values)-1]
			}
		}
		payload["requestParameters"] = parameters
	}
	if h == hp.OnChange || h == hp.OnDisconnect {
		payload["context"] = p.Context
	}
	if h == hp.OnChange {
		document, err := w.Transformer.FromFields(p.Document.Doc)
		if err != nil {
			return err
		}
		payload["document"] = document
	}
	body, err := json.Marshal(map[string]any{"event": event, "payload": payload})
	if err != nil {
		return err
	}
	if h == hp.OnChange && w.Debounce > 0 {
		w.schedule(p.DocumentName, body)
		return nil
	}
	result, err := w.send(ctx, body)
	if err != nil {
		if h == hp.OnConnect {
			return hp.ErrForbidden
		}
		slog.ErrorContext(ctx, "webhook failed", "event", event, "error", err)
		return nil
	}
	if h == hp.OnConnect && len(result) > 0 {
		var value any
		if json.Unmarshal(result, &value) == nil {
			p.Context = value
		}
	}
	if h == hp.OnLoadDocument && len(result) > 0 {
		fields := map[string]transformer.Node{}
		if err = json.Unmarshal(result, &fields); err != nil {
			return nil
		}
		for name, node := range fields {
			if !p.Document.IsEmpty(name) {
				continue
			}
			doc, e := w.Transformer.ToYDoc(node, name)
			if e != nil {
				return e
			}
			if e = crdt.ApplyUpdateV1(p.Document.Doc, crdt.EncodeStateAsUpdateV1(doc, nil), nil); e != nil {
				return e
			}
		}
	}
	return nil
}
func (w *Webhook) send(ctx context.Context, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	mac := hmac.New(sha256.New, w.Secret)
	_, _ = mac.Write(body)
	req.Header.Set("X-Hocuspocus-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	client := w.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("webhook status %d", response.StatusCode)
	}
	return io.ReadAll(io.LimitReader(response.Body, 2<<20))
}
func (w *Webhook) schedule(name string, body []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending == nil {
		w.pending = map[string]*pending{}
	}
	old := w.pending[name]
	started := time.Now()
	if old != nil {
		old.timer.Stop()
		started = old.started
	}
	delay := w.Debounce
	if w.MaxWait > 0 {
		remaining := time.Until(started.Add(w.MaxWait))
		if remaining < delay {
			delay = remaining
		}
	}
	entry := &pending{started: started, body: body}
	entry.timer = time.AfterFunc(delay, func() {
		w.mu.Lock()
		if w.pending[name] != entry {
			w.mu.Unlock()
			return
		}
		delete(w.pending, name)
		w.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := w.send(ctx, body); err != nil {
			slog.Error("webhook failed", "document", name, "error", err)
		}
	})
	w.pending[name] = entry
}
func (w *Webhook) flush(ctx context.Context) error {
	w.mu.Lock()
	entries := w.pending
	w.pending = map[string]*pending{}
	for _, entry := range entries {
		entry.timer.Stop()
	}
	w.mu.Unlock()
	for _, entry := range entries {
		if _, err := w.send(ctx, entry.body); err != nil {
			return err
		}
	}
	return nil
}
