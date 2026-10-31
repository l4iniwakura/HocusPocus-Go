package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	"github.com/l4iniwakura/HocusPocus-Go/transformer"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpstreamSignatureAndEnvelope(t *testing.T) {
	var calls atomic.Int32
	secret := []byte("test-webhook-secret")
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mac := hmac.New(sha256.New, secret)
		mac.Write(body)
		if got := r.Header.Get("X-Hocuspocus-Signature-256"); got != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
			t.Error(got)
		}
		var envelope struct {
			Event   string         `json:"event"`
			Payload map[string]any `json:"payload"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Error(err)
		}
		if envelope.Event != "change" || envelope.Payload["documentName"] != "note" {
			t.Error(envelope)
		}
		calls.Add(1)
		w.WriteHeader(204)
	}))
	defer endpoint.Close()
	webhook := New(endpoint.URL, secret)
	webhook.Debounce = 0
	cfg := hp.DefaultConfiguration()
	cfg.Extensions = []hp.Extension{webhook}
	server, err := hp.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	connection, err := server.OpenDirect(context.Background(), "note", nil)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := (transformer.Prosemirror{}).ToYDoc(transformer.Node{Type: "doc", Content: []transformer.Node{{Type: "paragraph"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Documents()[0].Apply(context.Background(), doc.EncodeStateAsUpdate(), hp.Origin{Source: "local"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
	_ = connection.Disconnect(context.Background(), true)
}
func TestDebouncedEventsFlushOnShutdown(t *testing.T) {
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(204) }))
	defer endpoint.Close()
	w := New(endpoint.URL, nil)
	w.Debounce = time.Hour
	w.MaxWait = time.Hour
	cfg := hp.DefaultConfiguration()
	cfg.Extensions = []hp.Extension{w}
	s, err := hp.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.schedule("note", []byte(`{"event":"change"}`))
	w.schedule("note", []byte(`{"event":"change"}`))
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}
func TestConnectFailureDeniesAccess(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	defer endpoint.Close()
	w := New(endpoint.URL, nil)
	w.Events = map[hp.Hook]bool{hp.OnConnect: true}
	if err := w.Handle(context.Background(), hp.OnConnect, &hp.Payload{DocumentName: "note"}); err != hp.ErrForbidden {
		t.Fatal(err)
	}
}
