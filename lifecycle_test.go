package hocuspocus

import (
	"context"
	"errors"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDuplicateUpdateDoesNotTriggerChangeOrStore(t *testing.T) {
	var changes, stores atomic.Int32
	s, _ := testServer(t, Hooks{OnChange: func(context.Context, *Payload) error { changes.Add(1); return nil }, OnStoreDocument: func(context.Context, *Payload) error { stores.Add(1); return nil }})
	connection, err := s.OpenDirect(context.Background(), "note", nil)
	if err != nil {
		t.Fatal(err)
	}
	d := s.Documents()[0]
	update := textUpdate("once")
	for range 3 {
		if err = d.Apply(context.Background(), update, Origin{Source: "local"}); err != nil {
			t.Fatal(err)
		}
	}
	if err = connection.Disconnect(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if changes.Load() != 1 || stores.Load() != 1 {
		t.Fatal(changes.Load(), stores.Load())
	}
}
func TestLoadCoalescingReservationsPreventEarlyUnload(t *testing.T) {
	var loads atomic.Int32
	cfg := DefaultConfiguration()
	cfg.Debounce = time.Nanosecond
	cfg.MaxDebounce = time.Nanosecond
	cfg.Hooks = Hooks{OnLoadDocument: func(context.Context, *Payload) error { loads.Add(1); return nil }}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			c, err := s.OpenDirect(context.Background(), "note", nil)
			if err != nil {
				t.Error(err)
				return
			}
			_ = c.Transact(context.Background(), func(doc *crdt.Doc) error {
				doc.Transact(func(tx *crdt.Transaction) { tx.GetMap("seen").Set(tx, "key", true) })
				return nil
			})
			_ = c.Disconnect(context.Background(), true)
		})
	}
	wg.Wait()
	if loads.Load() < 1 {
		t.Fatal("no loads")
	}
}
func TestSlowStoreDoesNotSerializeDifferentDocuments(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	s, _ := testServer(t, Hooks{OnStoreDocument: func(ctx context.Context, p *Payload) error {
		if p.DocumentName == "slow" {
			select {
			case <-entered:
			default:
				close(entered)
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}})
	a, _ := s.OpenDirect(context.Background(), "slow", nil)
	_ = s.Documents()[0].Apply(context.Background(), textUpdate("slow"), Origin{Source: "local"})
	done := make(chan struct{})
	go func() { _ = a.Disconnect(context.Background(), true); close(done) }()
	<-entered
	b, err := s.OpenDirect(context.Background(), "fast", nil)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	_ = b.Disconnect(context.Background(), true)
	close(release)
	<-done
}
func TestHooksPanicIsRejected(t *testing.T) {
	_, err := New(Configuration{Hooks: Hooks{OnConfigure: func(context.Context, *Payload) error { panic("bug") }}})
	if err == nil {
		t.Fatal("panic ignored")
	}
}
func TestShutdownReturnsPersistenceFailureAndCanRetry(t *testing.T) {
	var healthy atomic.Bool
	cfg := DefaultConfiguration()
	cfg.Hooks = Hooks{OnStoreDocument: func(context.Context, *Payload) error {
		if !healthy.Load() {
			return errors.New("offline")
		}
		return nil
	}}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.OpenDirect(context.Background(), "note", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Documents()[0].Apply(context.Background(), textUpdate("durable"), Origin{Source: "local"})
	if err = s.Shutdown(context.Background()); err == nil {
		t.Fatal("failed save hidden")
	}
	healthy.Store(true)
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestDocumentCloseKeepsOtherMultiplexedDocumentsAlive(t *testing.T) {
	_, url := testServer(t, Hooks{OnStateless: func(_ context.Context, p *Payload) error {
		if p.DocumentName == "close" {
			p.Connection.Close(4403, "revoked")
		} else {
			p.Connection.SendStateless("still alive")
		}
		return nil
	}})
	c := connect(t, url)
	authenticate(t, c, "close")
	authenticate(t, c, "keep")
	send(t, c, stringFrame("close", Stateless, "close"))
	next(t, c, Close)
	send(t, c, stringFrame("keep", Stateless, "hello"))
	name, _ := next(t, c, Stateless)
	if name != "keep" {
		t.Fatal(name)
	}
}
func TestBeforeBroadcastStatelessCanReject(t *testing.T) {
	s, _ := testServer(t, Hooks{BeforeBroadcastStateless: func(context.Context, *Payload) error { return ErrForbidden }})
	c, err := s.OpenDirect(context.Background(), "note", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Documents()[0].BroadcastStateless(context.Background(), "rejected"); err != ErrForbidden {
		t.Fatal(err)
	}
	_ = c.Disconnect(context.Background(), true)
}
func TestConfiguredBroadcastWindowCoalescesUpdates(t *testing.T) {
	s, url := testServer(t, nil)
	s.Config.FlushDelay = 30 * time.Millisecond
	a := connect(t, url)
	b := connect(t, url)
	authenticate(t, a, "note")
	authenticate(t, b, "note")
	doc := crdt.New()
	doc.Transact(func(tx *crdt.Transaction) { tx.GetText("body").Insert(tx, 0, "a", nil) })
	send(t, a, syncFrame("note", Sync, 2, doc.EncodeStateAsUpdate()))
	doc.Transact(func(tx *crdt.Transaction) { tx.GetText("body").Insert(tx, 1, "b", nil) })
	send(t, a, syncFrame("note", Sync, 2, doc.EncodeStateAsUpdate()))
	_, dec := next(t, b, Sync)
	_, _ = dec.uint()
	update, _ := dec.bytes()
	received := crdt.New()
	if err := received.ApplyUpdate(update); err != nil {
		t.Fatal(err)
	}
	if got := received.GetText("body").ToString(); got != "ab" {
		t.Fatal(got)
	}
}
