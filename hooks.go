package hocuspocus

import (
	"context"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
	"net/http"
	"time"
)

// Hook identifies the upstream lifecycle event. Hooks within one document run
// sequentially; extensions must support calls for different documents in parallel.
type Hook string

const (
	OnConfigure              Hook = "onConfigure"
	OnListen                 Hook = "onListen"
	OnUpgrade                Hook = "onUpgrade"
	OnConnect                Hook = "onConnect"
	Connected                Hook = "connected"
	OnAuthenticate           Hook = "onAuthenticate"
	OnTokenSync              Hook = "onTokenSync"
	OnCreateDocument         Hook = "onCreateDocument"
	OnLoadDocument           Hook = "onLoadDocument"
	AfterLoadDocument        Hook = "afterLoadDocument"
	BeforeHandleMessage      Hook = "beforeHandleMessage"
	AfterHandleMessage       Hook = "afterHandleMessage"
	BeforeHandleAwareness    Hook = "beforeHandleAwareness"
	BeforeSync               Hook = "beforeSync"
	BeforeBroadcastStateless Hook = "beforeBroadcastStateless"
	OnStateless              Hook = "onStateless"
	OnChange                 Hook = "onChange"
	OnStoreDocument          Hook = "onStoreDocument"
	AfterStoreDocument       Hook = "afterStoreDocument"
	OnAwarenessUpdate        Hook = "onAwarenessUpdate"
	OnRequest                Hook = "onRequest"
	OnDisconnect             Hook = "onDisconnect"
	BeforeUnloadDocument     Hook = "beforeUnloadDocument"
	AfterUnloadDocument      Hook = "afterUnloadDocument"
	OnDestroy                Hook = "onDestroy"
)

type Origin struct {
	Source         string
	Connection     *Connection
	Context        any
	SkipStoreHooks bool
}
type AwarenessState struct {
	Clock uint64
	State map[string]any
}
type Payload struct {
	Instance                *Server
	Document                *Document
	DocumentName            string
	ClientsCount            int
	Connection              *Connection
	Request                 *http.Request
	Response                http.ResponseWriter
	Context                 any
	Token                   string
	ProviderVersion         string
	Update                  []byte
	State                   []byte
	Type                    uint64
	Message                 string
	Origin                  Origin
	States                  map[uint64]*AwarenessState
	RemovedClients          map[uint64]uint64
	Added, Updated, Removed []uint64
	Error                   error
	cleanups                []func()
}

// Defer registers cleanup after an operation (including failed persistence).
func (p *Payload) Defer(fn func()) { p.cleanups = append(p.cleanups, fn) }
func (p *Payload) finish() {
	for i := len(p.cleanups) - 1; i >= 0; i-- {
		p.cleanups[i]()
	}
}

type Extension interface {
	Priority() int
	Handle(context.Context, Hook, *Payload) error
}
type ExtensionFunc func(context.Context, Hook, *Payload) error

func (f ExtensionFunc) Priority() int                                      { return 100 }
func (f ExtensionFunc) Handle(c context.Context, h Hook, p *Payload) error { return f(c, h, p) }

type Hooks map[Hook]func(context.Context, *Payload) error

func (h Hooks) Priority() int { return 100 }
func (h Hooks) Handle(c context.Context, k Hook, p *Payload) error {
	if f := h[k]; f != nil {
		return f(c, p)
	}
	return nil
}

type Configuration struct {
	Name                            string
	Timeout                         time.Duration
	Debounce                        time.Duration
	MaxDebounce                     time.Duration
	UnloadImmediately               bool
	MaxUnauthenticatedQueueSize     int
	MaxUnauthenticatedQueueMessages int
	MaxPendingDocuments             int
	MaxConnections                  int
	MaxMessageBytes                 int64
	OutboundCapacity                int
	DocumentQueueCapacity           int
	FlushDelay                      time.Duration
	FlushMaxBytes                   int
	DisableGC                       bool
	GCFilter                        func(*crdt.Item) bool
	AllowedOrigins                  []string
	Extensions                      []Extension
	Hooks                           Hooks
}

func DefaultConfiguration() Configuration {
	return Configuration{
		Timeout: 60 * time.Second, Debounce: 2 * time.Second, MaxDebounce: 10 * time.Second,
		UnloadImmediately: true, MaxUnauthenticatedQueueSize: 5 << 20,
		MaxUnauthenticatedQueueMessages: 1000, MaxPendingDocuments: 100,
		MaxMessageBytes: 16 << 20, OutboundCapacity: 256, DocumentQueueCapacity: 256,
		FlushMaxBytes: 1 << 20,
	}
}
