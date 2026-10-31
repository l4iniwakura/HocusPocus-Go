# HocusPocus Go

A native Go implementation of the Hocuspocus collaboration server, based on
[`@hocuspocus/server` 4.7.1](https://github.com/ueberdosis/hocuspocus/tree/v4.7.1).
It speaks the document-prefixed Hocuspocus protocol used by the original
TypeScript provider. This is an independent port, not an official Tiptap package.

The production server runs entirely in Go. Node.js is used only by compatibility
tests and the comparison benchmark.

## Run

Requires Go **1.27.3**.

```sh
go run ./cmd/hocuspocus -listen 127.0.0.1:1234
```

The standalone command uses ephemeral memory by default. To persist documents:

```sh
go run ./cmd/hocuspocus -listen 127.0.0.1:1234 -sqlite documents.sqlite
```

To connect multiple servers through Redis:

```sh
go run ./cmd/hocuspocus -listen 127.0.0.1:1234 -sqlite documents.sqlite -redis 127.0.0.1:6379
```

`REDIS_PASSWORD` supplies the Redis password. Use the same `-redis-prefix` on
nodes belonging to one cluster. Redis transports live updates; the configured
storage remains responsible for durable state. The Redis extension uses the
original Hocuspocus channel names and binary framing, including requester-only
sync replies, so Go and TypeScript nodes can exchange documents.

The CLI is an unauthenticated standalone server. Applications enforce their
session and document policy through the authentication and message hooks below.
`/health` returns `ok` on the CLI HTTP listener.

## Embed

```go
package main

import (
    "context"
    "log"
    "net"

    hp "github.com/l4iniwakura/HocusPocus-Go"
    "github.com/l4iniwakura/HocusPocus-Go/extensions/sqlite"
)

func main() {
    storage, err := sqlite.New("documents.sqlite")
    if err != nil { log.Fatal(err) }
    cfg := hp.DefaultConfiguration()
    cfg.Extensions = []hp.Extension{storage}
    cfg.Hooks = hp.Hooks{
        hp.OnAuthenticate: func(ctx context.Context, p *hp.Payload) error {
            // Replace with your session/ACL lookup. Reject until configured.
            return hp.ErrUnauthorized
        },
    }
    server, err := hp.New(cfg)
    if err != nil { log.Fatal(err) }
    listener, err := net.Listen("tcp", "127.0.0.1:1234")
    if err != nil { log.Fatal(err) }
    log.Fatal(server.Serve(listener))
}
```

Use `DefaultConfiguration()` before overriding fields. Configuration is immutable
after the server starts. Serve the same handler through an application-owned
`http.Server` with `ServeHTTP` when custom HTTP routing or TLS is required.
Call `Shutdown(ctx)` to drain connections and persist dirty documents. A failed
store is returned to the caller and retains dirty state for a retry.

## Server and extension API

- WebSocket document multiplexing, including `documentName\0sessionId` addresses.
- Token authentication, provider version metadata, read-only connections and token refresh.
- Yjs V1 sync, sync replies/status, awareness, stateless messages, ping/pong and document close.
- Priority-ordered lifecycle hooks, mutable awareness hooks and application context.
- Direct server-side connections, document merge, explicit flush and unload.
- Debounced persistence with a maximum wait; failed loads fail closed and failed
  stores retain state. A generation check prevents a slow store from marking
  newer edits clean.
- Configurable broadcast batching, shared immutable frames for equal addresses,
  bounded per-client queues and slow-reader eviction.
- Initial state catch-up, awareness and stateless relay through Redis.

`Hooks` implements `Extension`. Custom extensions implement `Priority()` and
`Handle(context.Context, Hook, *Payload)`. Implement `Handles(Hook) bool` to declare
the hooks an extension handles; this is especially important for distinguishing
extensions that require authentication. Larger priorities run first, preserving
registration order for ties. Configuration hooks run after ordinary extensions.

The hook names follow upstream:

```text
onConfigure onListen onUpgrade onConnect connected onAuthenticate onTokenSync
onCreateDocument onLoadDocument afterLoadDocument beforeHandleMessage
afterHandleMessage beforeHandleAwareness beforeSync beforeBroadcastStateless
onStateless onChange onStoreDocument afterStoreDocument onAwarenessUpdate
onRequest onDisconnect beforeUnloadDocument afterUnloadDocument onDestroy
```

An authentication hook sets `p.Context` and `p.Connection.ReadOnly`. A stateless
hook replies with `p.Connection.SendStateless`. A load hook can supply binary
state through `p.State` or mutate `p.Document.Doc`. Store hooks must persist
`p.State`: it is the immutable snapshot being committed, while the live document
may already contain newer edits. Hook errors before apply reject the message.
Post-change errors cannot roll back a CRDT transaction.

Hooks for a single document are serialized where mutation order matters.
Independent documents can run concurrently on different Go workers. Extensions
and application services must therefore be safe for concurrent calls across
documents. Never call a method that reacquires the same document mutation lock
from an in-mutation hook; use the supplied CRDT document and payload instead.

| Package | Purpose |
| --- | --- |
| `extensions/database` | Application-owned fetch/store interface and in-memory storage |
| `extensions/sqlite` | Pure-Go SQLite, compatible `documents(name,data)` table |
| `extensions/s3` | AWS SDK v2 client; S3-compatible endpoints and bounded reads |
| `extensions/redis` | Original Hocuspocus Redis framing, initial catch-up and storage locking |
| `extensions/logger` | Structured lifecycle logging through `slog` |
| `extensions/throttle` | Per-client rolling connection window and temporary bans |
| `extensions/webhook` | Original event envelope, `X-Hocuspocus-Signature-256`, debounce and lifecycle events |
| `transformer` | ProseMirror/Tiptap JSON ↔ Yjs XML without a headless editor |

The S3 extension accepts an application-configured AWS SDK client. The throttle
extension defaults to the socket peer address; configure `ClientIP` explicitly
when trusting headers from a known reverse proxy. Browser providers, React hooks,
DOM rendering and JavaScript editor extensions remain frontend packages; Go
applications supply their schema validation using `transformer.Prosemirror.Validate`.

## CRDT engine

The imported engine is ygo **v1.54.0**, commit
`64fa0b47c025c71fe5478957c85ce3eff8254b56`. Its runtime packages, tests and
interoperability fixtures are kept in `crdt`, `encoding`, `internal/anycodec` and
`testutil`, with the original MIT license in `third_party/licenses/ygo.txt`.
`crdt/UPSTREAM.json` records its provenance and local changes. Everything belongs
to the same Go module: applications can import the library without additional
`replace` directives. Its CRDT API is exported as
`github.com/l4iniwakura/HocusPocus-Go/crdt`.

The server adds public root inspection and GC-filter APIs. It also corrects the
UndoManager capture condition: a zero capture window must never merge operations,
including when two timestamps are equal. The upstream `TestFuzzRunGCMerge`
reproduced the problem on an unchanged v1.54.0 checkout. The seeded test and the
complete imported suite are retained; they are not skipped or weakened.

The two patch scripts apply to fresh, unmodified pinned upstream files. They are
maintenance helpers, not required build steps. The committed engine is already
patched. Updating it requires reviewing the small local diff and rerunning its
complete tests against JavaScript Yjs.

## Checks

Use Node **24.22.0**, npm **11.19.1**, Python 3 and Docker for the complete suite.

```sh
npm ci --ignore-scripts
npm ci --prefix testutil --ignore-scripts
python tools/check.py --race
```

The check command runs module verification, static analysis, server and imported
engine tests, real TypeScript-provider/transformer interoperability, isolated
Redis/S3 integrations, and a Go ↔ original TypeScript server test through Redis.
`--race` also checks the server and imported engine in Linux with the race
detector. The validation image includes Node so that JavaScript compatibility
tests execute there too. It also compiles and runs a separate consumer module.
Temporary containers bind only to loopback and are
removed in `finally`.

For fast Go-only iteration:

```sh
go test ./...
go vet ./...
```

These commands include the imported engine but not external-service tests or
the standalone TypeScript-provider scenarios. Use the complete check command
for acceptance. Integration tests use the
`integration` build tag and require `TEST_REDIS_ADDR` and `TEST_S3_ENDPOINT` rather
than silently skipping absent dependencies.

## Performance comparison

```sh
go build -o .data/hocuspocus ./cmd/hocuspocus
HOCUSPOCUS_GO_BINARY="$PWD/.data/hocuspocus" node tools/benchmark.mjs
```

The comparison uses separate Go and original TypeScript server processes, one
external wire client, identical incremental text updates and awaited sync ACKs.
It records 1, 8 and 32 independent documents with memory persistence. Results are
written to `.data/benchmark.json`. This measures server overhead and parallel
document processing; it does not predict application latency with PostgreSQL,
gRPC, editor projection or production network conditions.

## License

MIT. See `LICENSE`, `NOTICE` and `third_party/licenses/ygo.txt` for upstream attribution.
