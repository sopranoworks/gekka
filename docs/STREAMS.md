# Gekka Streams — Developer Guide (v0.12.0)

The `stream` package provides a pull-based reactive-streams DSL with demand-driven back-pressure.  Every pipeline is a chain of three primitive stages — **Source**, **Flow**, and **Sink** — connected through an internal `iterator` interface that lets the downstream drive throughput.  No element is ever produced unless the downstream has requested it.

---

## Table of Contents

1. [Core Abstractions](#1-core-abstractions)
2. [Basic Operators](#2-basic-operators)
3. [Typed Actor Integration](#3-typed-actor-integration)
4. [Supervision & Error Handling](#4-supervision--error-handling)
5. [Flow Control](#5-flow-control)
6. [Graph Operators](#6-graph-operators)
7. [Resilience](#7-resilience)
8. [File IO](#8-file-io)
9. [Remote Streaming — StreamRefs](#9-remote-streaming--streamrefs)
10. [Wire Protocol Reference](#10-wire-protocol-reference)

---

## 1. Core Abstractions

| Type | Description |
|------|-------------|
| `Source[T, Mat]` | One output port; emits elements of type `T`; materializes to `Mat` |
| `Flow[In, Out, Mat]` | One input + one output; transforms `In` → `Out`; materializes to `Mat` |
| `Sink[In, Mat]` | One input port; consumes `T`; materializes to `Mat` |
| `RunnableGraph[Mat]` | Fully connected topology, ready to execute |
| `Materializer` | Engine that executes a graph (`SyncMaterializer`, `ActorMaterializer`) |
| `NotUsed` | Materialized value for stages that produce no meaningful result |

**Connecting stages:**

```go
// Via connects a Source to a Flow:
src2 := stream.Via(src, flow)

// To connects a Source (or Via result) to a Sink:
graph := stream.Via(src, flow).To(sink)

// RunWith drives the graph and returns the Sink's materialized value:
result, err := stream.RunWith(src, sink, stream.ActorMaterializer{})
```

---

## 2. Basic Operators

### Source constructors

```go
stream.FromSlice([]int{1, 2, 3})               // finite slice source
stream.FromIteratorFunc(fn func() (T, bool, error)) // custom pull function
stream.Repeat(elem)                             // infinite; combine with Take
stream.Failed[T](err)                           // immediately fails with err
```

### Flow operators (element-type preserved)

```go
stream.Map(func(n int) int { return n * 2 })
stream.MapE(func(n int) (int, error) { ... })  // propagates errors
stream.Filter(func(n int) bool { return n > 0 })
stream.Take(n)                                  // emit at most n elements
stream.Drop(n)                                  // skip first n elements
stream.Delay(d time.Duration)                   // per-element delay
```

### Flow operators (type-changing — package-level functions)

```go
stream.MapAsync(src, parallelism, func(n int) <-chan string { ... })
stream.FilterMap(src, func(n int) (string, bool) { ... })
stream.StatefulMapConcat(src, newState, func(s State, n int) (State, []Out) { ... })
```

### Sink constructors

```go
stream.Collect[T]()           // accumulates all elements into []T
stream.Foreach(func(T) {})    // calls fn for each element; no result
stream.ForeachErr(func(T) error) // stops stream on error
stream.Ignore[T]()            // discards all elements
stream.Head[T]()              // captures the first element as *T
```

### Running a graph

```go
// Keep the Source's mat value:
mat, err := stream.Via(src, flow).To(sink).Run(stream.ActorMaterializer{})

// Keep the Sink's mat value:
result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
```

---

## 3. Typed Actor Integration

### Ask — sequential per-element request-reply

`Ask` sends one request to a typed actor per upstream element and emits the reply.  The actor controls throughput: the next element is not pulled from upstream until the reply arrives.

```go
type DoubleReq struct {
    N      int
    ReplyTo typed.TypedActorRef[int]
}

flow := stream.Ask(
    doubleActor,           // typed.TypedActorRef[DoubleReq]
    5*time.Second,
    func(n int, replyTo typed.TypedActorRef[int]) DoubleReq {
        return DoubleReq{N: n, ReplyTo: replyTo}
    },
)

result, err := stream.RunWith(
    stream.Via(stream.FromSlice([]int{1, 2, 3}), flow),
    stream.Collect[int](),
    stream.ActorMaterializer{},
)
// result == []int{2, 4, 6}
```

### FromTypedActorRef — fire-and-forget sink

```go
sink := stream.FromTypedActorRef(processorRef)
stream.Via(src, flow).To(sink).Run(stream.ActorMaterializer{})
```

### ActorSource — push from any goroutine

```go
src, ref, complete := stream.ActorSource[int](bufferSize)
go func() {
    for i := range 100 { ref.Tell(i) }
    complete()
}()
result, _ := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
```

Use `ActorSourceWithStrategy` for `OverflowDropHead`, `OverflowFail`, or `OverflowBackpressure`.

---

## 4. Supervision & Error Handling

### Decider-based supervision

```go
decider := func(err error) stream.Directive {
    if errors.Is(err, ErrTransient) {
        return stream.Resume  // skip the failing element
    }
    return stream.Stop        // fail the stream (default)
}

src := stream.FromSlice(risky).WithSupervisionStrategy(decider)
// Also available on Flow and Sink
```

Directives: `stream.Stop` (fail), `stream.Resume` (skip), `stream.Restart` (reset state).

### Recover — emit a fallback element on error

```go
recoveredFlow := stream.Map(transform).Recover(func(err error) int {
    return -1 // emitted instead of propagating the error
})
```

### RecoverWith — failover to a backup source

```go
failoverFlow := stream.Map(transform).RecoverWith(func(err error) stream.Source[int, stream.NotUsed] {
    return backup
})
```

### RestartSource / RestartFlow

```go
settings := stream.RestartSettings{
    MinBackoff:   100 * time.Millisecond,
    MaxBackoff:   10 * time.Second,
    RandomFactor: 0.2,
    MaxRestarts:  -1, // unlimited
}

resilientSrc := stream.RestartSource(settings, func() stream.Source[int, stream.NotUsed] {
    return connectToDatabase()
})
```

---

## 5. Flow Control

### Async boundary

Splits the pipeline at a goroutine boundary.  Upstream runs in a dedicated goroutine; back-pressure is enforced via a bounded channel of capacity `DefaultAsyncBufSize` (16).

```go
stream.Via(src.Async(), flow).To(sink.Async())
```

Use `ActorMaterializer` when the graph contains async boundaries.

### Buffer

```go
// OverflowBackpressure (default), OverflowDropTail, OverflowDropHead, OverflowFail
src.Buffer(64, stream.OverflowBackpressure)
```

### Throttle

```go
// At most 100 elements per second with a burst allowance of 10.
src.Throttle(100, time.Second, 10)
```

### Delay

```go
src.Delay(50 * time.Millisecond) // per-element delay
```

### KillSwitch

```go
flow, ks := stream.NewKillSwitch[int]()

go func() {
    stream.RunWith(stream.Via(src, flow), sink, m)
}()

time.Sleep(5 * time.Second)
ks.Shutdown() // stops cleanly
// or: ks.Abort(err) // fails with err
```

**SharedKillSwitch** controls multiple streams simultaneously:

```go
ks := stream.NewSharedKillSwitch()
go stream.RunWith(stream.Via(src1, stream.SharedKillSwitchFlow[int](ks)), sink1, m)
go stream.RunWith(stream.Via(src2, stream.SharedKillSwitchFlow[string](ks)), sink2, m)
ks.Shutdown() // stops both
```

---

## 6. Graph Operators

### Merge

Fan-in from multiple sources; emission order is non-deterministic.

```go
merged := stream.Merge(src1, src2, src3)
```

### Broadcast

Fan-out one source to N independent sinks (each receives a full copy).

```go
branches := stream.Broadcast(src, 3, 16) // fanOut=3, bufferSize=16
var wg sync.WaitGroup
for _, b := range branches {
    wg.Add(1)
    go func(s stream.Source[int, stream.NotUsed]) {
        defer wg.Done()
        stream.RunWith(s, mySink, m)
    }(b)
}
wg.Wait()
```

### Balance

Fan-out with work-stealing: each element goes to exactly one output.

```go
workers := stream.Balance(src, 4, 32)
```

### Zip / ZipWith

```go
pairs := stream.Zip(srcA, srcB)  // Source[Pair[A,B]]

sums := stream.ZipWith(srcInts, srcFloats, func(a int, b float64) float64 {
    return float64(a) + b
})
```

### Concat

Drain `s1` completely before starting `s2`.

```go
stream.Concat(header, body)
```

### Grouped / GroupedWithin

```go
stream.Grouped(src, 10)                               // batches of 10
stream.GroupedWithin(src, 10, 100*time.Millisecond)   // count OR time, whichever first
```

### GroupBy

Split a source into keyed sub-streams.

```go
substreams := stream.GroupBy(src, 8, func(t Transaction) string {
    return t.AccountID
})
```

Each element of `substreams` is a `SubStream[T]{Key string, Source Source[T, NotUsed]}`.

---

## 7. Resilience

### RestartSource / RestartFlow

See [Section 4](#4-supervision--error-handling).  Exponential backoff with optional jitter and max-restart cap:

```go
settings := stream.RestartSettings{
    MinBackoff:   50 * time.Millisecond,
    MaxBackoff:   30 * time.Second,
    RandomFactor: 0.1,
    MaxRestarts:  5, // -1 for unlimited
}
```

---

## 8. File IO

### SourceFromFile

Reads a file in chunks and emits each chunk as `[]byte`.  A `Future[int64]` delivers the total bytes read when the source completes.

```go
src, bytesFut := stream.SourceFromFile("/var/log/app.log", 4096)
_, err := stream.RunWith(stream.Via(src, processFlow), stream.Ignore[[]byte](), m)
total, _ := bytesFut.Get() // blocks until done; total = bytes read
```

### SinkToFile

Writes incoming `[]byte` chunks to a file.  The materialized `*Future[int64]` delivers the total bytes written.

```go
fut, err := stream.RunWith(chunkSource, stream.SinkToFile("/tmp/out.bin"), m)
written, writeErr := fut.Get()
```

### Future[T]

A single-assignment promise returned by file IO operators.  `Get()` blocks until resolved.

```go
func (f *Future[T]) Get() (T, error)
```

---

## 9. Remote Streaming — StreamRefs

StreamRefs allow a `Source` or `Sink` to be shared across the network using the Artery StreamRef wire protocol.  Back-pressure is preserved end-to-end.

### TypedSourceRef — share a Source across nodes

**Producer side** — materialize the source as a stage actor:

```go
ref, stop, err := stream.ToSourceRef(src, encode, decode)
// ref.Ref is a serializable SourceRef (host:port) — send it via actor Tell/Ask
actorRef.Tell(ref)
// stop() cancels the listener if no consumer is expected
```

**Consumer side** — subscribe to the remote source:

```go
// received ref from remote via actor message
result, err := stream.RunWith(
    stream.FromSourceRef(ref),
    stream.Collect[int](),
    stream.ActorMaterializer{},
)
```

**Back-pressure**: the stage actor (TCP server) sends elements only when the consumer has granted demand via `CumulativeDemand` frames.  A slow consumer naturally stalls the producer.

### TypedSinkRef — share a Sink across nodes

**Consumer side** — materialize the sink as a stage actor:

```go
ref, stop, err := stream.ToSinkRef(sink, decode, encode)
actorRef.Tell(ref) // send to the remote producer
```

**Producer side** — push elements to the remote sink:

```go
// received ref from remote via actor message
_, err := stream.RunWith(src, stream.FromSinkRef(ref), stream.ActorMaterializer{})
```

### TLS-encrypted TCP streams

```go
// Listener side
listener, err := stream.NewTcpListenerWithTLS("0.0.0.0:5000", decode, tlsServerConfig)

// Sender side
sink := stream.TcpOutWithTLS(ref, encode, tlsClientConfig)
```

### Codecs — Encoder[T] / Decoder[T]

```go
type Encoder[T any] func(T) (msgBytes []byte, serializerID int32, manifest []byte, err error)
type Decoder[T any] func(msgBytes []byte, serializerID int32, manifest []byte) (T, error)

// Built-in helpers:
stream.ByteArrayEncode  // raw []byte, serializer ID 4
stream.ByteArrayDecode

// JSON example:
func jsonEncode[T any](v T) ([]byte, int32, []byte, error) {
    b, err := json.Marshal(v)
    return b, 9, nil, err
}
```

---

## 10. Wire Protocol Reference

StreamRefs use a binary framing format compatible with Pekko's `StreamRefSerializer` (serializer ID 36).

**Frame layout:**

```
┌──────────────────────┬───────────┬───────────────────┐
│  length (4 B, LE)    │ manifest  │  protobuf payload │
│  = 1 + len(payload)  │  (1 byte) │  (variable)       │
└──────────────────────┴───────────┴───────────────────┘
```

**Manifest codes:**

| Byte | Name | Direction | Description |
|------|------|-----------|-------------|
| `A`  | `SequencedOnNext` | producer → consumer | Data frame with sequence number and payload |
| `B`  | `CumulativeDemand` | consumer → producer | Monotonically increasing total demand grant |
| `C`  | `RemoteStreamFailure` | either | Terminates with an error cause string |
| `D`  | `RemoteStreamCompleted` | producer → consumer | Normal end-of-stream |
| `E`  | `SourceRef` | Artery message | Contains the origin actor path |
| `F`  | `SinkRef` | Artery message | Contains the target actor path |
| `G`  | `OnSubscribeHandshake` | consumer → producer | First frame on a new connection |
| `H`  | `Ack` | either | Reserved |
