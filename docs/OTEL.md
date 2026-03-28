# OpenTelemetry (OTEL) Integration (v0.14.0)

Gekka ships with built-in hooks for distributed tracing and metrics via the
[OpenTelemetry](https://opentelemetry.io/) API. All instrumentation defaults
to **no-ops with zero overhead** until you register a real provider.

---

## Quick Start

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

    "github.com/sopranoworks/gekka"
    "github.com/sopranoworks/gekka/telemetry"
    gekkaotel "github.com/sopranoworks/gekka/telemetry/otel"
)

func main() {
    ctx := context.Background()

    // 1. Configure the OTEL SDK (application-specific).
    exporter, _ := otlptracehttp.New(ctx)
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithSampler(sdktrace.AlwaysSample()),
    )
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))
    defer tp.Shutdown(ctx)

    // 2. Register with gekka — a single call before any actors start.
    telemetry.SetProvider(gekkaotel.NewProvider())

    // 3. Start your cluster as normal.
    cluster, _ := gekka.NewCluster(gekka.ClusterConfig{
        SystemName: "MySystem",
        Port:       2552,
        Telemetry: gekka.TelemetryConfig{
            TracingEnabled: true,
            MetricsEnabled: true,
        },
    })
    defer cluster.Shutdown()
    // …
}
```

---

## Interfaces

The `telemetry` package defines three clean interfaces that you can implement
with any backend (OTEL, Datadog, custom):

| Interface | Purpose |
|-----------|---------|
| `telemetry.Provider` | Root factory; returns `Tracer` and `Meter` |
| `telemetry.Tracer` | Creates spans, injects/extracts W3C trace-context headers |
| `telemetry.Meter` | Creates `Counter`, `UpDownCounter`, and `Histogram` instruments |

### Provider

```go
type Provider interface {
    Tracer(instrumentationName string) Tracer
    Meter(instrumentationName string) Meter
}

// Register globally — must be called before actors start.
telemetry.SetProvider(myProvider)

// Read the current global provider.
p := telemetry.Global()
```

### Tracer

```go
type Tracer interface {
    // Start creates a child span of any span in ctx.
    Start(ctx context.Context, spanName string) (context.Context, Span)

    // Inject writes W3C headers into carrier from the active span in ctx.
    Inject(ctx context.Context, carrier map[string]string)

    // Extract reads W3C headers from carrier into a new context.
    Extract(ctx context.Context, carrier map[string]string) context.Context
}
```

### Meter

```go
type Meter interface {
    Counter(name, description, unit string) Counter
    UpDownCounter(name, description, unit string) UpDownCounter
    Histogram(name, description, unit string) Histogram
}
```

---

## Context Propagation

### Local Actor Tells

Use `ActorRef.TellCtx` to propagate the current trace into a local actor's
mailbox. The receiving actor's `Start()` loop automatically extracts the
context and starts a child span:

```go
func (a *OrderActor) Receive(msg any) {
    switch m := msg.(type) {
    case PlaceOrder:
        // Pass the current message's trace context to a downstream actor.
        inventoryRef.TellCtx(a.CurrentContext(), CheckStock{SKU: m.SKU}, a.Self())
    }
}
```

`BaseActor.CurrentContext()` returns the context associated with the message
being processed. Passing it to `TellCtx` creates an unbroken span chain:

```
PlaceOrder (HTTP handler)
  └── actor.Receive [OrderActor]
        └── actor.Receive [InventoryActor]   ← child span via TellCtx
```

### Remote Sends

For remote sends via `Cluster.Send(ctx, path, msg)` or `ActorRef.Ask`, the
`context.Context` parameter carries the active span. The context is used for
the OTEL span wrapping the Ask call itself; wire-level trace propagation
between Go and Scala/Pekko nodes is not yet supported in the Artery binary
protocol (Pekko does not define a standard trace-header slot in the Artery
envelope).

### Ask Pattern

`ActorRef.Ask` is automatically wrapped in an OTEL span:

```go
ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
defer cancel()

reply, err := ref.Ask(ctx, MyRequest{})
// → creates span "actor.Ask" with attribute "actor.path"
//   → duration covers the full round-trip
```

---

## Available Metrics

| Metric name | Type | Unit | Labels | Description |
|-------------|------|------|--------|-------------|
| `gekka_actor_mailbox_size` | UpDownCounter | `{messages}` | `actor.path` | Messages currently queued in the actor's mailbox |
| `gekka_actor_message_process_duration` | Histogram | `s` | `actor.path` | Wall-clock time spent inside `Receive` |
| `gekka_cluster_members_count` | UpDownCounter | `{members}` | `status`, `dc` | Live cluster member count, grouped by status and data-center |

### `gekka_actor_mailbox_size`

Decremented by 1 each time a message is dequeued from the mailbox.  To track
the **current** depth, pair this with an observable gauge or snapshot it
periodically.

### `gekka_actor_message_process_duration`

A histogram measuring how long each `Receive` invocation takes in seconds.
Use the P99 bucket to detect slow actors that may cause mailbox back-pressure.

### `gekka_cluster_members_count`

Updated on every `MemberUp`, `MemberLeft`, `MemberExited`, and `MemberRemoved`
cluster event.  The `status` label is one of `"up"` or `"leaving"`; `dc` is
the data-center name (defaults to `"default"`).

---

## HOCON Configuration

```hocon
gekka {
  telemetry {
    # Enable actor receive loop tracing.
    tracing.enabled = true

    # Enable mailbox, duration, and member count metrics.
    metrics.enabled = true
  }
}
```

Parse from a HOCON file:

```go
cfg, _ := gekka.LoadConfig("application.conf")
// cfg.Telemetry.TracingEnabled and cfg.Telemetry.MetricsEnabled are set
// from the HOCON keys above.
```

> **Note**: setting these to `true` only activates the hooks inside gekka.
> You must also call `telemetry.SetProvider(gekkaotel.NewProvider())` (where
> `gekkaotel` is `github.com/sopranoworks/gekka/telemetry/otel`) and
> configure the OTEL SDK with exporters to actually emit data.

---

## Custom Provider

Implement `telemetry.Provider` to use any backend:

```go
type datadogProvider struct{}

func (p *datadogProvider) Tracer(name string) telemetry.Tracer {
    return &datadogTracer{tracer: dd.StartSpan}
}
func (p *datadogProvider) Meter(name string) telemetry.Meter {
    return &datadogMeter{}
}

telemetry.SetProvider(&datadogProvider{})
```

---

## Noop (Default)

When no provider is registered, all calls go through `telemetry.NoopProvider`,
which returns singleton no-op values. The overhead is limited to a few
function-pointer dereferences per message — negligible in production.

---

## Span Names

| Span | When created |
|------|-------------|
| `actor.Receive` | Once per message in every actor's receive loop |
| `actor.Ask` | Once per `ActorRef.Ask` or `ActorSelection.Ask` call |

---

## Interoperability with Scala/Pekko

W3C trace-context propagation between Go (gekka) and Scala (Pekko) actors
over Artery TCP is not yet supported at the wire level — the Artery binary
framing format does not have a reserved header slot for trace metadata.

Span linking across the language boundary can be achieved by encoding trace
headers into the application-level message payload (e.g. a JSON wrapper) and
manually extracting them in the receiving actor:

```scala
// Scala side
class TracedActor extends Actor {
  def receive: Receive = {
    case TracedMessage(traceParent, payload) =>
      val span = tracer.spanBuilder("actor.Receive")
        .setParent(otel.propagators.textMapPropagator.extract(
          Context.current(), traceParent, TextMapGetter.forMap))
        .startSpan()
      try handle(payload)
      finally span.end()
  }
}
```

```go
// Go side — wrap message with trace headers
carrier := make(map[string]string)
telemetry.GetTracer("myapp").Inject(ctx, carrier)
ref.Tell(TracedMessage{TraceParent: carrier["traceparent"], Payload: payload})
```
