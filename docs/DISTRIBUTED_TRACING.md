## Distributed Tracing

Gekka instruments the full **command → journal → projection** pipeline with
OpenTelemetry spans.  A single `TraceID` flows through all stages:

```
Command received
  └─ ShardRegion.Receive           (injects W3C TraceContext into ShardingEnvelope)
       └─ Journal.Write            (TracingJournal starts "Journal.Write" child span)
            └─ Projection.Handle  (projection runner starts "Projection.Handle" child span)
```

### Enabling Tracing

Configure the global OTel tracer provider before starting the actor system:

```go
exp, _ := jaeger.New(jaeger.WithCollectorEndpoint(
    jaeger.WithEndpoint("http://jaeger:14268/api/traces"),
))
tp := sdktrace.NewTracerProvider(
    sdktrace.WithBatcher(exp),
    sdktrace.WithResource(resource.NewWithAttributes(
        semconv.SchemaURL,
        semconv.ServiceName("my-service"),
    )),
)
otel.SetTracerProvider(tp)
otel.SetTextMapPropagator(propagation.TraceContext{})
```

Wrap the journal with `TracingJournal` to enable write/read spans:

```go
journal := persistence.NewTracingJournal(persistence.NewInMemoryJournal())
```

### Viewing Traces in Jaeger

1. Start Jaeger all-in-one: `docker run -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one`
2. Open `http://localhost:16686`
3. Select service `my-service` and search for operation `Projection.Handle`
4. Click any trace to see the full span tree from the originating command through the journal write to the projection handler

### Span Names

| Span Name | Instrumentation Scope | Description |
|---|---|---|
| `Journal.Write` | `github.com/sopranoworks/gekka/persistence` | One span per `AsyncWriteMessages` call |
| `Journal.Read` | `github.com/sopranoworks/gekka/persistence` | One span per `ReplayMessages` call |
| `Projection.Handle` | `github.com/sopranoworks/gekka/persistence/projection` | One span per event processed by a projection |

### W3C TraceContext Propagation

`TraceContext` is carried as a `map[string]string` of W3C headers
(`traceparent`, `tracestate`) at each pipeline boundary:

- `ShardingEnvelope.TraceContext` — injected by `ShardRegion` at message dispatch
- `PersistentRepr.TraceContext` — set by the persistent actor before writing
- `EventEnvelope.TraceContext` — copied from the journal into the read model

This allows projections to resume the exact trace that originated the command,
making it possible to observe the entire write-to-read latency in a single
waterfall view.
