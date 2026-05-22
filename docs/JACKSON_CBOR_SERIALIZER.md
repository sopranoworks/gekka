# Gekka Jackson-CBOR Serializer

The `jackson-cbor` serializer implements Pekko's wire format for `org.apache.pekko.serialization.jackson.JacksonCborSerializer`, enabling cross-language interoperability between gekka and JVM (Pekko / Akka) nodes that bind message types to jackson-cbor.

Package: `github.com/sopranoworks/gekka/serialization/jackson-cbor` (workspace sub-module under `serialization/`).

---

## Design Note — wire BOTH registration paths

> Read this before adding any new cross-language serializer to gekka. The
> Phase 3 jackson-cbor implementation hit exactly the trap described here.

Gekka has **two independent registration paths** for remote serialization, and
they serve opposite directions of traffic:

- **(a) `SerializationRegistry.typeToManifests`** (in `internal/core`) — used by
  the **inbound** decoder to map a wire frame's `(serializerId, manifest)` to a
  Go type so `FromBinary` can produce the right struct. You wire this with
  `registry.RegisterManifest(manifest, goType, serializerID)` and
  `RegisterSerializer(...)`.
- **(b) the `actor.RemoteSerializable` interface** (`ArterySerializerID() int32`
  and `ArteryManifest() string`) — checked by `actor/router.go`'s
  `prepareMessage` on the **outbound** path to decide a value's
  `(serializerId, manifest)` *before* it ever consults the registry.

**New serializers MUST wire both paths.** They are not redundant:

| Wired | Result |
|---|---|
| (a) only | Registration *looks* successful, but on **outbound** `prepareMessage` never sees the value as `RemoteSerializable`, so it silently falls back to JSON — the remote peer receives an `OpaqueJSONMessage` and ERROR-logs it. Decode works; encode is broken. |
| (b) only | Outbound encodes correctly, but the **inbound** decoder has no `manifest → Go type` entry, so incoming frames cannot be reconstructed. Encode works; decode is broken — an encode-without-decode asymmetry. |
| (a) **and** (b) | Symmetric: both directions resolve to `(serializerId=33, manifest=<JVM class>)`. |

For the jackson-cbor serializer, path (b) is satisfied automatically: the
embeddable `JVMClassManifest` struct implements `ArterySerializerID()` (returns
`DefaultID`) and `ArteryManifest()` (returns `Class`), so every Go type that
embeds it auto-satisfies `actor.RemoteSerializable`. Path (a) is satisfied by the
explicit `RegisterManifest` loop shown in "Registering types and wiring with the
cluster" below. **This dual wiring is mandatory, not optional — see the Phase 3
Bug #1 narrative in
`docs/showcase-investigations/2026-05-18-cross-language-cbor-codec-gap.md`.**

---

## When to use it

Use this serializer when your gekka node participates in a Pekko / Akka cluster whose message types are bound to jackson-cbor in HOCON, e.g.

```hocon
pekko.actor {
  serializers {
    jackson-cbor = "org.apache.pekko.serialization.jackson.JacksonCborSerializer"
  }
  serialization-bindings {
    "com.example.MyMessage" = jackson-cbor
  }
}
```

Both sides must agree on which case-class equivalents exist on the Go side. The serializer is opt-in: gekka does NOT auto-register it.

---

## Serializer identifier

The serializer's identifier is **33**, matching Pekko's class-defined value in `serialization-jackson/src/main/resources/reference.conf:274`:

```hocon
pekko.actor.serialization-identifiers {
  jackson-cbor = 33
}
```

Pekko's `JacksonCborSerializer.identifier` is read from this entry at constructor time via `BaseSerializer.identifierFromConfig`. Gekka's `serialization/jackson-cbor` mirrors the value as `jcbor.DefaultID int32 = 33`. Both peers see the same identifier because both load the same Pekko library version's `reference.conf`.

If you ship a non-default identifier in your own HOCON, construct gekka's serializer with `jcbor.New(yourID)` and register at that ID.

---

## JVM class identity — the `JVMClassManifest` embed

Pekko's wire frame carries a `manifest` string that is the JVM-side fully-qualified class name of the message. Gekka's Go types declare their JVM identity by embedding `jacksoncbor.JVMClassManifest`:

```go
import jcbor "github.com/sopranoworks/gekka/serialization/jackson-cbor"

type EchoEnvelope struct {
    jcbor.JVMClassManifest `cbor:"-"`           // off the wire
    SeqNo                  int64       `cbor:"seqNo"`
    Originator             string      `cbor:"originator"`
    Direction              string      `cbor:"direction"`
    PayloadKind            string      `cbor:"payloadKind"`
    Payload                interface{} `cbor:"payload"`
}

func NewEchoEnvelope(seq int64, originator, direction, kind string, payload interface{}) *EchoEnvelope {
    return &EchoEnvelope{
        JVMClassManifest: jcbor.JVMClassManifest{Class: "com.example.EchoEnvelope"},
        SeqNo:            seq,
        Originator:       originator,
        Direction:        direction,
        PayloadKind:      kind,
        Payload:          payload,
    }
}
```

**Rules:**

1. **Embed `JVMClassManifest` tagged `cbor:"-"`.** The tag keeps the embedded struct off the CBOR wire — Pekko's Jackson does not expect to see a `Class` field in the payload.
2. **Set `Class` to the fully-qualified JVM class name.** This becomes the wire `manifest` field for outbound traffic and the lookup key for inbound decode.
3. **Use lowerCamelCase `cbor:"..."` tags on case-class fields.** Pekko's Jackson emits Scala field names (lowerCamelCase) as CBOR map keys.
4. **The embed exposes a `Manifest() string` method** (free with the embed via `JVMClassManifest.Manifest() string`). The serializer reads this via the `JVMTyped` interface to discover a value's JVM class on encode.

Go types **without** `JVMClassManifest` are rejected on encode with a structured error. The serializer never auto-derives a JVM class from the Go type name — Go's type system does not carry JVM class names, and auto-derivation would silently produce wire-incompatible bytes.

---

## Registering types and wiring with the cluster

```go
import (
    "reflect"
    gekka "github.com/sopranoworks/gekka"
    jcbor "github.com/sopranoworks/gekka/serialization/jackson-cbor"
)

cluster, _ := gekka.NewCluster(cfg)

// Construct the serializer at the default ID (33).
jcborSer := jcbor.New(jcbor.DefaultID)

// Register each Go type with the serializer so FromBinary can resolve the
// JVM manifest back to a Go pointer type.
jcborSer.Register(NewEchoEnvelope(0, "", "", "", nil))
jcborSer.Register(NewAskEnvelope(0, "", "", "", nil))
// ... one Register call per wire type

// Register the serializer instance with the core SerializationRegistry
// (binds the serializer at its Identifier()).
cluster.RegisterSerializerByValue(jcborSer)

// Register manifest → Go type bindings so the OUTBOUND path resolves
// Go-side values to (manifest, serializerID) on encode.
registry := cluster.Serialization()
for manifest, goType := range map[string]reflect.Type{
    "com.example.EchoEnvelope": reflect.TypeOf((*EchoEnvelope)(nil)),
    "com.example.AskEnvelope":  reflect.TypeOf((*AskEnvelope)(nil)),
    // ...
} {
    registry.RegisterManifest(manifest, goType, jcbor.DefaultID)
}
```

After this, sending a `*EchoEnvelope` via `cluster.System.ActorOf(...)` outbound will produce a CBOR frame with `serializerId=33` and `manifest="com.example.EchoEnvelope"` — Pekko's Jackson decoder accepts it as a `com.example.EchoEnvelope` case class.

---

## Polymorphic `AnyRef` payloads (the discriminator pattern)

Pekko's `JacksonCborSerializer` **does NOT emit `@class` tags** for polymorphic fields (typed as `AnyRef` on the Scala side). The nested CBOR map contains only the case-class fields. This was verified empirically against `JacksonCborSerializer`'s output for `EchoEnvelope.payload: AnyRef` — no `@class` key in the nested map.

Receivers therefore cannot reconstruct the typed payload from the wire alone. The contract is that the **envelope carries a discriminator field** (`payloadKind` in the showcase example) that the receiver uses to dispatch into the correct concrete type:

```go
// On decode: serializer fills env.Payload with map[interface{}]interface{}
// (nested object) or primitive (string / int64). Application code coerces:
func CoercePayload(kind string, payload interface{}) (interface{}, error) {
    switch kind {
    case "string":
        return payload.(string), nil
    case "long":
        return payload.(int64), nil
    case "system":
        m := payload.(map[interface{}]interface{})
        seq, _ := m["seqNo"].(int64)
        origin, _ := m["origin"].(string)
        return NewSystemMessagePing(seq, origin), nil
    case "custom":
        m := payload.(map[interface{}]interface{})
        // ... extract case-class fields
        return NewShowcaseEchoCustom(...), nil
    default:
        return nil, fmt.Errorf("unknown payloadKind %q", kind)
    }
}
```

The discriminator string + the JVM class registry is enough to dispatch unambiguously. The serializer itself stays generic — it doesn't know about the envelope-specific discriminator; application code (the actor or a thin coercion layer) provides that knowledge.

On encode, the reverse: Go application code places a `*SystemMessagePing` into `env.Payload`. The serializer emits its case-class fields as a nested CBOR map, with no `@class` tag. Pekko's Jackson decodes the nested map as a `java.util.Map` (lossy for type info, but Pekko's receivers also dispatch via the envelope's discriminator).

---

## Wire-shape compatibility notes

- **CBOR maps:** Pekko's Jackson emits indefinite-length maps (`0xbf … 0xff`). Gekka's encoder emits definite-length maps (`0xa0+N`). Both are valid CBOR; both decoders accept either. Round-trip is semantic, not byte-equal.
- **`Long`:** Pekko emits as CBOR positive int (major type 0). Gekka decodes into Go `int64` when target is `interface{}` (via `IntDec: IntDecConvertSigned`).
- **`Vector[Byte]`:** Pekko emits as CBOR array of small uints (`0x9f 0x18 0x42 … 0xff`), not as a CBOR byte string. Gekka decodes into `[]interface{}{int64, int64, ...}` when target is `interface{}`. The showcase's `ShowcaseEchoCustom.Payload` field uses `[]interface{}` for this reason; convert to `[]byte` in the coercion helper if needed.

---

## Test coverage

| Test (location) | What it proves |
|---|---|
| `serialization/jackson-cbor/jacksoncbor_test.go` (Go) | 18 tests: 4 EchoEnvelope payload kinds, 4 AskEnvelope payload kinds, Ping/Pong, SystemMessagePing standalone, Go-emits-stable, unknown-manifest negative, no-JVMClassManifest-encode negative, unknown-payloadKind negative, manifest-emission positives. |
| `test/showcase/scala/src/test/scala/com/gekka/showcase/JacksonCborGoldensSpec.scala` (Scala) | 2 tests: Pekko's `JacksonCborSerializer` decodes gekka-emitted `go_emitted_*.cbor` fixtures into source case-class field values. Direction-(b) cross-language identity. |

Test fixtures live under `internal/core/testdata/jackson_cbor/`:
- `scala_emitted_*.cbor` — produced by `test/showcase/scala/src/test/scala/com/gekka/showcase/EmitGoldens.scala` (run via `sbt Test/runMain com.gekka.showcase.EmitGoldens`).
- `go_emitted_*.cbor` — produced as a side effect by Go-side `*_GoEmits_Stable` tests.
- `*.json` — sidecar source-field manifests used by both sides for assertion.

Run the Go side: `cd serialization && go test ./jackson-cbor/...`.
Run the Scala side: `cd test/showcase/scala && sbt "Test/testOnly com.gekka.showcase.JacksonCborGoldensSpec"`.

---

## Limitations

1. **No `@JsonTypeInfo` support.** If a Pekko deployment annotates a case class with `@JsonTypeInfo(use=Id.CLASS)`, Pekko WILL emit `@class` on the wire — gekka's current decoder does not consume `@class` for type dispatch. Add support by extending the decoder when the use case arises.
2. **Default jackson-cbor compression off.** Pekko supports gzip/lz4 compression of jackson-cbor payloads larger than a configured threshold. Gekka's decoder does not currently decompress. Disable Pekko's compression for cross-language traffic (or implement decompression in gekka).
3. **Polymorphism via discriminator is application-level.** The showcase uses `payloadKind`; another app may use a different field. The serializer itself is generic and does not handle application discriminators.

---

## See also

- `docs/SERIALIZATION.md` — the broader serializer subsystem.
- `docs/showcase-investigations/2026-05-18-cross-language-cbor-codec-gap.md` — Phase 1-3 investigation and implementation notes for the showcase test that motivated this serializer.
- `pekko/serialization-jackson/src/main/scala/org/apache/pekko/serialization/jackson/JacksonSerializer.scala` — Pekko's reference implementation.
