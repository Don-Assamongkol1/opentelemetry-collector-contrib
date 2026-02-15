# Kafka Exporter: Trace Message Size Limiting — Context for Handoff

## What Was Built (PR #45650 / issue #36982)

The Kafka exporter was updated so **traces are split only when needed** to stay under Kafka’s `max.message.bytes`, instead of always sending one message per span.

- **Default**: Marshal each trace as **one** Kafka message (whole trace).
- **When that message would exceed the limit**: Re-marshal **span-by-span**; each span is a separate Kafka message.
- **When a single span still exceeds the limit**: Return a **permanent error** (no retry).

## Design Decisions

1. **Sizing is by encoded (wire) size, not pdata**  
   All checks use `len(Message.Value)` — the bytes produced by the configured marshaler (OTLP proto, OTLP JSON, Jaeger, etc.). Same trace can have different sizes per encoding; splitting is correct for the chosen encoding.

2. **Full Kafka record size (key + value + headers)**  
   Kafka’s limit applies to the whole record. The code uses an **effective max value size**: `maxMessageBytes - len(key) - estimatedHeaderOverhead` (with a cap so small limits still leave room for at least one span). Constant `estimatedKafkaHeadersOverhead` (1024) in `marshaler.go`; `minValueSizeBytes` (256) in the wrapper ensures a minimum value budget when `maxMessageBytes` is small (e.g. 512 in tests).

3. **Backwards compatible for consumers**  
   Kafka receiver and backends treat each record independently and group by trace ID. One message per span is already valid (Jaeger is defined as one span per message; OTLP allows one-span payloads).

## Main Files

| File | Role |
|------|------|
| `exporter/kafkaexporter/internal/marshaler/size_limiting_marshaler_wrapper.go` | Wrapper that wraps any `TracesMarshaler`, enforces effective max value size, splits to span-by-span when needed, returns permanent error if a single span is too large. |
| `exporter/kafkaexporter/internal/marshaler/size_limiting_marshaler_wrapper_test.go` | Unit tests: under limit → one message; over limit → split; single span over limit → permanent error. |
| `exporter/kafkaexporter/internal/marshaler/pdata_marshaler.go` | **Traces**: marshals whole trace into one message (reverted from one-message-per-span). |
| `exporter/kafkaexporter/marshaler.go` | `getTracesMarshaler(encoding, host, maxMessageBytes, estimatedHeaderOverhead)` builds the encoding-specific marshaler (`innerMarshaler`), then if `maxMessageBytes > 0` wraps it with `NewSizeLimitingTracesMarshaler(innerMarshaler, ...)`. Uses constant `estimatedKafkaHeadersOverhead`. |
| `exporter/kafkaexporter/kafka_exporter.go` | `newTracesExporter` calls `getTracesMarshaler(..., config.Producer.MaxMessageBytes, 0)`. The returned marshaler is stored on `kafkaTracesMessenger` and used in `marshalData`. |

## Naming Refactors Already Done

- `size_limiting_traces.go` / `_test.go` → `size_limiting_marshaler_wrapper.go` / `_test.go`
- `minValueSize` → `minValueSizeBytes`
- In `getTracesMarshaler`: variable `inner` → `innerMarshaler`
- Constant `defaultEstimatedHeaderOverhead` → `estimatedKafkaHeadersOverhead`

## Tests

- `TestTracesPusher_max_message_bytes_Kgo`: WithLowSpanCount (1 span) → 1 record; WithHighSpanCount (10k spans) → 10k records (split path).
- `TestSizeLimitingTracesMarshaler` in `size_limiting_marshaler_wrapper_test.go`
- `TestPdataTracesMarshaler`: expects one message per trace (whole-trace marshal).

## Optional Follow-ups (from earlier plan)

- Make header overhead configurable or derived from `IncludeMetadataKeys`.
- Document that consumers rely on backend/DB to stitch spans by trace ID when multiple messages per trace are used.
