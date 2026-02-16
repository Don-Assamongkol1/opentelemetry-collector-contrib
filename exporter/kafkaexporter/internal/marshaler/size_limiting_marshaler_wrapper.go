// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package marshaler // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/marshaler"

import (
	"fmt"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Default reserve for Kafka record key when not set by marshaler (e.g. trace ID hex = 32 bytes).
const defaultMaxKeySize = 32

// minValueSizeBytes is the minimum bytes we leave for the value when computing effective max,
// so that small maxMessageBytes (e.g. 512 in tests) still allow at least one span per message.
const minValueSizeBytes = 256

// SizeLimitingTracesMarshaler wraps a TracesMarshaler and ensures no message exceeds the
// effective max value size (maxMessageBytes minus key and header overhead), splitting
// into span-level messages when necessary. Size checks use the encoded payload size
// (final wire form), not pdata size.
type SizeLimitingTracesMarshaler struct {
	innerMarshaler          TracesMarshaler
	maxMessageBytes         int
	estimatedHeaderOverhead int
}

// NewSizeLimitingTracesMarshaler returns a TracesMarshaler that delegates to innerMarshaler but
// enforces that each message's value fits within the full Kafka record limit.
// maxMessageBytes is the broker's max.message.bytes (full record: key + value + headers).
// estimatedHeaderOverhead is a conservative reserve for Kafka record headers.
func NewSizeLimitingTracesMarshaler(innerMarshaler TracesMarshaler, maxMessageBytes, estimatedHeaderOverhead int) TracesMarshaler {
	return &SizeLimitingTracesMarshaler{
		innerMarshaler:          innerMarshaler,
		maxMessageBytes:         maxMessageBytes,
		estimatedHeaderOverhead: estimatedHeaderOverhead,
	}
}

// effectiveMaxValue returns the maximum allowed len(Value) for the given message so that
// (key + value + headers) does not exceed maxMessageBytes. When maxMessageBytes is small,
// we cap the header reserve so at least minValueSize bytes remain for the value.
func (s *SizeLimitingTracesMarshaler) effectiveMaxValue(msg Message) int {
	keySize := len(msg.Key)
	if keySize == 0 {
		keySize = defaultMaxKeySize
	}
	available := s.maxMessageBytes - keySize
	headerReserve := s.estimatedHeaderOverhead
	if minVal := minValueSizeBytes; available > minVal && headerReserve > available-minVal {
		headerReserve = available - minVal
	}
	if headerReserve < 0 {
		headerReserve = 0
	}
	return available - headerReserve
}

func (s *SizeLimitingTracesMarshaler) MarshalTraces(td ptrace.Traces) ([]Message, error) {
	messages, err := s.innerMarshaler.MarshalTraces(td)
	if err != nil {
		return nil, err
	}
	// Check if any message exceeds the effective max value size.
	var needsSplit bool
	for i := range messages {
		if len(messages[i].Value) > s.effectiveMaxValue(messages[i]) {
			needsSplit = true
			break
		}
	}
	if !needsSplit {
		return messages, nil
	}
	// Re-marshal span-by-span so each message fits. If any single span still exceeds the limit, fail.
	return s.marshalTracesSpanBySpan(td)
	// TODO: we'd also only want to call this "break up a kf message that represents a trace into multiple spans" only
	//   on messages that exceed the size, not all traces as is being done here.
}

// marshalTracesSpanBySpan produces one message per span using the innerMarshaler.
// Returns a permanent error if any span's encoded size exceeds the effective max.
func (s *SizeLimitingTracesMarshaler) marshalTracesSpanBySpan(td ptrace.Traces) ([]Message, error) {
	var out []Message
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)
		scopeSpans := resourceSpan.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			spans := scopeSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				singleSpanTraces := ptrace.NewTraces()
				newResourceSpan := singleSpanTraces.ResourceSpans().AppendEmpty()
				resourceSpan.Resource().CopyTo(newResourceSpan.Resource())
				newResourceSpan.SetSchemaUrl(resourceSpan.SchemaUrl())
				newScopeSpan := newResourceSpan.ScopeSpans().AppendEmpty()
				scopeSpan.Scope().CopyTo(newScopeSpan.Scope())
				newScopeSpan.SetSchemaUrl(scopeSpan.SchemaUrl())
				spans.At(k).CopyTo(newScopeSpan.Spans().AppendEmpty())

				msgs, err := s.innerMarshaler.MarshalTraces(singleSpanTraces)
				if err != nil {
					return nil, err
				}
				for _, m := range msgs {
					if len(m.Value) > s.effectiveMaxValue(m) {
						return nil, consumererror.NewPermanent(fmt.Errorf(
							"single span encoded size (%d bytes) exceeds max message size (effective value limit %d bytes); cannot split further",
							len(m.Value), s.effectiveMaxValue(m),
						))
					}
					out = append(out, m)
				}
			}

		}
	}
	return out, nil
}

var _ TracesMarshaler = (*SizeLimitingTracesMarshaler)(nil)
