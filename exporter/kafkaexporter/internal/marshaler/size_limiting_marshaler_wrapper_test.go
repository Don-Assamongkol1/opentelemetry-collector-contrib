// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package marshaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
)

func TestSizeLimitingTracesMarshaler(t *testing.T) {
	inner := NewPdataTracesMarshaler(&ptrace.ProtoMarshaler{})

	t.Run("trace under limit returns one message", func(t *testing.T) {
		// Single span, encoded size should be under 1MB.
		td := testdata.GenerateTracesOneSpan()
		m := NewSizeLimitingTracesMarshaler(inner, 1_000_000, 1024)
		messages, err := m.MarshalTraces(td)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		assert.NotEmpty(t, messages[0].Value)
	})

	t.Run("trace over limit splits to span-by-span", func(t *testing.T) {
		// Two spans; with a tiny limit the whole trace exceeds, so we split.
		td := testdata.GenerateTracesTwoSpansSameResource()
		// Use a limit that is smaller than the marshaled trace but larger than one span.
		m := NewSizeLimitingTracesMarshaler(inner, 400, 64)
		messages, err := m.MarshalTraces(td)
		require.NoError(t, err)
		require.Len(t, messages, 2, "expected two messages (one per span)")
		for i, msg := range messages {
			assert.NotEmpty(t, msg.Value, "message %d", i)
		}
	})

	t.Run("single span over limit returns permanent error", func(t *testing.T) {
		td := testdata.GenerateTracesOneSpan()
		// Limit so low that even one span cannot fit (effective max value = 1).
		m := NewSizeLimitingTracesMarshaler(inner, 64, 32)
		messages, err := m.MarshalTraces(td)
		require.Error(t, err)
		assert.Nil(t, messages)
		assert.True(t, consumererror.IsPermanent(err), "expected permanent error when single span exceeds limit")
	})
}
