// SPDX-License-Identifier: Apache-2.0

package sccconnector

import (
	"context"
	"path"
	"testing"

	"github.com/madvikinggod/otel-semconv-checker/pkg/match"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func Test_connectorImp_ConsumeTraces(t *testing.T) {

	tests := []struct {
		name    string
		config  Config
		args    ptrace.Traces
		wantlen int
	}{
		{
			name: "match everything",
			config: Config{
				Trace: []match.Match{
					{
						Match:   `.*`,
						Include: []string{"test"},
					},
				},
			},
			args: testMultiTrace(map[string]any{
				"test": "foo",
			}),
			wantlen: 0,
		},
		{
			name: "Missing Metrics",
			config: Config{
				Trace: []match.Match{
					{
						Match:   `.*`,
						Include: []string{"foo"},
					},
				},
			},
			args: testMultiTrace(map[string]any{
				"test": "foo",
			}),
			wantlen: 3,
		},
		{
			name: "Not found",
			config: Config{
				Trace: []match.Match{
					{
						Match:   `thisdoesnotexist`,
						Include: []string{"foo"},
					},
				},
				ReportUnmatched: true,
			},
			args: testMultiTrace(map[string]any{
				"test": "foo",
			}),
			wantlen: 3,
		},
		{
			name: "report extras",
			config: Config{
				Trace: []match.Match{
					{
						Match:            `.*`,
						Include:          []string{"test"},
						ReportAdditional: true,
					},
				},
			},
			args: testMultiTrace(map[string]any{
				"test":  "foo",
				"extra": "bar",
			}),
			wantlen: 3,
		},
	}
	f := NewFactory()
	ctx := context.Background()
	set := connectortest.NewNopSettings()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logsSink := new(consumertest.LogsSink)
			c, err := f.CreateTracesToLogs(ctx, set, &tt.config, logsSink)
			assert.NoError(t, err)
			assert.NotNil(t, c)

			err = c.ConsumeTraces(context.Background(), tt.args)
			assert.NoError(t, err)

			assert.Equal(t, tt.wantlen, logsSink.LogRecordCount())
		})
	}
}

func testMultiTrace(attrs map[string]any) ptrace.Traces {
	traces := ptrace.NewTraces()
	traces.ResourceSpans().EnsureCapacity(3)
	// attributes in the Resrouce
	rs := traces.ResourceSpans().AppendEmpty()
	_ = rs.Resource().Attributes().FromRaw(attrs)
	scope := rs.ScopeSpans().AppendEmpty()
	scope.Scope().SetName("resource Scope")
	spans := scope.Spans().AppendEmpty()
	spans.SetName("resource span")
	// attributes in the Scope
	rs = traces.ResourceSpans().AppendEmpty()
	scope = rs.ScopeSpans().AppendEmpty()
	scope.Scope().SetName("scope Scope")
	_ = scope.Scope().Attributes().FromRaw(attrs)
	spans = scope.Spans().AppendEmpty()
	spans.SetName("scope span")
	// attributes in the Span
	rs = traces.ResourceSpans().AppendEmpty()
	scope = rs.ScopeSpans().AppendEmpty()
	spans = scope.Spans().AppendEmpty()
	spans.SetName("span")
	_ = spans.Attributes().FromRaw(attrs)

	return traces
}

func Test_connectorImp_ConsumeTraces2(t *testing.T) {
	config := &Config{
		Trace: []match.Match{
			{
				SemanticVersion: "https://opentelemetry.io/schemas/1.24.0",
				Match:           ".*",
				Groups: []string{
					"general.server", // This includes "server.address" and "server.port"
					"peer",           // This includes "peer.service"
				},
				Ignore:  []string{"server.port"},
				Include: []string{"environment"},
			},
			{
				Match:   ".*_extra",
				Include: []string{"extra"},
			},
		},
	}

	req, err := golden.ReadTraces(path.Join("testdata", "traces", "input.yaml"))
	require.NoError(t, err)

	expected, err := golden.ReadLogs(path.Join("testdata", "traces", "output.yaml"))
	require.NoError(t, err)

	f := NewFactory()
	ctx := context.Background()
	set := connectortest.NewNopSettings()
	logsSink := new(consumertest.LogsSink)

	c, err := f.CreateTracesToLogs(ctx, set, config, logsSink)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	err = c.ConsumeTraces(context.Background(), req)
	assert.NoError(t, err)

	logs := logsSink.AllLogs()
	require.Equal(t, 1, len(logs))
	assert.NoError(t, plogtest.CompareLogs(expected, logs[0],
		plogtest.IgnoreObservedTimestamp(),
		plogtest.IgnoreTimestamp(),
		plogtest.IgnoreResourceLogsOrder(),
		plogtest.IgnoreScopeLogsOrder(),
		plogtest.IgnoreLogRecordsOrder(),
	))

}
