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
)

func Test_connectorImp_ConsumeTraces(t *testing.T) {
	config := &Config{
		Trace: []match.Match{
			{
				SemanticVersion: "https://opentelemetry.io/schemas/1.24.0",
				Match:           "match_.*",
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
			{
				Match:            ".*_additional",
				ReportAdditional: true,
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
