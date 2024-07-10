package sccconnector

import (
	"path/filepath"
	"testing"

	"github.com/madvikinggod/otel-semconv-checker/pkg/match"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/sccconnector/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	testCases := []struct {
		name   string
		expect *Config
	}{
		{
			name:   "",
			expect: &Config{},
		},
		{
			name: "full",
			expect: &Config{
				Trace: []match.Match{
					{
						SemanticVersion: "https://opentelemetry.io/schemas/1.26.0",
						Match:           "http.server.*",
						MatchAttributes: []match.Attribute{
							{
								Name:  "server.address",
								Value: "localhost",
							},
							{
								Name: "http.request.method",
							},
						},
						Groups:           []string{"trace.http.server"},
						Ignore:           []string{"error.type"},
						Include:          []string{"project.id"},
						ReportAdditional: true,
					},
				},
				Metrics: []match.Match{
					{
						SemanticVersion: "https://opentelemetry.io/schemas/1.25.0",
						Match:           "http.server.request.duration",
						MatchAttributes: []match.Attribute{
							{
								Name: "http.request.method",
							},
						},
						Groups:           []string{"metric_attributes.http.server"},
						Ignore:           []string{"ignored.attribute"},
						Include:          []string{"additional.attribute"},
						ReportAdditional: true,
					},
				},
				Log: []match.Match{
					{
						SemanticVersion:  "https://opentelemetry.io/schemas/1.24.0",
						Match:            ".*exception.*",
						MatchAttributes:  nil,
						Groups:           []string{"log-exception"},
						Ignore:           []string{"exception.stacktrace"},
						Include:          []string{"environment"},
						ReportAdditional: true,
					},
				},
				ReportUnmatched: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, tc.name).String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			assert.Equal(t, tc.expect, cfg)
		})
	}
}

func TestConfigErrors(t *testing.T) {
	testCases := []struct {
		name   string
		input  *Config
		expect []string
	}{
		{
			name: "errors",
			input: &Config{
				Trace:   []match.Match{{Match: ")invalid["}},
				Metrics: []match.Match{{Match: ")invalid["}},
				Log:     []match.Match{{Match: ")invalid["}},
			},
			expect: []string{
				"failed to parse trace",
				"failed to parse metrics",
				"failed to parse log",
				")invalid[",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.input.Validate()
			assert.Error(t, err)
			for _, errStr := range tc.expect {
				assert.Contains(t, err.Error(), errStr)
			}
		})
	}
}
