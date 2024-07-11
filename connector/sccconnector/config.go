// SPDX-License-Identifier: Apache-2.0

package sccconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/sccconnector"

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/madvikinggod/otel-semconv-checker/pkg/match"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	Trace           []match.Match `mapstructure:"trace"`
	Metrics         []match.Match `mapstructure:"metrics"`
	Log             []match.Match `mapstructure:"log"`
	ReportUnmatched bool          `mapstructure:"report_unmatched"`
}

func parseError(scope string, err error) error {
	return fmt.Errorf("failed to parse %s: %w", scope, err)
}

func (c *Config) Validate() error {
	errs := []error{}
	for _, m := range c.Trace {
		_, err := regexp.Compile(m.Match)
		if err != nil {
			errs = append(errs, parseError("trace", err))
		}
	}
	for _, m := range c.Metrics {
		_, err := regexp.Compile(m.Match)
		if err != nil {
			errs = append(errs, parseError("metrics", err))
		}
	}
	for _, m := range c.Log {
		_, err := regexp.Compile(m.Match)
		if err != nil {
			errs = append(errs, parseError("log", err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{}
}
