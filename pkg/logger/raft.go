// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"io"
	"log"

	hclog "github.com/hashicorp/go-hclog"
)

type ZerologRaftAdapter struct{}

func (z ZerologRaftAdapter) Debug(msg string, args ...interface{}) {
	Debug().Msgf(msg, args...)
}

func (z ZerologRaftAdapter) Info(msg string, args ...interface{}) {
	Info().Msgf(msg, args...)
}

func (z ZerologRaftAdapter) Warn(msg string, args ...interface{}) {
	Warn().Msgf(msg, args...)
}

func (z ZerologRaftAdapter) Error(msg string, args ...interface{}) {
	Error().Msgf(msg, args...)
}

func (z ZerologRaftAdapter) Trace(msg string, args ...interface{}) {
	Trace().Msgf(msg, args...)
}

func (z ZerologRaftAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace:
		z.Trace(msg, args...)
	case hclog.Debug:
		z.Debug(msg, args...)
	case hclog.Info:
		z.Info(msg, args...)
	case hclog.Warn:
		z.Warn(msg, args...)
	case hclog.Error:
		z.Error(msg, args...)
	default:
		Info().Msgf(msg, args...)
	}
}

func (z ZerologRaftAdapter) IsTrace() bool                                                { return true }
func (z ZerologRaftAdapter) IsDebug() bool                                                { return true }
func (z ZerologRaftAdapter) IsInfo() bool                                                 { return true }
func (z ZerologRaftAdapter) IsWarn() bool                                                 { return true }
func (z ZerologRaftAdapter) IsError() bool                                                { return true }
func (z ZerologRaftAdapter) GetLevel() hclog.Level                                        { return hclog.Info }
func (z ZerologRaftAdapter) SetLevel(level hclog.Level)                                   {}
func (z ZerologRaftAdapter) Name() string                                                 { return "zerolog-raft" }
func (z ZerologRaftAdapter) Named(name string) hclog.Logger                               { return z }
func (z ZerologRaftAdapter) ResetNamed(name string) hclog.Logger                          { return z }
func (z ZerologRaftAdapter) With(args ...interface{}) hclog.Logger                        { return z }
func (z ZerologRaftAdapter) ImpliedArgs() []interface{}                                   { return nil }
func (z ZerologRaftAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger { return nil }
func (z ZerologRaftAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer   { return nil }
