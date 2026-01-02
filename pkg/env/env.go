// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package env

import (
	"sync"

	"github.com/spf13/viper"
)

const (
	Local      = "local"
	Production = "production"
	Testing    = "testing"
)

var (
	Env string

	once sync.Once
)

func IsLocal() bool {
	return Env == Local
}

func IsProduction() bool {
	return Env == Production
}

func IsTesting() bool {
	return Env == Testing
}

func init() {
	once.Do(func() {
		Env = viper.GetString("ENV")
		if Env == "" {
			Env = Local
		}
	})
}
