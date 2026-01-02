// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

var (
	ConfigurationFileDirectory string
)

func LoadConfiguration(configFileName string, required bool) bool {
	viper.SetConfigName(configFileName)
	viper.AddConfigPath(ResolvePath(ConfigurationFileDirectory))
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.zapfs")
	viper.AddConfigPath("/usr/local/etc/zapfs/")
	viper.AddConfigPath("/etc/zapfs/")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := viper.MergeInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			if required {
				log.Fatal().Msgf("Config file not found: %s", configFileName)
			}
			log.Info().Msgf("Config file not found: %s", configFileName)
			return false
		}

		if required {
			log.Fatal().Msgf("Failed to load required config file: %s", configFileName)
		}
		return false
	}
	log.Info().Msgf("Loaded config file: %s", viper.ConfigFileUsed())

	return true
}
