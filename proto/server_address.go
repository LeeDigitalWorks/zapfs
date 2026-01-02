// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package proto

import (
	"strconv"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

type ServerAddress string

func NewServerAddress(host string, port int, grpcPort int) ServerAddress {
	if grpcPort == 0 || grpcPort == port+10000 {
		return ServerAddress(utils.JoinHostPort(host, port))
	}
	return ServerAddress(utils.JoinHostPort(host, port) + "." + strconv.Itoa(grpcPort))
}
