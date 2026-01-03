//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
)

// initializeLDAPBackedIAM returns an error in community edition.
// LDAP integration requires the enterprise edition with FeatureLDAP license.
func initializeLDAPBackedIAM(ldapURL string) (*iam.Service, error) {
	return nil, fmt.Errorf("LDAP integration requires enterprise edition")
}
