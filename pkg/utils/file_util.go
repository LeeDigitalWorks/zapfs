// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

func TestWritableFile(folder string) error {
	info, err := os.Stat(folder)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return os.ErrInvalid
	}

	permission := info.Mode().Perm()
	if permission&0200 != 0 {
		return nil
	}

	return os.ErrPermission
}

func ResolvePath(path string) string {
	if !strings.Contains(path, "~") {
		return path
	}

	if path == "~" {
		if usr, err := user.Current(); err == nil {
			path = usr.HomeDir
		}
	} else if strings.HasPrefix(path, "~/") {
		if usr, err := user.Current(); err == nil {
			path = filepath.Join(usr.HomeDir, path[2:])
		}
	}

	path = os.ExpandEnv(path)
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}

	return path
}
