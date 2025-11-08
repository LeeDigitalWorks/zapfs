package utils

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
)

type FreeSpaceType int

const (
	AsPercent FreeSpaceType = iota
	AsBytes
)

type FreeSpace struct {
	Type    FreeSpaceType
	Bytes   uint64
	Percent float32
	Raw     string
}

func (s FreeSpace) IsLow(freeBytes uint64, freePercent float32) (bool, string) {
	switch s.Type {
	case AsPercent:
		return freePercent < s.Percent, fmt.Sprintf("disk free percent %.2f%%, threshold %.2f%%", freePercent, s.Percent)
	case AsBytes:
		return freeBytes < s.Bytes, fmt.Sprintf("disk free bytes %d, threshold %d", freeBytes, s.Bytes)
	}
	return false, ""
}

func (s FreeSpace) String() string {
	switch s.Type {
	case AsPercent:
		return fmt.Sprintf("%.2f%%", s.Percent)
	default:
		return s.Raw
	}
}

func ParseMinFreeSpace(s string) (*FreeSpace, error) {
	if percent, err := strconv.ParseFloat(s, 32); err == nil {
		if percent < 0 || percent > 100 {
			return nil, fmt.Errorf("invalid percent value: %s", s)
		}
		return &FreeSpace{
			Type:    AsPercent,
			Percent: float32(percent),
			Raw:     s,
		}, nil
	}

	if bytes, err := humanize.ParseBytes(s); err == nil {
		if bytes <= 100 {
			return nil, fmt.Errorf("invalid byte value: %s", s)
		}
		return &FreeSpace{
			Type:  AsBytes,
			Bytes: bytes,
			Raw:   s,
		}, nil
	}

	return nil, errors.New("invalid min free space format")
}
