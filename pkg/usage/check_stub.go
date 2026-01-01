//go:build !enterprise

package usage

// IsUsageReportingEnabled always returns false in community edition.
func IsUsageReportingEnabled() bool {
	return false
}
