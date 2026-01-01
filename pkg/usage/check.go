package usage

// IsUsageReportingEnabled checks if usage reporting is enabled.
// In enterprise builds, this checks for FeatureAdvancedMetrics or FeatureMultiTenancy.
// In community builds, this always returns false.
//
// This function is implemented in check_enterprise.go and check_stub.go
// with build tags to select the appropriate implementation at compile time.
