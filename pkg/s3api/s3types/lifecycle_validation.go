package s3types

import (
	"fmt"
	"strings"
	"time"
)

// Validate validates the lifecycle configuration
func (lc *Lifecycle) Validate() error {
	if len(lc.Rules) == 0 {
		return fmt.Errorf("lifecycle configuration must have at least one rule")
	}

	if len(lc.Rules) > 1000 {
		return fmt.Errorf("lifecycle configuration cannot have more than 1000 rules")
	}

	// Track rule IDs to detect duplicates
	ruleIDs := make(map[string]bool)

	for i, rule := range lc.Rules {
		if err := rule.Validate(); err != nil {
			return fmt.Errorf("rule %d: %w", i, err)
		}

		// Check for duplicate IDs
		if rule.ID != nil {
			if ruleIDs[*rule.ID] {
				return fmt.Errorf("duplicate rule ID: %s", *rule.ID)
			}
			ruleIDs[*rule.ID] = true
		}
	}

	return nil
}

// Validate validates a single lifecycle rule
func (r *LifecycleRule) Validate() error {
	// Validate status
	if r.Status != LifecycleStatusEnabled && r.Status != LifecycleStatusDisabled {
		return fmt.Errorf("invalid status: %s (must be Enabled or Disabled)", r.Status)
	}

	// Rule must have at least one action
	hasAction := r.Expiration != nil ||
		len(r.Transitions) > 0 ||
		r.NoncurrentVersionExpiration != nil ||
		r.NoncurrentVersionTransition != nil ||
		r.AbortIncompleteMultipartUpload != nil

	if !hasAction {
		return fmt.Errorf("rule must specify at least one action")
	}

	// Validate filter if present
	if r.Filter != nil {
		if err := r.Filter.Validate(); err != nil {
			return fmt.Errorf("filter: %w", err)
		}
	}

	// Validate expiration
	if r.Expiration != nil {
		if err := r.Expiration.Validate(); err != nil {
			return fmt.Errorf("expiration: %w", err)
		}
	}

	// Validate transitions
	if len(r.Transitions) > 0 {
		if err := validateTransitions(r.Transitions); err != nil {
			return fmt.Errorf("transitions: %w", err)
		}
	}

	// Validate noncurrent version expiration
	if r.NoncurrentVersionExpiration != nil {
		if err := r.NoncurrentVersionExpiration.Validate(); err != nil {
			return fmt.Errorf("noncurrent version expiration: %w", err)
		}
	}

	// Validate noncurrent version transition
	if r.NoncurrentVersionTransition != nil {
		if err := r.NoncurrentVersionTransition.Validate(); err != nil {
			return fmt.Errorf("noncurrent version transition: %w", err)
		}
	}

	// Validate abort incomplete multipart upload
	if r.AbortIncompleteMultipartUpload != nil {
		if err := r.AbortIncompleteMultipartUpload.Validate(); err != nil {
			return fmt.Errorf("abort incomplete multipart upload: %w", err)
		}
	}

	return nil
}

// Validate validates the filter
func (f *LifecycleFilter) Validate() error {
	// Count how many filter criteria are set
	criteriaCount := 0
	if f.Prefix != nil {
		criteriaCount++
	}
	if f.Tag != nil {
		criteriaCount++
	}
	if f.And != nil {
		criteriaCount++
	}
	if f.ObjectSizeGreaterThan != nil {
		criteriaCount++
	}
	if f.ObjectSizeLessThan != nil {
		criteriaCount++
	}

	// If And is present, other top-level filters should not be
	if f.And != nil && criteriaCount > 1 {
		return fmt.Errorf("And filter cannot be combined with other top-level filter criteria")
	}

	// Validate And operator if present
	if f.And != nil {
		if err := f.And.Validate(); err != nil {
			return fmt.Errorf("And: %w", err)
		}
	}

	// Validate tag if present
	if f.Tag != nil {
		if err := f.Tag.Validate(); err != nil {
			return fmt.Errorf("tag: %w", err)
		}
	}

	// Validate size constraints
	if f.ObjectSizeGreaterThan != nil && *f.ObjectSizeGreaterThan < 0 {
		return fmt.Errorf("ObjectSizeGreaterThan must be non-negative")
	}
	if f.ObjectSizeLessThan != nil && *f.ObjectSizeLessThan < 0 {
		return fmt.Errorf("ObjectSizeLessThan must be non-negative")
	}
	if f.ObjectSizeGreaterThan != nil && f.ObjectSizeLessThan != nil {
		if *f.ObjectSizeGreaterThan >= *f.ObjectSizeLessThan {
			return fmt.Errorf("ObjectSizeGreaterThan must be less than ObjectSizeLessThan")
		}
	}

	return nil
}

// Validate validates the And operator
func (a *LifecycleRuleAndOperator) Validate() error {
	// And must have at least 2 criteria
	criteriaCount := 0
	if a.Prefix != nil {
		criteriaCount++
	}
	if len(a.Tags) > 0 {
		criteriaCount++
	}
	if a.ObjectSizeGreaterThan != nil {
		criteriaCount++
	}
	if a.ObjectSizeLessThan != nil {
		criteriaCount++
	}

	if criteriaCount < 2 {
		return fmt.Errorf("And operator must have at least 2 filter criteria")
	}

	// Validate tags
	for i, tag := range a.Tags {
		if err := tag.Validate(); err != nil {
			return fmt.Errorf("tag %d: %w", i, err)
		}
	}

	// Validate size constraints
	if a.ObjectSizeGreaterThan != nil && *a.ObjectSizeGreaterThan < 0 {
		return fmt.Errorf("ObjectSizeGreaterThan must be non-negative")
	}
	if a.ObjectSizeLessThan != nil && *a.ObjectSizeLessThan < 0 {
		return fmt.Errorf("ObjectSizeLessThan must be non-negative")
	}
	if a.ObjectSizeGreaterThan != nil && a.ObjectSizeLessThan != nil {
		if *a.ObjectSizeGreaterThan >= *a.ObjectSizeLessThan {
			return fmt.Errorf("ObjectSizeGreaterThan must be less than ObjectSizeLessThan")
		}
	}

	return nil
}

// Validate validates a tag
func (t *Tag) Validate() error {
	if t.Key == "" {
		return fmt.Errorf("tag key cannot be empty")
	}
	if len(t.Key) > 128 {
		return fmt.Errorf("tag key cannot exceed 128 characters")
	}
	if len(t.Value) > 256 {
		return fmt.Errorf("tag value cannot exceed 256 characters")
	}
	return nil
}

// Validate validates expiration settings
func (e *LifecycleExpiration) Validate() error {
	// Count how many expiration criteria are set
	criteriaCount := 0
	if e.Days != nil {
		criteriaCount++
	}
	if e.Date != nil {
		criteriaCount++
	}
	if e.ExpiredObjectDeleteMarker != nil && *e.ExpiredObjectDeleteMarker {
		criteriaCount++
	}

	// Must have exactly one criterion
	if criteriaCount == 0 {
		return fmt.Errorf("expiration must specify Days, Date, or ExpiredObjectDeleteMarker")
	}
	if criteriaCount > 1 {
		return fmt.Errorf("expiration can only specify one of Days, Date, or ExpiredObjectDeleteMarker")
	}

	// Validate Days
	if e.Days != nil && *e.Days < 0 {
		return fmt.Errorf("expiration days must be non-negative")
	}

	// Validate Date
	if e.Date != nil && e.Date.Before(time.Now().UTC().Truncate(24*time.Hour)) {
		return fmt.Errorf("expiration date must be in the future")
	}

	return nil
}

// validateTransitions validates transition rules
func validateTransitions(transitions []*LifecycleTransition) error {
	if len(transitions) == 0 {
		return nil
	}

	// Track storage classes to prevent duplicates
	storageClasses := make(map[string]bool)
	var prevDays *int64
	var prevDate *time.Time

	for i, transition := range transitions {
		if err := transition.Validate(); err != nil {
			return fmt.Errorf("transition %d: %w", i, err)
		}

		// Check for duplicate storage classes
		if transition.StorageClass != nil {
			sc := strings.ToUpper(*transition.StorageClass)
			if storageClasses[sc] {
				return fmt.Errorf("duplicate transition to storage class: %s", sc)
			}
			storageClasses[sc] = true
		}

		// If using Days, ensure they are in ascending order
		if transition.Days != nil {
			if prevDays != nil && *transition.Days <= *prevDays {
				return fmt.Errorf("transition days must be in ascending order")
			}
			prevDays = transition.Days
		}

		// If using Date, ensure they are in ascending order
		if transition.Date != nil {
			if prevDate != nil && !transition.Date.After(*prevDate) {
				return fmt.Errorf("transition dates must be in ascending order")
			}
			prevDate = transition.Date
		}

		// Cannot mix Days and Date
		if i > 0 {
			prevTransition := transitions[i-1]
			if (transition.Days != nil && prevTransition.Date != nil) ||
				(transition.Date != nil && prevTransition.Days != nil) {
				return fmt.Errorf("cannot mix Days and Date in transitions")
			}
		}
	}

	return nil
}

// Validate validates a single transition
func (t *LifecycleTransition) Validate() error {
	// Must have exactly one of Days or Date
	if (t.Days == nil && t.Date == nil) || (t.Days != nil && t.Date != nil) {
		return fmt.Errorf("transition must specify exactly one of Days or Date")
	}

	// Must have storage class
	if t.StorageClass == nil || *t.StorageClass == "" {
		return fmt.Errorf("transition must specify StorageClass")
	}

	// Validate Days
	if t.Days != nil && *t.Days < 0 {
		return fmt.Errorf("transition days must be non-negative")
	}

	// Validate Date
	if t.Date != nil && t.Date.Before(time.Now().UTC().Truncate(24*time.Hour)) {
		return fmt.Errorf("transition date must be in the future")
	}

	return nil
}

// Validate validates noncurrent version expiration
func (e *LifecycleNoncurrentVersionExpiration) Validate() error {
	if e.Days != nil && *e.Days < 0 {
		return fmt.Errorf("noncurrent days must be non-negative")
	}

	if e.NewerNoncurrentVersions != nil && *e.NewerNoncurrentVersions < 0 {
		return fmt.Errorf("newer noncurrent versions must be non-negative")
	}

	return nil
}

// Validate validates noncurrent version transition
func (t *LifecycleNoncurrentVersionTransition) Validate() error {
	if t.Days != nil && *t.Days < 0 {
		return fmt.Errorf("noncurrent days must be non-negative")
	}

	if t.NewerNoncurrentVersions != nil && *t.NewerNoncurrentVersions < 0 {
		return fmt.Errorf("newer noncurrent versions must be non-negative")
	}

	if t.StorageClass == nil || *t.StorageClass == "" {
		return fmt.Errorf("noncurrent version transition must specify StorageClass")
	}

	return nil
}

// Validate validates abort incomplete multipart upload
func (a *LifecycleAbortIncompleteMultipartUpload) Validate() error {
	if a.DaysAfterInitiation == nil {
		return fmt.Errorf("must specify DaysAfterInitiation")
	}

	if *a.DaysAfterInitiation < 0 {
		return fmt.Errorf("days after initiation must be non-negative")
	}

	return nil
}
