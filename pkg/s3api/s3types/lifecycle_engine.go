package s3types

import (
	"net/url"
	"strings"
	"time"
)

// Action represents what action should be taken on an object
type Action int

const (
	// NoneAction - no action required
	NoneAction Action = iota
	// DeleteAction - delete the object
	DeleteAction
	// DeleteVersionAction - delete a specific version
	DeleteVersionAction
	// DeleteAllVersionsAction - delete all versions of an object
	DeleteAllVersionsAction
	// TransitionAction - transition object to another storage class
	TransitionAction
	// TransitionVersionAction - transition a specific version
	TransitionVersionAction
	// AbortMultipartUploadAction - abort incomplete multipart upload
	AbortMultipartUploadAction
)

// String returns string representation of action
func (a Action) String() string {
	switch a {
	case NoneAction:
		return "None"
	case DeleteAction:
		return "Delete"
	case DeleteVersionAction:
		return "DeleteVersion"
	case DeleteAllVersionsAction:
		return "DeleteAllVersions"
	case TransitionAction:
		return "Transition"
	case TransitionVersionAction:
		return "TransitionVersion"
	case AbortMultipartUploadAction:
		return "AbortMultipartUpload"
	default:
		return "Unknown"
	}
}

// ObjectState contains object metadata for lifecycle evaluation
// This is separate from Object because lifecycle needs version/state info
// that isn't part of the object's content metadata
type ObjectState struct {
	// Object name/key
	Name string
	// Object size in bytes
	Size int64
	// Object modification time
	ModTime time.Time
	// Is this the latest version?
	IsLatest bool
	// Is this a delete marker?
	DeleteMarker bool
	// Version ID
	VersionID string
	// Object tags as URL query string (e.g., "key1=val1&key2=val2")
	UserTags string
	// Successor modification time (for noncurrent versions)
	SuccessorModTime time.Time
	// Total number of versions
	NumVersions int
}

// NewObjectState creates an ObjectState from an Object with version metadata
func NewObjectState(obj *Object, modTime time.Time, isLatest bool, versionID string) *ObjectState {
	return &ObjectState{
		Name:     obj.Name,
		Size:     obj.Size,
		ModTime:  modTime,
		IsLatest: isLatest,
		// UserTags would need to be set separately from object tags
		VersionID: versionID,
	}
}

// Event represents a lifecycle event that should be executed
type Event struct {
	// Action to take
	Action Action
	// Rule ID that triggered this action
	RuleID string
	// When this action should occur
	Due time.Time
	// Target storage class (for transitions)
	StorageClass string
	// Object this applies to
	Object ObjectState
}

// Evaluator evaluates lifecycle rules against objects
type Evaluator struct {
	lc *Lifecycle
}

// NewEvaluator creates a new lifecycle evaluator
func NewEvaluator(lc *Lifecycle) *Evaluator {
	return &Evaluator{lc: lc}
}

// Eval evaluates all enabled rules against the given object
// Returns the highest priority action that should be taken
func (e *Evaluator) Eval(obj ObjectState, now time.Time) Event {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	var event Event
	event.Object = obj
	event.Action = NoneAction

	for _, rule := range e.lc.Rules {
		// Skip disabled rules
		if rule.Status != LifecycleStatusEnabled {
			continue
		}

		// Check if rule matches this object
		if !e.ruleMatches(rule, obj) {
			continue
		}

		// Evaluate actions in priority order
		if evt := e.evalExpiration(rule, obj, now); evt.Action != NoneAction {
			if event.Action == NoneAction || evt.Due.Before(event.Due) {
				event = evt
			}
		}

		if evt := e.evalTransition(rule, obj, now); evt.Action != NoneAction {
			if event.Action == NoneAction || evt.Due.Before(event.Due) {
				event = evt
			}
		}

		if evt := e.evalNoncurrentVersionExpiration(rule, obj, now); evt.Action != NoneAction {
			if event.Action == NoneAction || evt.Due.Before(event.Due) {
				event = evt
			}
		}

		if evt := e.evalNoncurrentVersionTransition(rule, obj, now); evt.Action != NoneAction {
			if event.Action == NoneAction || evt.Due.Before(event.Due) {
				event = evt
			}
		}

		if evt := e.evalAbortMultipartUpload(rule, obj, now); evt.Action != NoneAction {
			if event.Action == NoneAction || evt.Due.Before(event.Due) {
				event = evt
			}
		}
	}

	return event
}

// ruleMatches checks if a rule matches the given object
func (e *Evaluator) ruleMatches(rule LifecycleRule, obj ObjectState) bool {
	if rule.Filter == nil {
		// No filter means match everything
		return true
	}

	filter := rule.Filter

	// Check prefix
	if filter.Prefix != nil {
		if !strings.HasPrefix(obj.Name, *filter.Prefix) {
			return false
		}
	}

	// Check tag
	if filter.Tag != nil {
		if !e.objectHasTag(obj, filter.Tag.Key, filter.Tag.Value) {
			return false
		}
	}

	// Check size constraints
	if filter.ObjectSizeGreaterThan != nil && obj.Size <= *filter.ObjectSizeGreaterThan {
		return false
	}
	if filter.ObjectSizeLessThan != nil && obj.Size >= *filter.ObjectSizeLessThan {
		return false
	}

	// Check And operator
	if filter.And != nil {
		return e.andMatches(filter.And, obj)
	}

	return true
}

// andMatches checks if And filter matches
func (e *Evaluator) andMatches(and *LifecycleRuleAndOperator, obj ObjectState) bool {
	// Check prefix
	if and.Prefix != nil {
		if !strings.HasPrefix(obj.Name, *and.Prefix) {
			return false
		}
	}

	// Check all tags
	for _, tag := range and.Tags {
		if !e.objectHasTag(obj, tag.Key, tag.Value) {
			return false
		}
	}

	// Check size constraints
	if and.ObjectSizeGreaterThan != nil && obj.Size <= *and.ObjectSizeGreaterThan {
		return false
	}
	if and.ObjectSizeLessThan != nil && obj.Size >= *and.ObjectSizeLessThan {
		return false
	}

	return true
}

// objectHasTag checks if object has a specific tag
func (e *Evaluator) objectHasTag(obj ObjectState, key, value string) bool {
	if obj.UserTags == "" {
		return false
	}

	tags, err := url.ParseQuery(obj.UserTags)
	if err != nil {
		return false
	}

	tagValue := tags.Get(key)
	return tagValue == value
}

// evalExpiration evaluates expiration action
func (e *Evaluator) evalExpiration(rule LifecycleRule, obj ObjectState, now time.Time) Event {
	if rule.Expiration == nil {
		return Event{Action: NoneAction}
	}

	exp := rule.Expiration

	// Handle ExpiredObjectDeleteMarker
	if exp.ExpiredObjectDeleteMarker != nil && *exp.ExpiredObjectDeleteMarker {
		if obj.DeleteMarker && obj.IsLatest {
			// Delete marker with no other versions should be removed
			if obj.NumVersions == 1 {
				return Event{
					Action: DeleteVersionAction,
					RuleID: e.getRuleID(rule),
					Due:    now,
					Object: obj,
				}
			}
		}
		return Event{Action: NoneAction}
	}

	// Don't expire delete markers with Days/Date expiration
	if obj.DeleteMarker {
		return Event{Action: NoneAction}
	}

	// Only apply to latest version
	if !obj.IsLatest {
		return Event{Action: NoneAction}
	}

	// Check if object is old enough
	var due time.Time
	if exp.Days != nil {
		due = obj.ModTime.Add(time.Duration(*exp.Days) * 24 * time.Hour)
	} else if exp.Date != nil {
		due = exp.Date.Truncate(24 * time.Hour)
	} else {
		return Event{Action: NoneAction}
	}

	if now.Before(due) {
		return Event{Action: NoneAction}
	}

	return Event{
		Action: DeleteAction,
		RuleID: e.getRuleID(rule),
		Due:    due,
		Object: obj,
	}
}

// evalTransition evaluates transition action
func (e *Evaluator) evalTransition(rule LifecycleRule, obj ObjectState, now time.Time) Event {
	if len(rule.Transitions) == 0 {
		return Event{Action: NoneAction}
	}

	// Only apply to latest version
	if !obj.IsLatest {
		return Event{Action: NoneAction}
	}

	// Don't transition delete markers
	if obj.DeleteMarker {
		return Event{Action: NoneAction}
	}

	// Find the first transition that is due
	for _, transition := range rule.Transitions {
		var due time.Time
		if transition.Days != nil {
			due = obj.ModTime.Add(time.Duration(*transition.Days) * 24 * time.Hour)
		} else if transition.Date != nil {
			due = transition.Date.Truncate(24 * time.Hour)
		} else {
			continue
		}

		if now.Before(due) {
			continue
		}

		return Event{
			Action:       TransitionAction,
			RuleID:       e.getRuleID(rule),
			Due:          due,
			StorageClass: *transition.StorageClass,
			Object:       obj,
		}
	}

	return Event{Action: NoneAction}
}

// evalNoncurrentVersionExpiration evaluates noncurrent version expiration
func (e *Evaluator) evalNoncurrentVersionExpiration(rule LifecycleRule, obj ObjectState, now time.Time) Event {
	if rule.NoncurrentVersionExpiration == nil {
		return Event{Action: NoneAction}
	}

	// Only apply to noncurrent versions
	if obj.IsLatest {
		return Event{Action: NoneAction}
	}

	exp := rule.NoncurrentVersionExpiration

	// Check NewerNoncurrentVersions - if set, we need external logic to determine
	// version ordering, so return NoneAction for now
	if exp.NewerNoncurrentVersions != nil {
		return Event{Action: NoneAction}
	}

	if exp.Days == nil {
		return Event{Action: NoneAction}
	}

	// Calculate when this version became noncurrent
	// Use SuccessorModTime if available, otherwise use ModTime
	becameNoncurrent := obj.SuccessorModTime
	if becameNoncurrent.IsZero() {
		becameNoncurrent = obj.ModTime
	}

	due := becameNoncurrent.Add(time.Duration(*exp.Days) * 24 * time.Hour)

	if now.Before(due) {
		return Event{Action: NoneAction}
	}

	return Event{
		Action: DeleteVersionAction,
		RuleID: e.getRuleID(rule),
		Due:    due,
		Object: obj,
	}
}

// evalNoncurrentVersionTransition evaluates noncurrent version transition
func (e *Evaluator) evalNoncurrentVersionTransition(rule LifecycleRule, obj ObjectState, now time.Time) Event {
	if rule.NoncurrentVersionTransition == nil {
		return Event{Action: NoneAction}
	}

	// Only apply to noncurrent versions
	if obj.IsLatest {
		return Event{Action: NoneAction}
	}

	trans := rule.NoncurrentVersionTransition

	// Check NewerNoncurrentVersions - if set, we need external logic
	if trans.NewerNoncurrentVersions != nil {
		return Event{Action: NoneAction}
	}

	if trans.Days == nil || trans.StorageClass == nil {
		return Event{Action: NoneAction}
	}

	// Calculate when this version became noncurrent
	becameNoncurrent := obj.SuccessorModTime
	if becameNoncurrent.IsZero() {
		becameNoncurrent = obj.ModTime
	}

	due := becameNoncurrent.Add(time.Duration(*trans.Days) * 24 * time.Hour)

	if now.Before(due) {
		return Event{Action: NoneAction}
	}

	return Event{
		Action:       TransitionVersionAction,
		RuleID:       e.getRuleID(rule),
		Due:          due,
		StorageClass: *trans.StorageClass,
		Object:       obj,
	}
}

// evalAbortMultipartUpload evaluates abort incomplete multipart upload
// Note: This requires tracking multipart upload state separately
func (e *Evaluator) evalAbortMultipartUpload(rule LifecycleRule, obj ObjectState, now time.Time) Event {
	if rule.AbortIncompleteMultipartUpload == nil {
		return Event{Action: NoneAction}
	}

	// This action is for incomplete multipart uploads, not regular objects
	// In practice, you'd query your multipart upload tracking system
	// and return AbortMultipartUploadAction for uploads that match

	return Event{Action: NoneAction}
}

// getRuleID gets the rule ID, or empty string if not set
func (e *Evaluator) getRuleID(rule LifecycleRule) string {
	if rule.ID == nil {
		return ""
	}
	return *rule.ID
}

// HasActiveRules checks if lifecycle has any active rules that could affect the given prefix
func (lc *Lifecycle) HasActiveRules(prefix string) bool {
	for _, rule := range lc.Rules {
		if rule.Status != LifecycleStatusEnabled {
			continue
		}

		if rule.Filter == nil {
			// No filter means it applies to everything
			return true
		}

		// Check if rule's prefix matches or overlaps with given prefix
		if rule.Filter.Prefix != nil {
			rulePrefix := *rule.Filter.Prefix
			// Rule matches if:
			// 1. Rule prefix is empty (matches everything)
			// 2. Given prefix starts with rule prefix
			// 3. Rule prefix starts with given prefix
			if rulePrefix == "" || strings.HasPrefix(prefix, rulePrefix) || strings.HasPrefix(rulePrefix, prefix) {
				return true
			}
		}

		// Check And filter
		if rule.Filter.And != nil && rule.Filter.And.Prefix != nil {
			rulePrefix := *rule.Filter.And.Prefix
			if rulePrefix == "" || strings.HasPrefix(prefix, rulePrefix) || strings.HasPrefix(rulePrefix, prefix) {
				return true
			}
		}

		// If filter has no prefix but has other criteria (tags, size), it could still apply
		if rule.Filter.Tag != nil || rule.Filter.ObjectSizeGreaterThan != nil || rule.Filter.ObjectSizeLessThan != nil {
			return true
		}
		if rule.Filter.And != nil {
			return true
		}
	}

	return false
}
