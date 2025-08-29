package matcher

import (
	"errors"
	"sync"
)

// MatchFn represents a function that matches field values against primitive values
// field_value: the value from the event
// values: the values to match against from the primitive
// modifiers: applied modifiers for this match
type MatchFn func(fieldValue string, values []string, modifiers []string) (bool, error)

// ModifierFn represents a function that transforms a field value
// input: the original field value
// returns: transformed value or error
type ModifierFn func(input string) (string, error)

// FieldExtractorFn represents a function that extracts field values from events
// event: the event data
// fieldPath: the field path to extract (e.g., "nested.field")
// returns: extracted value or error
type FieldExtractorFn func(event interface{}, fieldPath string) (interface{}, error)

// MatchResult represents the result of matching a primitive against an event
type MatchResult struct {
	Matched        bool   `json:"matched"`
	FieldPath      string `json:"field_path"`
	MatchedValue   string `json:"matched_value,omitempty"`
	TransformedValue string `json:"transformed_value,omitempty"`
	Error          string `json:"error,omitempty"`
}

// NewMatchResult creates a new match result
func NewMatchResult(matched bool, fieldPath string) *MatchResult {
	return &MatchResult{
		Matched:   matched,
		FieldPath: fieldPath,
	}
}

// WithMatchedValue sets the matched value
func (mr *MatchResult) WithMatchedValue(value string) *MatchResult {
	mr.MatchedValue = value
	return mr
}

// WithTransformedValue sets the transformed value
func (mr *MatchResult) WithTransformedValue(value string) *MatchResult {
	mr.TransformedValue = value
	return mr
}

// WithError sets an error message
func (mr *MatchResult) WithError(err error) *MatchResult {
	if err != nil {
		mr.Error = err.Error()
		mr.Matched = false
	}
	return mr
}

// MatcherRegistry manages the registration and lookup of match functions
type MatcherRegistry struct {
	matchers  map[string]MatchFn
	modifiers map[string]ModifierFn
	mutex     sync.RWMutex
}

// NewMatcherRegistry creates a new matcher registry
func NewMatcherRegistry() *MatcherRegistry {
	return &MatcherRegistry{
		matchers:  make(map[string]MatchFn),
		modifiers: make(map[string]ModifierFn),
	}
}

// RegisterMatcher registers a match function
func (r *MatcherRegistry) RegisterMatcher(name string, matcher MatchFn) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.matchers[name] = matcher
}

// RegisterModifier registers a modifier function
func (r *MatcherRegistry) RegisterModifier(name string, modifier ModifierFn) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.modifiers[name] = modifier
}

// GetMatcher retrieves a match function by name
func (r *MatcherRegistry) GetMatcher(name string) (MatchFn, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	matcher, exists := r.matchers[name]
	return matcher, exists
}

// GetModifier retrieves a modifier function by name
func (r *MatcherRegistry) GetModifier(name string) (ModifierFn, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	modifier, exists := r.modifiers[name]
	return modifier, exists
}

// ListMatchers returns all registered matcher names
func (r *MatcherRegistry) ListMatchers() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	names := make([]string, 0, len(r.matchers))
	for name := range r.matchers {
		names = append(names, name)
	}
	return names
}

// ListModifiers returns all registered modifier names
func (r *MatcherRegistry) ListModifiers() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	names := make([]string, 0, len(r.modifiers))
	for name := range r.modifiers {
		names = append(names, name)
	}
	return names
}

// MatcherCount returns the number of registered matchers
func (r *MatcherRegistry) MatcherCount() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return len(r.matchers)
}

// ModifierCount returns the number of registered modifiers
func (r *MatcherRegistry) ModifierCount() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return len(r.modifiers)
}

// Clear removes all registered matchers and modifiers
func (r *MatcherRegistry) Clear() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.matchers = make(map[string]MatchFn)
	r.modifiers = make(map[string]ModifierFn)
}

// Common errors
var (
	ErrUnsupportedMatchType = errors.New("unsupported match type")
	ErrUnsupportedModifier  = errors.New("unsupported modifier")
	ErrFieldNotFound        = errors.New("field not found")
	ErrFieldExtractionFailed = errors.New("field extraction failed")
	ErrInvalidFieldValue    = errors.New("invalid field value")
	ErrMatchFunctionFailed  = errors.New("match function failed")
	ErrModifierFailed       = errors.New("modifier function failed")
)

// Default registry instance (can be used globally)
var defaultRegistry = NewMatcherRegistry()

// GetDefaultRegistry returns the default global registry
func GetDefaultRegistry() *MatcherRegistry {
	return defaultRegistry
}

// RegisterDefaultMatcher registers a matcher in the default registry
func RegisterDefaultMatcher(name string, matcher MatchFn) {
	defaultRegistry.RegisterMatcher(name, matcher)
}

// RegisterDefaultModifier registers a modifier in the default registry
func RegisterDefaultModifier(name string, modifier ModifierFn) {
	defaultRegistry.RegisterModifier(name, modifier)
}

// GetDefaultMatcher retrieves a matcher from the default registry
func GetDefaultMatcher(name string) (MatchFn, bool) {
	return defaultRegistry.GetMatcher(name)
}

// GetDefaultModifier retrieves a modifier from the default registry
func GetDefaultModifier(name string) (ModifierFn, bool) {
	return defaultRegistry.GetModifier(name)
}
