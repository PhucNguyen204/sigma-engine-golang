package matcher

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// EventContext provides efficient field value extraction and caching for events
// This is designed for zero-allocation repeated field access during evaluation
type EventContext struct {
	event     interface{}
	cache     map[string]interface{}
	cacheMux  sync.RWMutex
	extractor FieldExtractorFn
}

// NewEventContext creates a new event context with the given event
func NewEventContext(event interface{}) *EventContext {
	return &EventContext{
		event:     event,
		cache:     make(map[string]interface{}),
		extractor: DefaultFieldExtractor,
	}
}

// NewEventContextWithExtractor creates a new event context with a custom field extractor
func NewEventContextWithExtractor(event interface{}, extractor FieldExtractorFn) *EventContext {
	return &EventContext{
		event:     event,
		cache:     make(map[string]interface{}),
		extractor: extractor,
	}
}

// GetField extracts a field value from the event using dot notation
// Returns the field value and whether it was found
func (ctx *EventContext) GetField(fieldPath string) (interface{}, bool, error) {
	// Check cache first
	ctx.cacheMux.RLock()
	if value, exists := ctx.cache[fieldPath]; exists {
		ctx.cacheMux.RUnlock()
		return value, true, nil
	}
	ctx.cacheMux.RUnlock()

	// Extract field value
	value, err := ctx.extractor(ctx.event, fieldPath)
	if err != nil {
		return nil, false, err
	}

	// Cache the result
	ctx.cacheMux.Lock()
	ctx.cache[fieldPath] = value
	ctx.cacheMux.Unlock()

	return value, value != nil, nil
}

// GetFieldAsString extracts a field value and converts it to string
func (ctx *EventContext) GetFieldAsString(fieldPath string) (string, bool, error) {
	value, exists, err := ctx.GetField(fieldPath)
	if err != nil || !exists {
		return "", exists, err
	}

	if value == nil {
		return "", false, nil
	}

	return fmt.Sprintf("%v", value), true, nil
}

// GetFieldAsStringSlice extracts a field value and converts it to string slice
func (ctx *EventContext) GetFieldAsStringSlice(fieldPath string) ([]string, bool, error) {
	value, exists, err := ctx.GetField(fieldPath)
	if err != nil || !exists {
		return nil, exists, err
	}

	if value == nil {
		return nil, false, nil
	}

	// Handle different types
	switch v := value.(type) {
	case []string:
		return v, true, nil
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			result[i] = fmt.Sprintf("%v", item)
		}
		return result, true, nil
	case string:
		return []string{v}, true, nil
	default:
		return []string{fmt.Sprintf("%v", v)}, true, nil
	}
}

// HasField checks if a field exists in the event
func (ctx *EventContext) HasField(fieldPath string) bool {
	_, exists, _ := ctx.GetField(fieldPath)
	return exists
}

// ClearCache clears the field value cache
func (ctx *EventContext) ClearCache() {
	ctx.cacheMux.Lock()
	defer ctx.cacheMux.Unlock()
	ctx.cache = make(map[string]interface{})
}

// CacheSize returns the number of cached field values
func (ctx *EventContext) CacheSize() int {
	ctx.cacheMux.RLock()
	defer ctx.cacheMux.RUnlock()
	return len(ctx.cache)
}

// GetEvent returns the underlying event object
func (ctx *EventContext) GetEvent() interface{} {
	return ctx.event
}

// SetExtractor sets a custom field extractor
func (ctx *EventContext) SetExtractor(extractor FieldExtractorFn) {
	ctx.extractor = extractor
}

// DefaultFieldExtractor is the default implementation for extracting field values
// Supports map[string]interface{} and struct field access with dot notation
func DefaultFieldExtractor(event interface{}, fieldPath string) (interface{}, error) {
	if event == nil {
		return nil, ErrFieldNotFound
	}

	// Split field path on dots
	parts := strings.Split(fieldPath, ".")
	if len(parts) == 0 {
		return nil, ErrFieldNotFound
	}

	current := event
	for _, part := range parts {
		if current == nil {
			return nil, ErrFieldNotFound
		}

		// Handle map access
		if m, ok := current.(map[string]interface{}); ok {
			value, exists := m[part]
			if !exists {
				return nil, ErrFieldNotFound
			}
			current = value
			continue
		}

		// Handle struct access using reflection
		v := reflect.ValueOf(current)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return nil, ErrFieldNotFound
			}
			v = v.Elem()
		}

		if v.Kind() != reflect.Struct {
			return nil, ErrFieldExtractionFailed
		}

		field := v.FieldByName(part)
		if !field.IsValid() {
			// Try case-insensitive match
			field = findFieldCaseInsensitive(v, part)
			if !field.IsValid() {
				return nil, ErrFieldNotFound
			}
		}

		if !field.CanInterface() {
			return nil, ErrFieldExtractionFailed
		}

		current = field.Interface()
	}

	return current, nil
}

// findFieldCaseInsensitive finds a struct field by name (case-insensitive)
func findFieldCaseInsensitive(v reflect.Value, name string) reflect.Value {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if strings.EqualFold(field.Name, name) {
			return v.Field(i)
		}
		
		// Check json tag
		if jsonTag := field.Tag.Get("json"); jsonTag != "" {
			tagName := strings.Split(jsonTag, ",")[0]
			if strings.EqualFold(tagName, name) {
				return v.Field(i)
			}
		}
	}
	return reflect.Value{}
}

// JsonFieldExtractor is a field extractor optimized for JSON-like map structures
func JsonFieldExtractor(event interface{}, fieldPath string) (interface{}, error) {
	if event == nil {
		return nil, ErrFieldNotFound
	}

	// Only handle map[string]interface{} for JSON events
	m, ok := event.(map[string]interface{})
	if !ok {
		return nil, ErrFieldExtractionFailed
	}

	// Split field path on dots
	parts := strings.Split(fieldPath, ".")
	current := interface{}(m)

	for _, part := range parts {
		if current == nil {
			return nil, ErrFieldNotFound
		}

		if currentMap, ok := current.(map[string]interface{}); ok {
			value, exists := currentMap[part]
			if !exists {
				return nil, ErrFieldNotFound
			}
			current = value
		} else {
			return nil, ErrFieldNotFound
		}
	}

	return current, nil
}

// CaseSensitiveFieldExtractor is a field extractor that enforces case-sensitive field matching
func CaseSensitiveFieldExtractor(event interface{}, fieldPath string) (interface{}, error) {
	if event == nil {
		return nil, ErrFieldNotFound
	}

	// Split field path on dots
	parts := strings.Split(fieldPath, ".")
	current := event

	for _, part := range parts {
		if current == nil {
			return nil, ErrFieldNotFound
		}

		// Handle map access (case-sensitive)
		if m, ok := current.(map[string]interface{}); ok {
			value, exists := m[part]
			if !exists {
				return nil, ErrFieldNotFound
			}
			current = value
			continue
		}

		// Handle struct access (case-sensitive)
		v := reflect.ValueOf(current)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return nil, ErrFieldNotFound
			}
			v = v.Elem()
		}

		if v.Kind() != reflect.Struct {
			return nil, ErrFieldExtractionFailed
		}

		field := v.FieldByName(part)
		if !field.IsValid() || !field.CanInterface() {
			return nil, ErrFieldNotFound
		}

		current = field.Interface()
	}

	return current, nil
}

// FlatFieldExtractor treats all field paths as flat keys (no dot notation)
func FlatFieldExtractor(event interface{}, fieldPath string) (interface{}, error) {
	if event == nil {
		return nil, ErrFieldNotFound
	}

	// Only handle map access for flat fields
	if m, ok := event.(map[string]interface{}); ok {
		value, exists := m[fieldPath]
		if !exists {
			return nil, ErrFieldNotFound
		}
		return value, nil
	}

	return nil, ErrFieldExtractionFailed
}
