package matcher

import (
	"fmt"
	"strings"
	"sync"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// CompiledPrimitive represents a pre-compiled primitive with optimized match functions
// This structure is designed for zero-allocation evaluation during runtime
type CompiledPrimitive struct {
	// Pre-parsed field path (e.g., ["nested", "field"] for "nested.field")
	FieldPath []string

	// Pre-compiled match function for zero-allocation evaluation
	MatchFn MatchFn

	// Pre-compiled modifier pipeline applied in sequence
	ModifierChain []ModifierFn

	// Pre-allocated values for matching
	Values []string

	// Raw modifier names for reference and debugging
	RawModifiers []string

	// Field path as a dot-separated string (cached for performance)
	fieldPathString string

	// Whether all values are literal (no wildcards)
	isLiteralOnly bool

	// Estimated memory usage
	memoryUsage int
}

// NewCompiledPrimitive creates a new compiled primitive
func NewCompiledPrimitive(
	fieldPath []string,
	matchFn MatchFn,
	modifierChain []ModifierFn,
	values []string,
	rawModifiers []string,
) *CompiledPrimitive {
	// Deep copy slices to avoid external modifications
	fieldPathCopy := make([]string, len(fieldPath))
	copy(fieldPathCopy, fieldPath)

	valuesCopy := make([]string, len(values))
	copy(valuesCopy, values)

	modifiersCopy := make([]string, len(rawModifiers))
	copy(modifiersCopy, rawModifiers)

	modifierChainCopy := make([]ModifierFn, len(modifierChain))
	copy(modifierChainCopy, modifierChain)

	fieldPathString := strings.Join(fieldPath, ".")
	isLiteralOnly := calculateIsLiteralOnly(values)
	memoryUsage := calculateMemoryUsage(fieldPathCopy, valuesCopy, modifiersCopy)

	return &CompiledPrimitive{
		FieldPath:       fieldPathCopy,
		MatchFn:         matchFn,
		ModifierChain:   modifierChainCopy,
		Values:          valuesCopy,
		RawModifiers:    modifiersCopy,
		fieldPathString: fieldPathString,
		isLiteralOnly:   isLiteralOnly,
		memoryUsage:     memoryUsage,
	}
}

// FieldPathString returns the field path as a dot-separated string
func (cp *CompiledPrimitive) FieldPathString() string {
	return cp.fieldPathString
}

// HasModifiers returns true if the primitive has any modifiers
func (cp *CompiledPrimitive) HasModifiers() bool {
	return len(cp.ModifierChain) > 0
}

// ValueCount returns the number of values this primitive matches against
func (cp *CompiledPrimitive) ValueCount() int {
	return len(cp.Values)
}

// IsLiteralOnly returns true if all values are literal (no wildcards or regex)
func (cp *CompiledPrimitive) IsLiteralOnly() bool {
	return cp.isLiteralOnly
}

// MemoryUsage returns the estimated memory usage in bytes
func (cp *CompiledPrimitive) MemoryUsage() int {
	return cp.memoryUsage
}

// Matches evaluates this primitive against an event context
func (cp *CompiledPrimitive) Matches(ctx *EventContext) (bool, error) {
	// Extract field value from event
	fieldValue, exists, err := ctx.GetFieldAsString(cp.fieldPathString)
	if err != nil {
		return false, fmt.Errorf("field extraction failed: %w", err)
	}
	if !exists {
		return false, nil // Field not found = no match
	}

	// Apply modifier chain to transform the field value
	transformedValue := fieldValue
	for _, modifier := range cp.ModifierChain {
		transformedValue, err = modifier(transformedValue)
		if err != nil {
			return false, fmt.Errorf("modifier failed: %w", err)
		}
	}

	// Apply match function
	matched, err := cp.MatchFn(transformedValue, cp.Values, cp.RawModifiers)
	if err != nil {
		return false, fmt.Errorf("match function failed: %w", err)
	}

	return matched, nil
}

// MatchesWithResult evaluates this primitive and returns detailed match result
func (cp *CompiledPrimitive) MatchesWithResult(ctx *EventContext) *MatchResult {
	result := NewMatchResult(false, cp.fieldPathString)

	// Extract field value from event
	fieldValue, exists, err := ctx.GetFieldAsString(cp.fieldPathString)
	if err != nil {
		return result.WithError(fmt.Errorf("field extraction failed: %w", err))
	}
	if !exists {
		return result // Field not found = no match
	}

	result.MatchedValue = fieldValue

	// Apply modifier chain to transform the field value
	transformedValue := fieldValue
	for _, modifier := range cp.ModifierChain {
		var modErr error
		transformedValue, modErr = modifier(transformedValue)
		if modErr != nil {
			return result.WithError(fmt.Errorf("modifier failed: %w", modErr))
		}
	}

	result.TransformedValue = transformedValue

	// Apply match function
	matched, err := cp.MatchFn(transformedValue, cp.Values, cp.RawModifiers)
	if err != nil {
		return result.WithError(fmt.Errorf("match function failed: %w", err))
	}

	result.Matched = matched
	return result
}

// Clone creates a deep copy of the compiled primitive
func (cp *CompiledPrimitive) Clone() *CompiledPrimitive {
	return NewCompiledPrimitive(
		cp.FieldPath,
		cp.MatchFn,
		cp.ModifierChain,
		cp.Values,
		cp.RawModifiers,
	)
}

// String returns a string representation for debugging
func (cp *CompiledPrimitive) String() string {
	return fmt.Sprintf("CompiledPrimitive{field=%s, values=%v, modifiers=%v}",
		cp.fieldPathString, cp.Values, cp.RawModifiers)
}

// FromPrimitive creates a CompiledPrimitive from an IR Primitive
func FromPrimitive(primitive ir.Primitive) (*CompiledPrimitive, error) {
	// Parse field path (split on dots for nested access)
	fieldPath := strings.Split(primitive.Field, ".")

	// Get match function from default registry
	matchFn, exists := GetDefaultMatcher(primitive.MatchType)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedMatchType, primitive.MatchType)
	}

	// Build modifier chain
	var modifierChain []ModifierFn
	for _, modifierName := range primitive.Modifiers {
		modifier, modExists := GetDefaultModifier(modifierName)
		if !modExists {
			// For now, skip unknown modifiers (could be configurable)
			continue
		}
		modifierChain = append(modifierChain, modifier)
	}

	return NewCompiledPrimitive(
		fieldPath,
		matchFn,
		modifierChain,
		primitive.Values,
		primitive.Modifiers,
	), nil
}

// calculateIsLiteralOnly checks if all values are literal (no wildcards or regex)
func calculateIsLiteralOnly(values []string) bool {
	for _, value := range values {
		// Check for common wildcard characters
		if strings.Contains(value, "*") ||
			strings.Contains(value, "?") ||
			strings.Contains(value, "[") ||
			strings.Contains(value, "\\") ||
			strings.Contains(value, "^") ||
			strings.Contains(value, "$") {
			return false
		}
	}
	return true
}

// calculateMemoryUsage estimates memory usage for the compiled primitive
func calculateMemoryUsage(fieldPath, values, modifiers []string) int {
	size := 0

	// Field path
	for _, part := range fieldPath {
		size += len(part) + 16 // string overhead
	}

	// Values
	for _, value := range values {
		size += len(value) + 16 // string overhead
	}

	// Modifiers
	for _, modifier := range modifiers {
		size += len(modifier) + 16 // string overhead
	}

	// Struct overhead
	size += 200

	return size
}

// CompiledPrimitivePool manages a pool of reusable CompiledPrimitive instances
type CompiledPrimitivePool struct {
	pool sync.Pool
}

// NewCompiledPrimitivePool creates a new primitive pool
func NewCompiledPrimitivePool() *CompiledPrimitivePool {
	return &CompiledPrimitivePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &CompiledPrimitive{}
			},
		},
	}
}

// Get retrieves a primitive from the pool
func (p *CompiledPrimitivePool) Get() *CompiledPrimitive {
	return p.pool.Get().(*CompiledPrimitive)
}

// Put returns a primitive to the pool after resetting it
func (p *CompiledPrimitivePool) Put(primitive *CompiledPrimitive) {
	// Reset the primitive
	primitive.FieldPath = nil
	primitive.MatchFn = nil
	primitive.ModifierChain = nil
	primitive.Values = nil
	primitive.RawModifiers = nil
	primitive.fieldPathString = ""
	primitive.isLiteralOnly = false
	primitive.memoryUsage = 0

	p.pool.Put(primitive)
}

// Default primitive pool instance
var defaultPrimitivePool = NewCompiledPrimitivePool()

// GetDefaultPrimitivePool returns the default global primitive pool
func GetDefaultPrimitivePool() *CompiledPrimitivePool {
	return defaultPrimitivePool
}

// CompiledPrimitiveStats provides statistics about compiled primitives
type CompiledPrimitiveStats struct {
	TotalPrimitives     int `json:"total_primitives"`
	LiteralPrimitives   int `json:"literal_primitives"`
	WildcardPrimitives  int `json:"wildcard_primitives"`
	TotalMemoryUsage    int `json:"total_memory_usage"`
	AverageMemoryUsage  int `json:"average_memory_usage"`
	TotalValues         int `json:"total_values"`
	TotalModifiers      int `json:"total_modifiers"`
	UniqueFieldPaths    int `json:"unique_field_paths"`
}

// CalculateStats computes statistics for a slice of compiled primitives
func CalculateStats(primitives []*CompiledPrimitive) *CompiledPrimitiveStats {
	if len(primitives) == 0 {
		return &CompiledPrimitiveStats{}
	}

	stats := &CompiledPrimitiveStats{
		TotalPrimitives: len(primitives),
	}

	fieldPaths := make(map[string]bool)
	totalMemory := 0

	for _, primitive := range primitives {
		if primitive.IsLiteralOnly() {
			stats.LiteralPrimitives++
		} else {
			stats.WildcardPrimitives++
		}

		totalMemory += primitive.MemoryUsage()
		stats.TotalValues += primitive.ValueCount()
		stats.TotalModifiers += len(primitive.RawModifiers)
		fieldPaths[primitive.FieldPathString()] = true
	}

	stats.TotalMemoryUsage = totalMemory
	stats.AverageMemoryUsage = totalMemory / len(primitives)
	stats.UniqueFieldPaths = len(fieldPaths)

	return stats
}
