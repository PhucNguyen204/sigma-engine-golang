package matcher

import (
	"fmt"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// MatcherBuilder provides a builder pattern for creating compiled primitives
// with custom match functions, modifiers, and compilation hooks
type MatcherBuilder struct {
	registry *MatcherRegistry
	compiled []*CompiledPrimitive
}

// NewMatcherBuilder creates a new matcher builder with default registry
func NewMatcherBuilder() *MatcherBuilder {
	// Create a new registry and register defaults
	registry := NewMatcherRegistry()

	return &MatcherBuilder{
		registry: registry,
		compiled: make([]*CompiledPrimitive, 0),
	}
}

// NewMatcherBuilderWithRegistry creates a new matcher builder with custom registry
func NewMatcherBuilderWithRegistry(registry *MatcherRegistry) *MatcherBuilder {
	return &MatcherBuilder{
		registry: registry,
		compiled: make([]*CompiledPrimitive, 0),
	}
}

// WithDefaults registers default matchers and modifiers
func (b *MatcherBuilder) WithDefaults() *MatcherBuilder {
	// Register defaults using the builder's registry
	b.registerDefaultsToRegistry()
	return b
}

// WithComprehensiveDefaults registers all available matchers and modifiers
func (b *MatcherBuilder) WithComprehensiveDefaults() *MatcherBuilder {
	b.registerDefaultsToRegistry()
	b.registerAdvancedToRegistry()
	return b
}

// RegisterMatcher registers a custom match function
func (b *MatcherBuilder) RegisterMatcher(name string, matcher MatchFn) *MatcherBuilder {
	b.registry.RegisterMatcher(name, matcher)
	return b
}

// RegisterModifier registers a custom modifier function
func (b *MatcherBuilder) RegisterModifier(name string, modifier ModifierFn) *MatcherBuilder {
	b.registry.RegisterModifier(name, modifier)
	return b
}

// Compile compiles a slice of primitives into CompiledPrimitives
func (b *MatcherBuilder) Compile(primitives []ir.Primitive) ([]*CompiledPrimitive, error) {
	compiled := make([]*CompiledPrimitive, 0, len(primitives))

	for i, primitive := range primitives {
		compiledPrimitive, err := b.CompilePrimitive(primitive)
		if err != nil {
			return nil, fmt.Errorf("failed to compile primitive %d: %w", i, err)
		}
		compiled = append(compiled, compiledPrimitive)
	}

	b.compiled = compiled
	return compiled, nil
}

// CompilePrimitive compiles a single primitive into a CompiledPrimitive
func (b *MatcherBuilder) CompilePrimitive(primitive ir.Primitive) (*CompiledPrimitive, error) {
	// Get match function
	matchFn, exists := b.registry.GetMatcher(primitive.MatchType)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedMatchType, primitive.MatchType)
	}

	// Build modifier chain
	var modifierChain []ModifierFn
	for _, modifierName := range primitive.Modifiers {
		modifier, modExists := b.registry.GetModifier(modifierName)
		if !modExists {
			// Skip unknown modifiers (could be configurable)
			continue
		}
		modifierChain = append(modifierChain, modifier)
	}

	// Parse field path
	fieldPath := parseFieldPath(primitive.Field)

	// Create compiled primitive
	compiled := NewCompiledPrimitive(
		fieldPath,
		matchFn,
		modifierChain,
		primitive.Values,
		primitive.Modifiers,
	)

	return compiled, nil
}

// GetCompiledPrimitives returns the currently compiled primitives
func (b *MatcherBuilder) GetCompiledPrimitives() []*CompiledPrimitive {
	return b.compiled
}

// GetRegistry returns the matcher registry
func (b *MatcherBuilder) GetRegistry() *MatcherRegistry {
	return b.registry
}

// Reset clears all compiled primitives
func (b *MatcherBuilder) Reset() *MatcherBuilder {
	b.compiled = make([]*CompiledPrimitive, 0)
	return b
}

// Stats returns compilation statistics
func (b *MatcherBuilder) Stats() *CompiledPrimitiveStats {
	return CalculateStats(b.compiled)
}

// Validate validates all compiled primitives
func (b *MatcherBuilder) Validate() error {
	for i, primitive := range b.compiled {
		if primitive == nil {
			return fmt.Errorf("primitive %d is nil", i)
		}
		if len(primitive.FieldPath) == 0 {
			return fmt.Errorf("primitive %d has empty field path", i)
		}
		if primitive.MatchFn == nil {
			return fmt.Errorf("primitive %d has nil match function", i)
		}
		if len(primitive.Values) == 0 {
			return fmt.Errorf("primitive %d has no values", i)
		}
	}
	return nil
}

// parseFieldPath splits a field path on dots and handles edge cases
func parseFieldPath(field string) []string {
	if field == "" {
		return []string{}
	}

	// Split on dots but handle escaped dots
	parts := make([]string, 0)
	current := ""
	escaped := false

	for i, char := range field {
		if char == '\\' && !escaped {
			escaped = true
			continue
		}

		if char == '.' && !escaped {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}

		escaped = false
		_ = i // Suppress unused variable warning
	}

	if current != "" {
		parts = append(parts, current)
	}

	return parts
}

// registerDefaultsToRegistry registers default matchers and modifiers to the builder's registry
func (b *MatcherBuilder) registerDefaultsToRegistry() {
	// Exact match functions
	b.registry.RegisterMatcher("equals", CreateExactMatch())
	b.registry.RegisterMatcher("exact", CreateExactMatch())

	// String matching functions
	b.registry.RegisterMatcher("contains", CreateContainsMatch())
	b.registry.RegisterMatcher("startswith", CreateStartsWithMatch())
	b.registry.RegisterMatcher("endswith", CreateEndsWithMatch())

	// Pattern matching functions
	b.registry.RegisterMatcher("regex", CreateRegexMatch())
	b.registry.RegisterMatcher("re", CreateRegexMatch())

	// Wildcard matching functions
	b.registry.RegisterMatcher("glob", CreateGlobMatch())
	b.registry.RegisterMatcher("wildcard", CreateGlobMatch())

	// Case transformation
	b.registry.RegisterModifier("lowercase", CreateLowercaseModifier())
	b.registry.RegisterModifier("uppercase", CreateUppercaseModifier())

	// Encoding/decoding
	b.registry.RegisterModifier("base64", CreateBase64DecodeModifier())
	b.registry.RegisterModifier("base64decode", CreateBase64DecodeModifier())

	// String manipulation
	b.registry.RegisterModifier("trim", CreateTrimModifier())
	b.registry.RegisterModifier("trimspace", CreateTrimModifier())
}

// registerAdvancedToRegistry registers advanced matchers to the builder's registry
func (b *MatcherBuilder) registerAdvancedToRegistry() {
	// Case-insensitive matchers
	b.registry.RegisterMatcher("iequals", CreateCaseInsensitiveMatch())
	b.registry.RegisterMatcher("icontains", CreateCaseInsensitiveContains())

	// Numeric matchers
	b.registry.RegisterMatcher("numeric", CreateNumericMatch())
}

// MatcherEvaluator provides evaluation capabilities for compiled primitives
type MatcherEvaluator struct {
	primitives []*CompiledPrimitive
}

// NewMatcherEvaluator creates a new evaluator with compiled primitives
func NewMatcherEvaluator(primitives []*CompiledPrimitive) *MatcherEvaluator {
	return &MatcherEvaluator{
		primitives: primitives,
	}
}

// Evaluate evaluates all primitives against an event
func (e *MatcherEvaluator) Evaluate(event interface{}) ([]bool, error) {
	ctx := NewEventContext(event)
	results := make([]bool, len(e.primitives))

	for i, primitive := range e.primitives {
		matched, err := primitive.Matches(ctx)
		if err != nil {
			return nil, fmt.Errorf("primitive %d evaluation failed: %w", i, err)
		}
		results[i] = matched
	}

	return results, nil
}

// EvaluateWithResults evaluates all primitives and returns detailed results
func (e *MatcherEvaluator) EvaluateWithResults(event interface{}) ([]*MatchResult, error) {
	ctx := NewEventContext(event)
	results := make([]*MatchResult, len(e.primitives))

	for i, primitive := range e.primitives {
		result := primitive.MatchesWithResult(ctx)
		results[i] = result
	}

	return results, nil
}

// EvaluateWithContext evaluates all primitives with a custom event context
func (e *MatcherEvaluator) EvaluateWithContext(ctx *EventContext) ([]bool, error) {
	results := make([]bool, len(e.primitives))

	for i, primitive := range e.primitives {
		matched, err := primitive.Matches(ctx)
		if err != nil {
			return nil, fmt.Errorf("primitive %d evaluation failed: %w", i, err)
		}
		results[i] = matched
	}

	return results, nil
}

// GetPrimitives returns the compiled primitives
func (e *MatcherEvaluator) GetPrimitives() []*CompiledPrimitive {
	return e.primitives
}

// PrimitiveCount returns the number of primitives
func (e *MatcherEvaluator) PrimitiveCount() int {
	return len(e.primitives)
}

// BuildEvaluator builds both compiler and evaluator in one step
func (b *MatcherBuilder) BuildEvaluator(primitives []ir.Primitive) (*MatcherEvaluator, error) {
	compiled, err := b.Compile(primitives)
	if err != nil {
		return nil, err
	}

	return NewMatcherEvaluator(compiled), nil
}

// QuickBuild provides a convenient way to build an evaluator with defaults
func QuickBuild(primitives []ir.Primitive) (*MatcherEvaluator, error) {
	builder := NewMatcherBuilder().WithDefaults()
	return builder.BuildEvaluator(primitives)
}

// QuickBuildComprehensive provides a convenient way to build an evaluator with all features
func QuickBuildComprehensive(primitives []ir.Primitive) (*MatcherEvaluator, error) {
	builder := NewMatcherBuilder().WithComprehensiveDefaults()
	return builder.BuildEvaluator(primitives)
}
