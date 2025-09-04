package matcher

import (
	"testing"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

func TestMatcherRegistry(t *testing.T) {
	registry := NewMatcherRegistry()

	// Test initial state
	if registry.MatcherCount() != 0 {
		t.Errorf("Expected 0 matchers, got %d", registry.MatcherCount())
	}
	if registry.ModifierCount() != 0 {
		t.Errorf("Expected 0 modifiers, got %d", registry.ModifierCount())
	}

	// Test matcher registration
	testMatcher := func(fieldValue string, values []string, modifiers []string) (bool, error) {
		return fieldValue == "test", nil
	}
	registry.RegisterMatcher("test", testMatcher)

	if registry.MatcherCount() != 1 {
		t.Errorf("Expected 1 matcher, got %d", registry.MatcherCount())
	}

	// Test matcher retrieval
	matcher, exists := registry.GetMatcher("test")
	if !exists {
		t.Error("Expected matcher to exist")
	}
	if matcher == nil {
		t.Error("Expected non-nil matcher")
	}

	// Test modifier registration
	testModifier := func(input string) (string, error) {
		return input + "_modified", nil
	}
	registry.RegisterModifier("test", testModifier)

	if registry.ModifierCount() != 1 {
		t.Errorf("Expected 1 modifier, got %d", registry.ModifierCount())
	}

	// Test modifier retrieval
	modifier, exists := registry.GetModifier("test")
	if !exists {
		t.Error("Expected modifier to exist")
	}
	if modifier == nil {
		t.Error("Expected non-nil modifier")
	}

	// Test clear
	registry.Clear()
	if registry.MatcherCount() != 0 {
		t.Errorf("Expected 0 matchers after clear, got %d", registry.MatcherCount())
	}
	if registry.ModifierCount() != 0 {
		t.Errorf("Expected 0 modifiers after clear, got %d", registry.ModifierCount())
	}
}

func TestEventContext(t *testing.T) {
	// Test with map event
	event := map[string]interface{}{
		"EventID":     "4624",
		"ProcessName": "explorer.exe",
		"nested": map[string]interface{}{
			"field": "value",
		},
	}

	ctx := NewEventContext(event)

	// Test field extraction
	value, exists, err := ctx.GetField("EventID")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !exists {
		t.Error("Expected field to exist")
	}
	if value != "4624" {
		t.Errorf("Expected '4624', got '%v'", value)
	}

	// Test nested field extraction
	value, exists, err = ctx.GetField("nested.field")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !exists {
		t.Error("Expected nested field to exist")
	}
	if value != "value" {
		t.Errorf("Expected 'value', got '%v'", value)
	}

	// Test field as string
	strValue, exists, err := ctx.GetFieldAsString("EventID")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !exists {
		t.Error("Expected field to exist")
	}
	if strValue != "4624" {
		t.Errorf("Expected '4624', got '%s'", strValue)
	}

	// Test non-existent field
	_, exists, _ = ctx.GetField("NonExistent")
	if exists {
		t.Error("Expected field to not exist")
	}

	// Test cache
	if ctx.CacheSize() == 0 {
		t.Error("Expected cache to have entries")
	}

	ctx.ClearCache()
	if ctx.CacheSize() != 0 {
		t.Error("Expected cache to be empty after clear")
	}
}

func TestCompiledPrimitive(t *testing.T) {
	// Create test match function
	matchFn := func(fieldValue string, values []string, modifiers []string) (bool, error) {
		for _, value := range values {
			if fieldValue == value {
				return true, nil
			}
		}
		return false, nil
	}

	// Create test modifier
	modifierFn := func(input string) (string, error) {
		return input + "_modified", nil
	}

	// Create compiled primitive
	primitive := NewCompiledPrimitive(
		[]string{"EventID"},
		matchFn,
		[]ModifierFn{modifierFn},
		[]string{"4624", "4625"},
		[]string{"test_modifier"},
	)

	// Test basic properties
	if primitive.FieldPathString() != "EventID" {
		t.Errorf("Expected 'EventID', got '%s'", primitive.FieldPathString())
	}

	if !primitive.HasModifiers() {
		t.Error("Expected primitive to have modifiers")
	}

	if primitive.ValueCount() != 2 {
		t.Errorf("Expected 2 values, got %d", primitive.ValueCount())
	}

	if !primitive.IsLiteralOnly() {
		t.Error("Expected primitive to be literal only")
	}

	// Test matching (considering modifier transforms "4624" to "4624_modified")
	event := map[string]interface{}{
		"EventID": "4624",
	}
	ctx := NewEventContext(event)

	// Since the modifier adds "_modified", the field value becomes "4624_modified"
	// but we're still matching against ["4624", "4625"], so it won't match
	matched, err := primitive.Matches(ctx)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	// This should not match because "4624_modified" != "4624"
	if matched {
		t.Error("Expected primitive to not match due to modifier transformation")
	}

	// Test non-matching
	event2 := map[string]interface{}{
		"EventID": "1234",
	}
	ctx2 := NewEventContext(event2)

	matched, err = primitive.Matches(ctx2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if matched {
		t.Error("Expected primitive to not match")
	}
}

func TestDefaultMatchers(t *testing.T) {
	// Test exact match
	exactMatch := CreateExactMatch()
	matched, err := exactMatch("test", []string{"test", "other"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !matched {
		t.Error("Expected exact match to succeed")
	}

	matched, err = exactMatch("nomatch", []string{"test", "other"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if matched {
		t.Error("Expected exact match to fail")
	}

	// Test contains match
	containsMatch := CreateContainsMatch()
	matched, err = containsMatch("this is a test", []string{"test"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !matched {
		t.Error("Expected contains match to succeed")
	}

	// Test starts with match
	startsWithMatch := CreateStartsWithMatch()
	matched, err = startsWithMatch("testing", []string{"test"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !matched {
		t.Error("Expected starts with match to succeed")
	}

	// Test ends with match
	endsWithMatch := CreateEndsWithMatch()
	matched, err = endsWithMatch("mytest", []string{"test"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !matched {
		t.Error("Expected ends with match to succeed")
	}
}

func TestDefaultModifiers(t *testing.T) {
	// Test lowercase modifier
	lowercaseModifier := CreateLowercaseModifier()
	result, err := lowercaseModifier("TEST")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result != "test" {
		t.Errorf("Expected 'test', got '%s'", result)
	}

	// Test uppercase modifier
	uppercaseModifier := CreateUppercaseModifier()
	result, err = uppercaseModifier("test")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result != "TEST" {
		t.Errorf("Expected 'TEST', got '%s'", result)
	}

	// Test trim modifier
	trimModifier := CreateTrimModifier()
	result, err = trimModifier("  test  ")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result != "test" {
		t.Errorf("Expected 'test', got '%s'", result)
	}
}

func TestMatcherBuilder(t *testing.T) {
	builder := NewMatcherBuilder().WithDefaults()

	// Test builder state
	if builder.GetRegistry().MatcherCount() == 0 {
		t.Error("Expected builder to have registered matchers")
	}

	// Create test primitives
	primitives := []ir.Primitive{
		{
			Field:     "EventID",
			MatchType: "equals",
			Values:    []string{"4624"},
			Modifiers: []string{},
		},
		{
			Field:     "ProcessName",
			MatchType: "contains",
			Values:    []string{"explorer"},
			Modifiers: []string{"lowercase"},
		},
	}

	// Compile primitives
	compiled, err := builder.Compile(primitives)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(compiled) != 2 {
		t.Errorf("Expected 2 compiled primitives, got %d", len(compiled))
	}

	// Test validation
	err = builder.Validate()
	if err != nil {
		t.Errorf("Validation failed: %v", err)
	}

	// Test stats
	stats := builder.Stats()
	if stats.TotalPrimitives != 2 {
		t.Errorf("Expected 2 total primitives, got %d", stats.TotalPrimitives)
	}
}

func TestMatcherEvaluator(t *testing.T) {
	// Build evaluator
	primitives := []ir.Primitive{
		{
			Field:     "EventID",
			MatchType: "equals",
			Values:    []string{"4624"},
			Modifiers: []string{},
		},
		{
			Field:     "ProcessName",
			MatchType: "contains",
			Values:    []string{"explorer"},
			Modifiers: []string{},
		},
	}

	evaluator, err := QuickBuild(primitives)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if evaluator.PrimitiveCount() != 2 {
		t.Errorf("Expected 2 primitives, got %d", evaluator.PrimitiveCount())
	}

	// Test matching event
	event := map[string]interface{}{
		"EventID":     "4624",
		"ProcessName": "explorer.exe",
	}

	results, err := evaluator.Evaluate(event)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	if !results[0] {
		t.Error("Expected first primitive to match")
	}

	if !results[1] {
		t.Error("Expected second primitive to match")
	}

	// Test non-matching event
	event2 := map[string]interface{}{
		"EventID":     "1234",
		"ProcessName": "notepad.exe",
	}

	results, err = evaluator.Evaluate(event2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if results[0] {
		t.Error("Expected first primitive to not match")
	}

	if results[1] {
		t.Error("Expected second primitive to not match")
	}
}

func TestFromPrimitive(t *testing.T) {
	// Register defaults first
	RegisterDefaults()

	primitive := ir.Primitive{
		Field:     "EventID",
		MatchType: "equals",
		Values:    []string{"4624"},
		Modifiers: []string{"lowercase"},
	}

	compiled, err := FromPrimitive(primitive)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if compiled.FieldPathString() != "EventID" {
		t.Errorf("Expected 'EventID', got '%s'", compiled.FieldPathString())
	}

	if compiled.ValueCount() != 1 {
		t.Errorf("Expected 1 value, got %d", compiled.ValueCount())
	}

	if compiled.Values[0] != "4624" {
		t.Errorf("Expected '4624', got '%s'", compiled.Values[0])
	}
}

func TestGlobMatch(t *testing.T) {
	globMatcher := CreateGlobMatch()

	// Test simple wildcard
	matched, err := globMatcher("test.exe", []string{"*.exe"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !matched {
		t.Error("Expected glob match to succeed")
	}

	// Test question mark wildcard
	matched, err = globMatcher("test", []string{"t?st"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !matched {
		t.Error("Expected glob match to succeed")
	}

	// Test no match
	matched, err = globMatcher("testing", []string{"*.exe"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if matched {
		t.Error("Expected glob match to fail")
	}
}

func TestRegexMatch(t *testing.T) {
	regexMatcher := CreateRegexMatch()

	// Test simple regex
	matched, err := regexMatcher("test123", []string{"test\\d+"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !matched {
		t.Error("Expected regex match to succeed")
	}

	// Test no match
	matched, err = regexMatcher("testing", []string{"test\\d+"}, []string{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if matched {
		t.Error("Expected regex match to fail")
	}

	// Test invalid regex
	_, err = regexMatcher("test", []string{"[invalid"}, []string{})
	if err == nil {
		t.Error("Expected error for invalid regex")
	}
}

func TestCompiledPrimitiveStats(t *testing.T) {
	primitives := []*CompiledPrimitive{
		NewCompiledPrimitive(
			[]string{"EventID"},
			CreateExactMatch(),
			[]ModifierFn{},
			[]string{"4624"},
			[]string{},
		),
		NewCompiledPrimitive(
			[]string{"ProcessName"},
			CreateContainsMatch(),
			[]ModifierFn{CreateLowercaseModifier()},
			[]string{"test*"},
			[]string{"lowercase"},
		),
	}

	stats := CalculateStats(primitives)

	if stats.TotalPrimitives != 2 {
		t.Errorf("Expected 2 total primitives, got %d", stats.TotalPrimitives)
	}

	if stats.LiteralPrimitives != 1 {
		t.Errorf("Expected 1 literal primitive, got %d", stats.LiteralPrimitives)
	}

	if stats.WildcardPrimitives != 1 {
		t.Errorf("Expected 1 wildcard primitive, got %d", stats.WildcardPrimitives)
	}

	if stats.UniqueFieldPaths != 2 {
		t.Errorf("Expected 2 unique field paths, got %d", stats.UniqueFieldPaths)
	}
}
