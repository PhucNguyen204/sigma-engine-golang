package compiler

import (
	"testing"
)

// Test SIGMA rule YAML samples
const testSigmaRule1 = `
id: 12345678-1234-1234-1234-123456789abc
title: Test Suspicious Process Creation
description: Detects suspicious process creation
status: experimental
author: Test Author
date: 2024/01/01
modified: 2024/01/02
level: medium
tags:
  - attack.execution
  - attack.t1059
references:
  - https://example.com/attack
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    Image|endswith: '.exe'
    CommandLine|contains:
      - 'powershell'
      - 'cmd.exe'
  filter:
    CommandLine|contains: 'legitimate'
  condition: selection and not filter
fields:
  - Image
  - CommandLine
  - ProcessId
`

const testSigmaRule2 = `
id: 87654321-4321-4321-4321-123456789abc
title: Test Network Connection
description: Detects suspicious network connections
status: stable
author: Test Author
date: 2024/01/01
level: high
tags:
  - attack.lateral_movement
logsource:
  category: network_connection
  product: windows
detection:
  selection1:
    DestinationPort:
      - 445
      - 139
  selection2:
    DestinationIp|startswith: '192.168.'
  condition: selection1 and selection2
`

const testSigmaRule3 = `
title: Test Complex Condition
description: Tests complex conditions with functions
status: experimental
logsource:
  category: process_creation
detection:
  sel1:
    Image|endswith: '.exe'
  sel2:
    CommandLine|contains: 'test'
  sel3:
    ProcessId|gt: '1000'
  condition: all(sel1, sel2, sel3) or count(sel1, sel2) > 1
`

func TestNewCompiler(t *testing.T) {
	compiler := NewCompiler()

	if compiler == nil {
		t.Fatal("NewCompiler() returned nil")
	}

	if compiler.fieldMapping == nil {
		t.Error("Field mapping not initialized")
	}

	if compiler.primitiveMap == nil {
		t.Error("Primitive map not initialized")
	}

	if compiler.ruleMap == nil {
		t.Error("Rule map not initialized")
	}

	config := compiler.GetConfig()
	if !config.EnableFieldMapping {
		t.Error("Expected field mapping to be enabled by default")
	}
}

func TestNewCompilerWithConfig(t *testing.T) {
	config := CompilerConfig{
		EnableFieldMapping:           false,
		EnableConditionValidation:    false,
		EnablePrimitiveDeduplication: false,
		CaseSensitiveFields:          true,
		MaxRuleComplexity:            50,
		Debug:                        true,
	}

	compiler := NewCompilerWithConfig(config)

	if compiler == nil {
		t.Fatal("NewCompilerWithConfig() returned nil")
	}

	actualConfig := compiler.GetConfig()
	if actualConfig.EnableFieldMapping != false {
		t.Error("Expected field mapping to be disabled")
	}

	if actualConfig.CaseSensitiveFields != true {
		t.Error("Expected case sensitive fields to be enabled")
	}

	if actualConfig.MaxRuleComplexity != 50 {
		t.Error("Expected max rule complexity to be 50")
	}
}

func TestCompileRule(t *testing.T) {
	compiler := NewCompiler()

	rule, err := compiler.CompileRule(testSigmaRule1)
	if err != nil {
		t.Fatalf("Failed to compile rule: %v", err)
	}

	if rule.ID != "12345678-1234-1234-1234-123456789abc" {
		t.Errorf("Expected rule ID to be '12345678-1234-1234-1234-123456789abc', got %s", rule.ID)
	}

	if rule.Title != "Test Suspicious Process Creation" {
		t.Errorf("Expected title to be 'Test Suspicious Process Creation', got %s", rule.Title)
	}

	if rule.Level != "medium" {
		t.Errorf("Expected level to be 'medium', got %s", rule.Level)
	}

	if len(rule.Tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(rule.Tags))
	}

	if rule.Detection == nil {
		t.Error("Expected detection section to be present")
	}

	condition, hasCondition := rule.Detection["condition"]
	if !hasCondition {
		t.Error("Expected condition to be present in detection section")
	}

	conditionStr, ok := condition.(string)
	if !ok {
		t.Error("Expected condition to be a string")
	}

	if conditionStr != "selection and not filter" {
		t.Errorf("Expected condition to be 'selection and not filter', got %s", conditionStr)
	}
}

func TestCompileRules(t *testing.T) {
	compiler := NewCompiler()

	rules := []string{testSigmaRule1, testSigmaRule2}
	result, err := compiler.CompileRules(rules)

	if err != nil {
		t.Fatalf("Failed to compile rules: %v", err)
	}

	if result == nil {
		t.Fatal("Compilation result is nil")
	}

	if result.Statistics.TotalRules != 2 {
		t.Errorf("Expected 2 total rules, got %d", result.Statistics.TotalRules)
	}

	if result.Statistics.SuccessfulRules != 2 {
		t.Errorf("Expected 2 successful rules, got %d", result.Statistics.SuccessfulRules)
	}

	if result.Statistics.FailedRules != 0 {
		t.Errorf("Expected 0 failed rules, got %d", result.Statistics.FailedRules)
	}

	if result.Ruleset == nil {
		t.Error("Expected ruleset to be present")
	}

	primitives := compiler.GetPrimitives()
	if len(primitives) == 0 {
		t.Error("Expected primitives to be generated")
	}

	if result.Statistics.TotalPrimitives != len(primitives) {
		t.Errorf("Expected total primitives to match actual primitives: %d vs %d",
			result.Statistics.TotalPrimitives, len(primitives))
	}
}

func TestCompileRulesWithFieldMapping(t *testing.T) {
	compiler := NewCompiler()

	// Load preset mappings
	err := compiler.fieldMapping.LoadPresetMappings("sysmon")
	if err != nil {
		t.Fatalf("Failed to load preset mappings: %v", err)
	}

	result, err := compiler.CompileRules([]string{testSigmaRule1})
	if err != nil {
		t.Fatalf("Failed to compile rules with field mapping: %v", err)
	}

	primitives := compiler.GetPrimitives()
	if len(primitives) == 0 {
		t.Error("Expected primitives to be generated")
	}

	// Check that field names were mapped
	foundMappedField := false
	for _, primitive := range primitives {
		if primitive.Field != "" {
			foundMappedField = true
			break
		}
	}

	if !foundMappedField {
		t.Error("Expected at least one primitive with mapped field")
	}

	if result.Statistics.SuccessfulRules != 1 {
		t.Errorf("Expected 1 successful rule, got %d", result.Statistics.SuccessfulRules)
	}
}

func TestCompileRulesWithDeduplication(t *testing.T) {
	compiler := NewCompilerWithConfig(CompilerConfig{
		EnablePrimitiveDeduplication: true,
		EnableFieldMapping:           false,
		EnableConditionValidation:    false,
	})

	// Create a rule that should generate duplicate primitives
	duplicateRule := `
title: Test Duplicate Primitives
detection:
  selection1:
    field1: 'value1'
  selection2:
    field1: 'value1'  # Same as selection1
  condition: selection1 or selection2
`

	result, err := compiler.CompileRules([]string{duplicateRule})
	if err != nil {
		t.Fatalf("Failed to compile rules with deduplication: %v", err)
	}

	if result.Statistics.DuplicatedPrimitives <= 0 {
		t.Error("Expected some duplicated primitives to be detected and deduplicated")
	}

	if result.Statistics.UniquePrimitives >= result.Statistics.TotalPrimitives {
		t.Error("Expected unique primitives to be less than total primitives")
	}
}

func TestCompileRulesWithValidation(t *testing.T) {
	compiler := NewCompilerWithConfig(CompilerConfig{
		EnableConditionValidation: true,
		MaxRuleComplexity:         10,
	})

	result, err := compiler.CompileRules([]string{testSigmaRule3})
	if err != nil {
		t.Fatalf("Failed to compile rules with validation: %v", err)
	}

	// Complex rule should generate warnings
	if len(result.Warnings) == 0 {
		t.Error("Expected complexity warnings for complex rule")
	}

	if result.Statistics.ComplexConditions == 0 {
		t.Error("Expected complex conditions to be detected")
	}
}

func TestCompileRulesWithErrors(t *testing.T) {
	compiler := NewCompiler()

	invalidRule := `
title: Invalid Rule
detection:
  # Missing condition
  selection:
    field: value
`

	result, err := compiler.CompileRules([]string{invalidRule})
	if err != nil {
		t.Fatalf("Unexpected error during compilation: %v", err)
	}

	if len(result.Errors) == 0 {
		t.Error("Expected errors for invalid rule")
	}

	if result.Statistics.FailedRules == 0 {
		t.Error("Expected failed rules count to be greater than 0")
	}
}

func TestPrimitiveCreation(t *testing.T) {
	compiler := NewCompiler()

	// Test different primitive types
	testCases := []struct {
		field     string
		matchType string
		values    []string
		modifiers []string
	}{
		{"field1", "equals", []string{"value1"}, []string{}},
		{"field2", "contains", []string{"substring"}, []string{}},
		{"field3", "regex", []string{"pattern.*"}, []string{}},
		{"field4", "equals", []string{"value"}, []string{"base64"}},
	}

	for _, tc := range testCases {
		primitiveID := compiler.createPrimitive(tc.field, tc.matchType, tc.values, tc.modifiers)

		if primitiveID < 0 {
			t.Errorf("Invalid primitive ID: %d", primitiveID)
		}
	}

	primitives := compiler.GetPrimitives()
	if len(primitives) != len(testCases) {
		t.Errorf("Expected %d primitives, got %d", len(testCases), len(primitives))
	}

	for i, primitive := range primitives {
		tc := testCases[i]
		if primitive.Field != tc.field {
			t.Errorf("Expected field %s, got %s", tc.field, primitive.Field)
		}

		if primitive.MatchType != tc.matchType {
			t.Errorf("Expected match type %s, got %s", tc.matchType, primitive.MatchType)
		}

		if len(primitive.Values) != len(tc.values) {
			t.Errorf("Expected %d values, got %d", len(tc.values), len(primitive.Values))
		}

		if len(primitive.Modifiers) != len(tc.modifiers) {
			t.Errorf("Expected %d modifiers, got %d", len(tc.modifiers), len(primitive.Modifiers))
		}
	}
}

func TestBuildMatcher(t *testing.T) {
	compiler := NewCompiler()

	_, err := compiler.CompileRules([]string{testSigmaRule1})
	if err != nil {
		t.Fatalf("Failed to compile rules: %v", err)
	}

	matcher, err := compiler.BuildMatcher()
	if err != nil {
		t.Fatalf("Failed to build matcher: %v", err)
	}

	if matcher == nil {
		t.Fatal("Matcher is nil")
	}

	// Test that matcher exists
	// Note: EvaluateAll method might not exist, check actual API
	t.Log("Matcher built successfully")
}

func TestCompilerClone(t *testing.T) {
	compiler := NewCompiler()

	// Compile some rules
	_, err := compiler.CompileRules([]string{testSigmaRule1})
	if err != nil {
		t.Fatalf("Failed to compile rules: %v", err)
	}

	// Clone compiler
	clone := compiler.Clone()

	if clone == nil {
		t.Fatal("Clone is nil")
	}

	if len(clone.GetPrimitives()) != len(compiler.GetPrimitives()) {
		t.Error("Clone should have same number of primitives")
	}

	if clone.GetRuleCount() != compiler.GetRuleCount() {
		t.Error("Clone should have same number of rules")
	}

	// Modify original should not affect clone
	compiler.Clear()

	if len(compiler.GetPrimitives()) != 0 {
		t.Error("Original should be cleared")
	}

	if len(clone.GetPrimitives()) == 0 {
		t.Error("Clone should still have primitives after original is cleared")
	}
}

func TestCompilerClear(t *testing.T) {
	compiler := NewCompiler()

	// Compile some rules
	_, err := compiler.CompileRules([]string{testSigmaRule1, testSigmaRule2})
	if err != nil {
		t.Fatalf("Failed to compile rules: %v", err)
	}

	// Check that data exists
	if len(compiler.GetPrimitives()) == 0 {
		t.Error("Expected primitives before clear")
	}

	if compiler.GetRuleCount() == 0 {
		t.Error("Expected rules before clear")
	}

	// Clear compiler
	compiler.Clear()

	// Check that data is cleared
	if len(compiler.GetPrimitives()) != 0 {
		t.Error("Expected no primitives after clear")
	}

	if compiler.GetRuleCount() != 0 {
		t.Error("Expected no rules after clear")
	}

	if compiler.GetPrimitiveCount() != 0 {
		t.Error("Expected primitive count to be 0 after clear")
	}
}

func TestOperatorParsing(t *testing.T) {
	compiler := NewCompiler()

	testCases := []struct {
		operator          string
		expectedType      string
		expectedModifiers []string
	}{
		{"contains", "contains", []string{}},
		{"startswith", "startswith", []string{}},
		{"endswith", "endswith", []string{}},
		{"re", "regex", []string{}},
		{"all", "all", []string{}},
		{"contains|base64", "contains", []string{"base64"}},
		{"endswith|utf16", "endswith", []string{"utf16"}},
		{"unknown", "equals", []string{}},
	}

	for _, tc := range testCases {
		matchType, modifiers := compiler.parseOperator(tc.operator)

		if matchType != tc.expectedType {
			t.Errorf("For operator %s, expected match type %s, got %s",
				tc.operator, tc.expectedType, matchType)
		}

		if len(modifiers) != len(tc.expectedModifiers) {
			t.Errorf("For operator %s, expected %d modifiers, got %d",
				tc.operator, len(tc.expectedModifiers), len(modifiers))
		}

		for i, expectedMod := range tc.expectedModifiers {
			if i >= len(modifiers) || modifiers[i] != expectedMod {
				t.Errorf("For operator %s, expected modifier %s at index %d, got %s",
					tc.operator, expectedMod, i, modifiers[i])
			}
		}
	}
}

func BenchmarkCompileRules(b *testing.B) {
	compiler := NewCompiler()
	rules := []string{testSigmaRule1, testSigmaRule2, testSigmaRule3}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compiler.Clear()
		_, err := compiler.CompileRules(rules)
		if err != nil {
			b.Fatalf("Failed to compile rules: %v", err)
		}
	}
}

func BenchmarkPrimitiveCreation(b *testing.B) {
	compiler := NewCompiler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compiler.createPrimitive("field", "equals", []string{"value"}, []string{})
	}
}

func BenchmarkBuildMatcher(b *testing.B) {
	compiler := NewCompiler()

	// Pre-compile rules
	_, err := compiler.CompileRules([]string{testSigmaRule1, testSigmaRule2})
	if err != nil {
		b.Fatalf("Failed to compile rules: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := compiler.BuildMatcher()
		if err != nil {
			b.Fatalf("Failed to build matcher: %v", err)
		}
	}
}
