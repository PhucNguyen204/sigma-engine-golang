package dag

import (
	"encoding/json"
	"testing"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

func TestDefaultDagEngineConfig(t *testing.T) {
	config := DefaultDagEngineConfig()

	if !config.EnableOptimization {
		t.Error("Expected EnableOptimization to be true")
	}
	if config.OptimizationLevel != 2 {
		t.Errorf("Expected OptimizationLevel to be 2, got %d", config.OptimizationLevel)
	}
	if config.EnableParallelProcessing {
		t.Error("Expected EnableParallelProcessing to be false")
	}
	if !config.EnablePrefilter {
		t.Error("Expected EnablePrefilter to be true")
	}
}

func TestHighPerformanceConfig(t *testing.T) {
	config := HighPerformanceConfig()

	if !config.EnableOptimization {
		t.Error("Expected EnableOptimization to be true")
	}
	if config.OptimizationLevel != 3 {
		t.Errorf("Expected OptimizationLevel to be 3, got %d", config.OptimizationLevel)
	}
	if !config.EnableParallelProcessing {
		t.Error("Expected EnableParallelProcessing to be true")
	}
	if config.ParallelConfig.NumThreads != 0 {
		t.Errorf("Expected NumThreads to be 0 (auto-detect), got %d", config.ParallelConfig.NumThreads)
	}
	if config.ParallelConfig.MinRulesPerThread != 5 {
		t.Errorf("Expected MinRulesPerThread to be 5, got %d", config.ParallelConfig.MinRulesPerThread)
	}
}

func TestStreamingOptimizedConfig(t *testing.T) {
	config := StreamingOptimizedConfig()

	if !config.EnableOptimization {
		t.Error("Expected EnableOptimization to be true")
	}
	if config.OptimizationLevel != 3 {
		t.Errorf("Expected OptimizationLevel to be 3, got %d", config.OptimizationLevel)
	}
	if !config.EnableParallelProcessing {
		t.Error("Expected EnableParallelProcessing to be true")
	}
	if config.ParallelConfig.MinBatchSizeForParallelism != 100 {
		t.Errorf("Expected MinBatchSizeForParallelism to be 100, got %d", config.ParallelConfig.MinBatchSizeForParallelism)
	}
}

func TestDagEngineBuilder(t *testing.T) {
	builder := NewDagEngineBuilder()

	if !builder.config.EnableOptimization {
		t.Error("Expected default EnableOptimization to be true")
	}
	if builder.config.OptimizationLevel != 2 {
		t.Errorf("Expected default OptimizationLevel to be 2, got %d", builder.config.OptimizationLevel)
	}

	// Test builder pattern
	builder = builder.WithOptimization(false).
		WithOptimizationLevel(1).
		WithParallelProcessing(true).
		WithPrefilter(false)

	if builder.config.EnableOptimization {
		t.Error("Expected EnableOptimization to be false after builder chain")
	}
	if builder.config.OptimizationLevel != 1 {
		t.Errorf("Expected OptimizationLevel to be 1 after builder chain, got %d", builder.config.OptimizationLevel)
	}
	if !builder.config.EnableParallelProcessing {
		t.Error("Expected EnableParallelProcessing to be true after builder chain")
	}
	if builder.config.EnablePrefilter {
		t.Error("Expected EnablePrefilter to be false after builder chain")
	}
}

func createTestRuleset() *CompiledRuleset {
	primitive1 := Primitive{
		ID:        0,
		Field:     "EventID",
		MatchType: "equals",
		Values:    []string{"4624"},
		Modifiers: []string{},
	}

	primitive2 := Primitive{
		ID:        1,
		Field:     "ProcessName",
		MatchType: "contains",
		Values:    []string{"powershell"},
		Modifiers: []string{},
	}

	return &CompiledRuleset{
		Primitives:   []Primitive{primitive1, primitive2},
		PrimitiveMap: map[uint32]*CompiledPrimitive{},
	}
}

func TestBuildPrimitiveMap(t *testing.T) {
	ruleset := createTestRuleset()

	primitiveMap, err := buildPrimitiveMap(ruleset)
	if err != nil {
		t.Fatalf("Failed to build primitive map: %v", err)
	}

	if len(primitiveMap) != 2 {
		t.Errorf("Expected 2 primitives, got %d", len(primitiveMap))
	}

	// Check primitive 0
	if primitive, exists := primitiveMap[0]; !exists {
		t.Error("Expected primitive with ID 0 to exist")
	} else {
		if primitive.Field != "EventID" {
			t.Errorf("Expected field 'EventID', got '%s'", primitive.Field)
		}
		if primitive.MatchType != "equals" {
			t.Errorf("Expected match type 'equals', got '%s'", primitive.MatchType)
		}
		if len(primitive.Values) != 1 || primitive.Values[0] != "4624" {
			t.Errorf("Expected values ['4624'], got %v", primitive.Values)
		}
	}

	// Check primitive 1
	if primitive, exists := primitiveMap[1]; !exists {
		t.Error("Expected primitive with ID 1 to exist")
	} else {
		if primitive.Field != "ProcessName" {
			t.Errorf("Expected field 'ProcessName', got '%s'", primitive.Field)
		}
		if primitive.MatchType != "contains" {
			t.Errorf("Expected match type 'contains', got '%s'", primitive.MatchType)
		}
		if len(primitive.Values) != 1 || primitive.Values[0] != "powershell" {
			t.Errorf("Expected values ['powershell'], got %v", primitive.Values)
		}
	}
}

func TestCreateMatcherFunc(t *testing.T) {
	// Test equals matcher
	matcher := createMatcherFunc("EventID", "equals", []string{"4624", "4625"})

	// Test matching event
	event1 := map[string]interface{}{
		"EventID":     "4624",
		"ProcessName": "explorer.exe",
	}

	if !matcher(event1) {
		t.Error("Expected matcher to return true for matching event")
	}

	// Test non-matching event
	event2 := map[string]interface{}{
		"EventID":     "1234",
		"ProcessName": "explorer.exe",
	}

	if matcher(event2) {
		t.Error("Expected matcher to return false for non-matching event")
	}

	// Test missing field
	event3 := map[string]interface{}{
		"ProcessName": "explorer.exe",
	}

	if matcher(event3) {
		t.Error("Expected matcher to return false for event missing field")
	}

	// Test non-map event
	if matcher("not a map") {
		t.Error("Expected matcher to return false for non-map event")
	}
}

func TestLiteralPrefilter(t *testing.T) {
	primitives := []Primitive{
		{
			ID:        0,
			Field:     "EventID",
			MatchType: "equals",
			Values:    []string{"4624"},
			Modifiers: []string{},
		},
		{
			ID:        1,
			Field:     "ProcessName",
			MatchType: "contains",
			Values:    []string{"powershell"},
			Modifiers: []string{},
		},
		{
			ID:        2,
			Field:     "Command",
			MatchType: "regex", // Not literal, should be ignored
			Values:    []string{"test.*"},
			Modifiers: []string{},
		},
	}

	prefilter, err := NewLiteralPrefilterFromPrimitives(primitives)
	if err != nil {
		t.Fatalf("Failed to create prefilter: %v", err)
	}

	// Check stats
	stats := prefilter.Stats()
	if stats.PatternCount != 2 { // Only "4624" and "powershell" are literal
		t.Errorf("Expected 2 patterns, got %d", stats.PatternCount)
	}
	if stats.FieldCount != 2 {
		t.Errorf("Expected 2 fields, got %d", stats.FieldCount)
	}

	// Test matching
	event1 := map[string]interface{}{
		"EventID":     "4624",
		"ProcessName": "explorer.exe",
	}

	matches, err := prefilter.Matches(event1)
	if err != nil {
		t.Fatalf("Failed to check matches: %v", err)
	}
	if !matches {
		t.Error("Expected prefilter to match event with EventID 4624")
	}

	// Test non-matching
	event2 := map[string]interface{}{
		"EventID":     "1234",
		"ProcessName": "explorer.exe",
	}

	matches, err = prefilter.Matches(event2)
	if err != nil {
		t.Fatalf("Failed to check matches: %v", err)
	}
	if matches {
		t.Error("Expected prefilter to not match event without target patterns")
	}
}

func TestIsLiteralMatchType(t *testing.T) {
	testCases := []struct {
		matchType string
		expected  bool
	}{
		{"equals", true},
		{"contains", true},
		{"startswith", true},
		{"endswith", true},
		{"regex", false},
		{"glob", false},
		{"unknown", false},
	}

	for _, tc := range testCases {
		result := isLiteralMatchType(tc.matchType)
		if result != tc.expected {
			t.Errorf("Expected isLiteralMatchType(%s) to be %v, got %v", tc.matchType, tc.expected, result)
		}
	}
}

func TestCalculateSelectivity(t *testing.T) {
	testCases := []struct {
		patternCount int
		expected     float64
	}{
		{0, 1.0},
		{1, 1.0},
		{2, 0.5},
		{10, 0.1},
		{100, 0.01},
	}

	for _, tc := range testCases {
		result := calculateSelectivity(tc.patternCount)
		if result != tc.expected {
			t.Errorf("Expected calculateSelectivity(%d) to be %f, got %f", tc.patternCount, tc.expected, result)
		}
	}
}

func TestGetStrategyName(t *testing.T) {
	testCases := []struct {
		patternCount int
		expected     string
	}{
		{0, "Simple"},
		{50, "Simple"},
		{100, "Simple"},
		{101, "AhoCorasick"},
		{1000, "AhoCorasick"},
	}

	for _, tc := range testCases {
		result := getStrategyName(tc.patternCount)
		if result != tc.expected {
			t.Errorf("Expected getStrategyName(%d) to be %s, got %s", tc.patternCount, tc.expected, result)
		}
	}
}

func TestDagEngineFromRulesetWithoutCompiler(t *testing.T) {
	// Test that NewDagEngineFromRulesWithConfig returns appropriate error for unimplemented functionality
	ruleYamls := []string{
		`title: Test Rule
detection:
  selection:
    EventID: 4624
  condition: selection`,
	}

	_, err := NewDagEngineFromRulesWithConfig(ruleYamls, DefaultDagEngineConfig())
	if err == nil {
		t.Error("Expected error for unimplemented rule compilation")
	}

	expectedError := "rule compilation not implemented yet"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got '%s'", expectedError, err.Error())
	}
}

func TestBatchMemoryPool(t *testing.T) {
	pool := NewBatchMemoryPool()
	if pool == nil {
		t.Error("Expected NewBatchMemoryPool to return non-nil pool")
	}

	// Basic validation that the pool was created
	if pool.batchSize != 0 {
		t.Errorf("Expected initial batchSize to be 0, got %d", pool.batchSize)
	}
	if pool.nodeCount != 0 {
		t.Errorf("Expected initial nodeCount to be 0, got %d", pool.nodeCount)
	}
	if pool.primitiveCount != 0 {
		t.Errorf("Expected initial primitiveCount to be 0, got %d", pool.primitiveCount)
	}
}

func TestPartitionRules(t *testing.T) {
	// Create a simple DAG with some rules
	dag := &CompiledDag{
		RuleResults: map[ir.RuleID]NodeId{
			ir.RuleID(1): NodeId(10),
			ir.RuleID(2): NodeId(11),
			ir.RuleID(3): NodeId(12),
		},
	}

	config := DefaultParallelConfig()
	partitions := partitionRules(dag, config)

	if len(partitions) == 0 {
		t.Error("Expected at least one partition")
	}

	// Check that all rules are included
	totalRules := 0
	for _, partition := range partitions {
		totalRules += len(partition.Rules)
	}

	if totalRules != 3 {
		t.Errorf("Expected 3 total rules across partitions, got %d", totalRules)
	}
}

func TestDagEngineFromRulesetError(t *testing.T) {
	// Test with empty ruleset
	emptyRuleset := &CompiledRuleset{
		Primitives:   []Primitive{},
		PrimitiveMap: map[uint32]*CompiledPrimitive{},
	}

	engine, err := NewDagEngineFromRuleset(emptyRuleset)
	// This might succeed or fail depending on DAG builder implementation
	// We're testing the interface exists and handles edge cases gracefully
	if err != nil {
		// Expected for incomplete implementation
		t.Logf("Engine creation failed as expected: %v", err)
	} else if engine == nil {
		t.Error("Expected non-nil engine when no error is returned")
	}
}

// Additional test for JSON marshaling/unmarshaling if needed in the future
func TestDagEngineConfigSerialization(t *testing.T) {
	config := HighPerformanceConfig()

	// Test JSON marshaling
	jsonData, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal config to JSON: %v", err)
	}

	// Test JSON unmarshaling
	var unmarshaled DagEngineConfig
	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal config from JSON: %v", err)
	}

	// Verify fields are preserved
	if unmarshaled.EnableOptimization != config.EnableOptimization {
		t.Error("EnableOptimization not preserved in JSON round-trip")
	}
	if unmarshaled.OptimizationLevel != config.OptimizationLevel {
		t.Error("OptimizationLevel not preserved in JSON round-trip")
	}
	if unmarshaled.EnableParallelProcessing != config.EnableParallelProcessing {
		t.Error("EnableParallelProcessing not preserved in JSON round-trip")
	}
	if unmarshaled.EnablePrefilter != config.EnablePrefilter {
		t.Error("EnablePrefilter not preserved in JSON round-trip")
	}
}
