// Package dag provides DAG-based SIGMA rule evaluation engine
package dag

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// DagEngineConfig controls DAG engine behavior and optimization
type DagEngineConfig struct {
	// Enable optimization during DAG construction
	EnableOptimization bool

	// Optimization level (0-3)
	// 0: No optimization (fastest compilation)
	// 1: Basic optimizations (DCE, constant folding)
	// 2: Standard optimizations (CSE, reordering) - Default
	// 3: Aggressive optimizations (all techniques)
	OptimizationLevel uint8

	// Enable parallel processing for rule evaluation
	EnableParallelProcessing bool

	// Parallel processing configuration
	ParallelConfig ParallelConfig

	// Enable literal prefiltering for fast event elimination
	EnablePrefilter bool
}

// ParallelConfig contains parallel processing settings
type ParallelConfig struct {
	// Number of threads to use (0 = auto-detect)
	NumThreads int

	// Minimum number of rules per thread
	MinRulesPerThread int

	// Enable parallel event processing for batches
	EnableEventParallelism bool

	// Minimum batch size to enable parallelism
	MinBatchSizeForParallelism int
}

// DefaultDagEngineConfig returns a default configuration
func DefaultDagEngineConfig() DagEngineConfig {
	return DagEngineConfig{
		EnableOptimization:       true,
		OptimizationLevel:        2,
		EnableParallelProcessing: false,
		ParallelConfig:           DefaultParallelConfig(),
		EnablePrefilter:          true,
	}
}

// DefaultParallelConfig returns default parallel configuration
func DefaultParallelConfig() ParallelConfig {
	return ParallelConfig{
		NumThreads:                 4, // Default to 4 threads
		MinRulesPerThread:          10,
		EnableEventParallelism:     true,
		MinBatchSizeForParallelism: 100,
	}
}

// HighPerformanceConfig returns a configuration optimized for high-performance
func HighPerformanceConfig() DagEngineConfig {
	return DagEngineConfig{
		EnableOptimization:       true,
		OptimizationLevel:        3,
		EnableParallelProcessing: true,
		ParallelConfig: ParallelConfig{
			NumThreads:                 0, // Auto-detect
			MinRulesPerThread:          5,
			EnableEventParallelism:     true,
			MinBatchSizeForParallelism: 50,
		},
		EnablePrefilter: true,
	}
}

// StreamingOptimizedConfig returns a configuration optimized for streaming workloads
func StreamingOptimizedConfig() DagEngineConfig {
	return DagEngineConfig{
		EnableOptimization:       true,
		OptimizationLevel:        3,
		EnableParallelProcessing: true,
		ParallelConfig: ParallelConfig{
			NumThreads:                 0, // Auto-detect
			MinRulesPerThread:          10,
			EnableEventParallelism:     true,
			MinBatchSizeForParallelism: 100,
		},
		EnablePrefilter: true,
	}
}

// DagEngine provides high-performance rule evaluation using DAG-based approach
type DagEngine struct {
	// Compiled DAG structure
	dag *CompiledDag

	// Compiled primitives for field matching
	primitives map[uint32]*CompiledPrimitive

	// Engine configuration
	config DagEngineConfig

	// Cached evaluators for reuse
	evaluator         *DagEvaluator
	batchEvaluator    *BatchDagEvaluator
	parallelEvaluator *ParallelDagEvaluator

	// Optional prefilter for literal pattern matching
	prefilter *LiteralPrefilter

	// Mutex for thread safety
	mu sync.Mutex
}

// CompiledPrimitive represents a compiled matcher for primitives
type CompiledPrimitive struct {
	ID          uint32
	Field       string
	MatchType   string
	Values      []string
	Modifiers   []string
	MatcherFunc func(interface{}) bool
}

// LiteralPrefilter provides fast literal pattern matching
type LiteralPrefilter struct {
	patterns   map[string]bool
	fieldCount int
	stats      *PrefilterStats
}

// PrefilterStats contains prefilter performance statistics
type PrefilterStats struct {
	PatternCount         int
	FieldCount           int
	EstimatedSelectivity float64
	StrategyName         string
}

// BatchDagEvaluator provides high-performance batch evaluation
type BatchDagEvaluator struct {
	dag                       *CompiledDag
	primitives                map[uint32]*CompiledPrimitive
	memoryPool                *BatchMemoryPool
	totalNodesEvaluated       int
	totalPrimitiveEvaluations int
}

// BatchMemoryPool manages memory allocation for batch processing
type BatchMemoryPool struct {
	nodeResults      [][]bool
	primitiveResults [][]bool
	batchSize        int
	nodeCount        int
	primitiveCount   int
}

// ParallelDagEvaluator provides parallel rule processing
type ParallelDagEvaluator struct {
	dag                       *CompiledDag
	primitives                map[uint32]*CompiledPrimitive
	config                    ParallelConfig
	rulePartitions            []RulePartition
	totalNodesEvaluated       int
	totalPrimitiveEvaluations int
}

// RulePartition represents a partition of rules for parallel processing
type RulePartition struct {
	Rules      []uint32
	Complexity float32
	ThreadID   int
}

// DagEngineBuilder provides a builder pattern for creating DagEngine
type DagEngineBuilder struct {
	compiler Compiler
	config   DagEngineConfig
}

// Compiler interface for rule compilation
type Compiler interface {
	CompileRules(rules []string) (*CompiledRuleset, error)
}

// CompiledRuleset represents a compiled set of rules
type CompiledRuleset struct {
	Primitives   []Primitive
	PrimitiveMap map[uint32]*CompiledPrimitive
}

// Primitive represents a basic matching primitive
type Primitive struct {
	ID        uint32
	Field     string
	MatchType string
	Values    []string
	Modifiers []string
}

// NewDagEngineBuilder creates a new DAG engine builder
func NewDagEngineBuilder() *DagEngineBuilder {
	return &DagEngineBuilder{
		config: DefaultDagEngineConfig(),
	}
}

// WithConfig sets the engine configuration
func (b *DagEngineBuilder) WithConfig(config DagEngineConfig) *DagEngineBuilder {
	b.config = config
	return b
}

// WithCompiler sets a custom compiler
func (b *DagEngineBuilder) WithCompiler(compiler Compiler) *DagEngineBuilder {
	b.compiler = compiler
	return b
}

// WithOptimization enables or disables optimization
func (b *DagEngineBuilder) WithOptimization(enable bool) *DagEngineBuilder {
	b.config.EnableOptimization = enable
	return b
}

// WithOptimizationLevel sets the optimization level
func (b *DagEngineBuilder) WithOptimizationLevel(level uint8) *DagEngineBuilder {
	b.config.OptimizationLevel = level
	return b
}

// WithParallelProcessing enables or disables parallel processing
func (b *DagEngineBuilder) WithParallelProcessing(enable bool) *DagEngineBuilder {
	b.config.EnableParallelProcessing = enable
	return b
}

// WithPrefilter enables or disables prefiltering
func (b *DagEngineBuilder) WithPrefilter(enable bool) *DagEngineBuilder {
	b.config.EnablePrefilter = enable
	return b
}

// Build creates the engine from SIGMA rule YAML strings
func (b *DagEngineBuilder) Build(ruleYamls []string) (*DagEngine, error) {
	if b.compiler != nil {
		return NewDagEngineFromRulesWithCompiler(ruleYamls, b.compiler, b.config)
	}
	return NewDagEngineFromRulesWithConfig(ruleYamls, b.config)
}

// NewDagEngineFromRuleset creates a DAG engine from a compiled ruleset
func NewDagEngineFromRuleset(ruleset *CompiledRuleset) (*DagEngine, error) {
	return NewDagEngineFromRulesetWithConfig(ruleset, DefaultDagEngineConfig())
}

// NewDagEngineFromRulesetWithConfig creates a DAG engine from a compiled ruleset with config
func NewDagEngineFromRulesetWithConfig(ruleset *CompiledRuleset, config DagEngineConfig) (*DagEngine, error) {
	// Build DAG from ruleset (simplified implementation)
	// In a real implementation, this would properly construct the DAG from the ruleset
	dag := &CompiledDag{
		Nodes:            make([]DagNode, 0),
		ExecutionOrder:   make([]NodeId, 0),
		PrimitiveMap:     make(map[ir.PrimitiveID]NodeId),
		RuleResults:      make(map[ir.RuleID]NodeId),
		ResultBufferSize: 0,
	}

	// Apply optimization if enabled
	if config.EnableOptimization {
		optimizer := NewDagOptimizer()
		optimizedDag, err := optimizer.Optimize(dag)
		if err == nil && optimizedDag != nil {
			dag = optimizedDag
		}
	}

	// Build primitive map
	primitives, err := buildPrimitiveMap(ruleset)
	if err != nil {
		return nil, fmt.Errorf("failed to build primitive map: %w", err)
	}

	// Create prefilter if enabled
	var prefilter *LiteralPrefilter
	if config.EnablePrefilter {
		prefilter, err = NewLiteralPrefilterFromPrimitives(ruleset.Primitives)
		if err != nil {
			return nil, fmt.Errorf("failed to create prefilter: %w", err)
		}
	}

	return &DagEngine{
		dag:        dag,
		primitives: primitives,
		config:     config,
		prefilter:  prefilter,
	}, nil
}

// NewDagEngineFromRulesWithConfig creates a DAG engine from rule YAML strings with config
func NewDagEngineFromRulesWithConfig(ruleYamls []string, config DagEngineConfig) (*DagEngine, error) {
	// For now, return a placeholder implementation
	// In a real implementation, this would compile the YAML rules
	return nil, fmt.Errorf("rule compilation not implemented yet")
}

// NewDagEngineFromRulesWithCompiler creates a DAG engine from rules with a custom compiler
func NewDagEngineFromRulesWithCompiler(ruleYamls []string, compiler Compiler, config DagEngineConfig) (*DagEngine, error) {
	// Compile rules using the provided compiler
	ruleset, err := compiler.CompileRules(ruleYamls)
	if err != nil {
		return nil, fmt.Errorf("failed to compile rules: %w", err)
	}

	return NewDagEngineFromRulesetWithConfig(ruleset, config)
}

// buildPrimitiveMap builds the primitive matcher map from compiled ruleset
func buildPrimitiveMap(ruleset *CompiledRuleset) (map[uint32]*CompiledPrimitive, error) {
	primitives := make(map[uint32]*CompiledPrimitive)

	for _, primitive := range ruleset.Primitives {
		// Create a basic matcher function (simplified)
		matcherFunc := createMatcherFunc(primitive.Field, primitive.MatchType, primitive.Values)

		primitives[primitive.ID] = &CompiledPrimitive{
			ID:          primitive.ID,
			Field:       primitive.Field,
			MatchType:   primitive.MatchType,
			Values:      primitive.Values,
			Modifiers:   primitive.Modifiers,
			MatcherFunc: matcherFunc,
		}
	}

	return primitives, nil
}

// createMatcherFunc creates a basic matcher function for a primitive
func createMatcherFunc(field, matchType string, values []string) func(interface{}) bool {
	return func(event interface{}) bool {
		// Simplified matcher implementation
		// In a real implementation, this would handle various match types
		eventMap, ok := event.(map[string]interface{})
		if !ok {
			return false
		}

		fieldValue, exists := eventMap[field]
		if !exists {
			return false
		}

		fieldStr := fmt.Sprintf("%v", fieldValue)

		// Simple equality check for demonstration
		for _, value := range values {
			if fieldStr == value {
				return true
			}
		}

		return false
	}
}

// Evaluate evaluates the DAG against an event and returns matches
func (e *DagEngine) Evaluate(event interface{}) (*DagEvaluationResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	startTime := time.Now()

	// Get or create evaluator
	if e.evaluator == nil {
		e.evaluator = NewDagEvaluatorWithPrimitivesAndPrefilter(e.dag)
	} else {
		e.evaluator.reset()
	}

	// Convert event to map[string]interface{}
	eventMap, ok := event.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("event must be a map[string]interface{}")
	}

	// Perform evaluation
	result, err := e.evaluator.Evaluate(eventMap)
	if err != nil {
		return nil, err
	}

	// Add timing information
	_ = time.Since(startTime)

	return result, nil
}

// EvaluateRaw evaluates the DAG against a raw JSON string
func (e *DagEngine) EvaluateRaw(jsonStr string) (*DagEvaluationResult, error) {
	var event map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &event); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return e.Evaluate(event)
}

// EvaluateParallel evaluates the DAG using parallel processing
func (e *DagEngine) EvaluateParallel(event interface{}) (*DagEvaluationResult, error) {
	if !e.config.EnableParallelProcessing {
		return e.Evaluate(event)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get or create parallel evaluator
	if e.parallelEvaluator == nil {
		e.parallelEvaluator = NewParallelDagEvaluator(e.dag, e.primitives, e.config.ParallelConfig)
	} else {
		e.parallelEvaluator.Reset()
	}

	// Perform parallel evaluation
	return e.parallelEvaluator.Evaluate(event)
}

// EvaluateBatch evaluates multiple events using batch processing
func (e *DagEngine) EvaluateBatch(events []interface{}) ([]*DagEvaluationResult, error) {
	if len(events) == 0 {
		return []*DagEvaluationResult{}, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get or create batch evaluator
	if e.batchEvaluator == nil {
		e.batchEvaluator = NewBatchDagEvaluator(e.dag, e.primitives)
	} else {
		e.batchEvaluator.Reset()
	}

	// Perform batch evaluation
	return e.batchEvaluator.EvaluateBatch(events)
}

// EvaluateBatchParallel evaluates multiple events using parallel batch processing
func (e *DagEngine) EvaluateBatchParallel(events []interface{}) ([]*DagEvaluationResult, error) {
	if !e.config.EnableParallelProcessing {
		return e.EvaluateBatch(events)
	}

	if len(events) == 0 {
		return []*DagEvaluationResult{}, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get or create parallel evaluator
	if e.parallelEvaluator == nil {
		e.parallelEvaluator = NewParallelDagEvaluator(e.dag, e.primitives, e.config.ParallelConfig)
	} else {
		e.parallelEvaluator.Reset()
	}

	// Perform parallel batch evaluation
	return e.parallelEvaluator.EvaluateBatch(events)
}

// EvaluateWithPrimitiveResults evaluates using pre-computed primitive results
func (e *DagEngine) EvaluateWithPrimitiveResults(primitiveResults []bool) (*DagEvaluationResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Get or create evaluator
	if e.evaluator == nil {
		e.evaluator = NewDagEvaluatorWithPrimitivesAndPrefilter(e.dag)
	} else {
		e.evaluator.reset()
	}

	// Perform evaluation with pre-computed results
	// TODO: Implement EvaluateWithPrimitiveResults method in evaluator
	return nil, fmt.Errorf("EvaluateWithPrimitiveResults not implemented yet")
}

// GetStatistics returns DAG statistics
func (e *DagEngine) GetStatistics() *DagStatistics {
	return e.dag.Statistics()
}

// RuleCount returns the number of rules in the DAG
func (e *DagEngine) RuleCount() int {
	return len(e.dag.RuleResults)
}

// NodeCount returns the number of nodes in the DAG
func (e *DagEngine) NodeCount() int {
	return len(e.dag.Nodes)
}

// PrimitiveCount returns the number of primitive nodes in the DAG
func (e *DagEngine) PrimitiveCount() int {
	count := 0
	for _, node := range e.dag.Nodes {
		if node.NodeType.Type == "Primitive" {
			count++
		}
	}
	return count
}

// ContainsRule checks if the DAG contains a specific rule
func (e *DagEngine) ContainsRule(ruleID uint32) bool {
	ruleIDConverted := ir.RuleID(ruleID)
	_, exists := e.dag.RuleResults[ruleIDConverted]
	return exists
}

// Config returns the engine configuration
func (e *DagEngine) Config() DagEngineConfig {
	return e.config
}

// PrefilterStats returns prefilter statistics if prefilter is enabled
func (e *DagEngine) PrefilterStats() *PrefilterStats {
	if e.prefilter != nil {
		return e.prefilter.Stats()
	}
	return nil
}

// Placeholder implementations for additional components

// NewLiteralPrefilterFromPrimitives creates a prefilter from primitives
func NewLiteralPrefilterFromPrimitives(primitives []Primitive) (*LiteralPrefilter, error) {
	patterns := make(map[string]bool)
	fieldCount := 0

	for _, primitive := range primitives {
		// Only handle literal patterns for prefiltering
		if isLiteralMatchType(primitive.MatchType) {
			for _, value := range primitive.Values {
				patterns[value] = true
			}
			fieldCount++
		}
	}

	stats := &PrefilterStats{
		PatternCount:         len(patterns),
		FieldCount:           fieldCount,
		EstimatedSelectivity: calculateSelectivity(len(patterns)),
		StrategyName:         getStrategyName(len(patterns)),
	}

	return &LiteralPrefilter{
		patterns:   patterns,
		fieldCount: fieldCount,
		stats:      stats,
	}, nil
}

// isLiteralMatchType checks if a match type is suitable for literal prefiltering
func isLiteralMatchType(matchType string) bool {
	switch matchType {
	case "equals", "contains", "startswith", "endswith":
		return true
	default:
		return false
	}
}

// calculateSelectivity estimates the selectivity of the prefilter
func calculateSelectivity(patternCount int) float64 {
	// Simple heuristic: more patterns = higher selectivity
	if patternCount == 0 {
		return 1.0
	}
	return 1.0 / float64(patternCount)
}

// getStrategyName returns the strategy name based on pattern count
func getStrategyName(patternCount int) string {
	if patternCount > 100 {
		return "AhoCorasick"
	}
	return "Simple"
}

// Matches checks if an event matches any prefilter patterns
func (p *LiteralPrefilter) Matches(event interface{}) (bool, error) {
	eventMap, ok := event.(map[string]interface{})
	if !ok {
		return false, nil
	}

	// Check if any field value matches our patterns
	for _, value := range eventMap {
		valueStr := fmt.Sprintf("%v", value)
		if p.patterns[valueStr] {
			return true, nil
		}
	}

	return false, nil
}

// Stats returns prefilter statistics
func (p *LiteralPrefilter) Stats() *PrefilterStats {
	return p.stats
}

// NewBatchDagEvaluator creates a new batch evaluator
func NewBatchDagEvaluator(dag *CompiledDag, primitives map[uint32]*CompiledPrimitive) *BatchDagEvaluator {
	return &BatchDagEvaluator{
		dag:        dag,
		primitives: primitives,
		memoryPool: NewBatchMemoryPool(),
	}
}

// NewBatchMemoryPool creates a new batch memory pool
func NewBatchMemoryPool() *BatchMemoryPool {
	return &BatchMemoryPool{}
}

// EvaluateBatch evaluates multiple events using batch processing
func (b *BatchDagEvaluator) EvaluateBatch(events []interface{}) ([]*DagEvaluationResult, error) {
	results := make([]*DagEvaluationResult, len(events))

	// Simplified batch evaluation - in practice this would be optimized
	for i, event := range events {
		evaluator := NewDagEvaluatorWithPrimitivesAndPrefilter(b.dag)
		eventMap, ok := event.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("event at index %d must be a map[string]interface{}", i)
		}
		result, err := evaluator.Evaluate(eventMap)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}

	return results, nil
}

// Reset resets the batch evaluator state
func (b *BatchDagEvaluator) Reset() {
	b.totalNodesEvaluated = 0
	b.totalPrimitiveEvaluations = 0
}

// NewParallelDagEvaluator creates a new parallel evaluator
func NewParallelDagEvaluator(dag *CompiledDag, primitives map[uint32]*CompiledPrimitive, config ParallelConfig) *ParallelDagEvaluator {
	return &ParallelDagEvaluator{
		dag:            dag,
		primitives:     primitives,
		config:         config,
		rulePartitions: partitionRules(dag, config),
	}
}

// partitionRules partitions rules for parallel processing
func partitionRules(dag *CompiledDag, config ParallelConfig) []RulePartition {
	// Simplified partitioning - in practice this would be more sophisticated
	partitions := []RulePartition{}
	rules := make([]uint32, 0, len(dag.RuleResults))
	for ruleID := range dag.RuleResults {
		rules = append(rules, uint32(ruleID))
	}

	if len(rules) > 0 {
		partitions = append(partitions, RulePartition{
			Rules:      rules,
			Complexity: 1.0,
			ThreadID:   0,
		})
	}

	return partitions
}

// Evaluate evaluates using parallel processing
func (p *ParallelDagEvaluator) Evaluate(event interface{}) (*DagEvaluationResult, error) {
	// Simplified parallel evaluation - fallback to sequential for now
	evaluator := NewDagEvaluatorWithPrimitivesAndPrefilter(p.dag)
	eventMap, ok := event.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("event must be a map[string]interface{}")
	}
	return evaluator.Evaluate(eventMap)
}

// EvaluateBatch evaluates multiple events using parallel batch processing
func (p *ParallelDagEvaluator) EvaluateBatch(events []interface{}) ([]*DagEvaluationResult, error) {
	results := make([]*DagEvaluationResult, len(events))

	// Simplified parallel batch evaluation
	for i, event := range events {
		result, err := p.Evaluate(event)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}

	return results, nil
}

// Reset resets the parallel evaluator state
func (p *ParallelDagEvaluator) Reset() {
	p.totalNodesEvaluated = 0
	p.totalPrimitiveEvaluations = 0
}
