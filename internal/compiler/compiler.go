package compiler

import (
	"fmt"
	"strings"
	"sync"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
	"github.com/PhucNguyen204/sigma-engine-golang/internal/matcher"
	"gopkg.in/yaml.v3"
)

// Compiler handles the compilation of SIGMA YAML rules into executable structures
type Compiler struct {
	// Primitive management
	primitiveMap     map[string]ir.PrimitiveID
	primitives       []ir.Primitive
	nextPrimitiveID  ir.PrimitiveID
	
	// Rule management
	ruleMap          map[string]ir.RuleID
	nextRuleID       ir.RuleID
	
	// Field mapping for normalization
	fieldMapping     *FieldMapping
	
	// Current compilation state
	currentRule      *SigmaRule
	currentSelections map[string][]ir.PrimitiveID
	
	// Thread safety
	mutex            sync.Mutex
	
	// Configuration
	config           CompilerConfig
}

// CompilerConfig contains configuration options for the compiler
type CompilerConfig struct {
	// Enable field mapping normalization
	EnableFieldMapping bool
	
	// Enable condition validation
	EnableConditionValidation bool
	
	// Enable primitive deduplication
	EnablePrimitiveDeduplication bool
	
	// Case sensitive field matching
	CaseSensitiveFields bool
	
	// Maximum rule complexity allowed
	MaxRuleComplexity int
	
	// Enable debug output
	Debug bool
}

// DefaultCompilerConfig returns a default compiler configuration
func DefaultCompilerConfig() CompilerConfig {
	return CompilerConfig{
		EnableFieldMapping:           true,
		EnableConditionValidation:    true,
		EnablePrimitiveDeduplication: true,
		CaseSensitiveFields:         false,
		MaxRuleComplexity:           100,
		Debug:                       false,
	}
}

// SigmaRule represents a parsed SIGMA rule
type SigmaRule struct {
	ID          string                 `yaml:"id"`
	Title       string                 `yaml:"title"`
	Description string                 `yaml:"description"`
	Status      string                 `yaml:"status"`
	Author      string                 `yaml:"author"`
	Date        string                 `yaml:"date"`
	Modified    string                 `yaml:"modified"`
	LogSource   map[string]interface{} `yaml:"logsource"`
	Detection   map[string]interface{} `yaml:"detection"`
	Fields      []string               `yaml:"fields"`
	FalsePositives []string            `yaml:"falsepositives"`
	Level       string                 `yaml:"level"`
	Tags        []string               `yaml:"tags"`
	References  []string               `yaml:"references"`
}

// CompilationResult represents the result of compiling SIGMA rules
type CompilationResult struct {
	Ruleset    *ir.CompiledRuleset
	Statistics CompilationStatistics
	Errors     []CompilationError
	Warnings   []CompilationWarning
}

// CompilationStatistics contains statistics about the compilation process
type CompilationStatistics struct {
	TotalRules          int
	SuccessfulRules     int
	FailedRules         int
	TotalPrimitives     int
	UniquePrimitives    int
	DuplicatedPrimitives int
	TotalSelections     int
	ComplexConditions   int
	AverageComplexity   float64
	CompilationTimeMs   int64
}

// CompilationError represents an error during compilation
type CompilationError struct {
	RuleID      string
	RuleTitle   string
	Type        string
	Message     string
	Field       string
	Line        int
}

// CompilationWarning represents a warning during compilation
type CompilationWarning struct {
	RuleID      string
	RuleTitle   string
	Type        string
	Message     string
	Field       string
}

// NewCompiler creates a new SIGMA rule compiler
func NewCompiler() *Compiler {
	return &Compiler{
		primitiveMap:      make(map[string]ir.PrimitiveID),
		primitives:        make([]ir.Primitive, 0),
		nextPrimitiveID:   0,
		ruleMap:           make(map[string]ir.RuleID),
		nextRuleID:        0,
		fieldMapping:      NewFieldMapping(),
		currentSelections: make(map[string][]ir.PrimitiveID),
		config:            DefaultCompilerConfig(),
	}
}

// NewCompilerWithConfig creates a new compiler with custom configuration
func NewCompilerWithConfig(config CompilerConfig) *Compiler {
	compiler := NewCompiler()
	compiler.config = config
	return compiler
}

// NewCompilerWithFieldMapping creates a new compiler with custom field mapping
func NewCompilerWithFieldMapping(fieldMapping *FieldMapping) *Compiler {
	compiler := NewCompiler()
	compiler.fieldMapping = fieldMapping
	return compiler
}

// SetFieldMapping sets the field mapping for the compiler
func (c *Compiler) SetFieldMapping(fieldMapping *FieldMapping) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.fieldMapping = fieldMapping
}

// GetFieldMapping returns the current field mapping
func (c *Compiler) GetFieldMapping() *FieldMapping {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.fieldMapping
}

// SetConfig sets the compiler configuration
func (c *Compiler) SetConfig(config CompilerConfig) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.config = config
}

// GetConfig returns the current compiler configuration
func (c *Compiler) GetConfig() CompilerConfig {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.config
}

// CompileRules compiles multiple SIGMA rule YAML strings into a CompiledRuleset
func (c *Compiler) CompileRules(ruleYamls []string) (*CompilationResult, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	result := &CompilationResult{
		Ruleset: ir.NewCompiledRuleset(),
		Statistics: CompilationStatistics{
			TotalRules: len(ruleYamls),
		},
		Errors:   make([]CompilationError, 0),
		Warnings: make([]CompilationWarning, 0),
	}
	
	// Reset state for new compilation
	c.resetState()
	
	// Compile each rule
	for i, ruleYaml := range ruleYamls {
		err := c.compileRule(ruleYaml, result)
		if err != nil {
			result.Errors = append(result.Errors, CompilationError{
				RuleID:  fmt.Sprintf("rule_%d", i),
				Type:    "ParseError",
				Message: err.Error(),
			})
			result.Statistics.FailedRules++
		} else {
			result.Statistics.SuccessfulRules++
		}
	}
	
	// Finalize statistics
	c.finalizeStatistics(result)
	
	// Copy primitives to ruleset
	for _, primitive := range c.primitives {
		result.Ruleset.AddPrimitive(primitive)
	}
	
	return result, nil
}

// CompileRule compiles a single SIGMA rule YAML string
func (c *Compiler) CompileRule(ruleYaml string) (*SigmaRule, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	var rule SigmaRule
	err := yaml.Unmarshal([]byte(ruleYaml), &rule)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}
	
	// Validate rule
	if err := c.validateRule(&rule); err != nil {
		return nil, fmt.Errorf("rule validation failed: %w", err)
	}
	
	return &rule, nil
}

// compileRule compiles a single rule and updates the result
func (c *Compiler) compileRule(ruleYaml string, result *CompilationResult) error {
	// Parse YAML directly without mutex lock to avoid deadlock
	var rule SigmaRule
	err := yaml.Unmarshal([]byte(ruleYaml), &rule)
	if err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}
	
	c.currentRule = &rule
	c.currentSelections = make(map[string][]ir.PrimitiveID)
	
	// Process detection section
	detection, ok := rule.Detection["detection"]
	if !ok {
		detection = rule.Detection
	}
	
	detectionMap, ok := detection.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid detection section format")
	}
	
	// Extract condition
	condition, hasCondition := detectionMap["condition"]
	if !hasCondition {
		return fmt.Errorf("no condition found in detection section")
	}
	
	conditionStr, ok := condition.(string)
	if !ok {
		return fmt.Errorf("condition must be a string")
	}
	
	// Validate condition if enabled
	if c.config.EnableConditionValidation {
		if err := ValidateCondition(conditionStr); err != nil {
			return fmt.Errorf("invalid condition syntax: %w", err)
		}
		
		// Check complexity
		complexity, err := ConditionComplexity(conditionStr)
		if err == nil && complexity > c.config.MaxRuleComplexity {
			result.Warnings = append(result.Warnings, CompilationWarning{
				RuleID:  rule.ID,
				RuleTitle: rule.Title,
				Type:    "Complexity",
				Message: fmt.Sprintf("Rule complexity %d exceeds limit %d", complexity, c.config.MaxRuleComplexity),
			})
			result.Statistics.ComplexConditions++
		}
	}
	
	// Process selections
	for key, value := range detectionMap {
		if key == "condition" {
			continue
		}
		
		err := c.processSelection(key, value, result)
		if err != nil {
			result.Warnings = append(result.Warnings, CompilationWarning{
				RuleID:    rule.ID,
				RuleTitle: rule.Title,
				Type:      "SelectionError",
				Message:   err.Error(),
				Field:     key,
			})
		}
	}
	
	return nil
}

// processSelection processes a selection block in the detection section
func (c *Compiler) processSelection(selectionName string, selectionValue interface{}, result *CompilationResult) error {
	selectionMap, ok := selectionValue.(map[string]interface{})
	if !ok {
		return fmt.Errorf("selection %s must be a map", selectionName)
	}
	
	var selectionPrimitives []ir.PrimitiveID
	
	for fieldName, fieldValue := range selectionMap {
		// Apply field mapping if enabled
		if c.config.EnableFieldMapping {
			fieldName = c.fieldMapping.MapField(fieldName)
		}
		
		primitives, err := c.createPrimitivesFromField(fieldName, fieldValue)
		if err != nil {
			return fmt.Errorf("failed to process field %s: %w", fieldName, err)
		}
		
		selectionPrimitives = append(selectionPrimitives, primitives...)
	}
	
	c.currentSelections[selectionName] = selectionPrimitives
	result.Statistics.TotalSelections++
	
	return nil
}

// createPrimitivesFromField creates primitives from a field definition
func (c *Compiler) createPrimitivesFromField(fieldName string, fieldValue interface{}) ([]ir.PrimitiveID, error) {
	var primitives []ir.PrimitiveID
	
	switch value := fieldValue.(type) {
	case string:
		// Single value
		primitive := c.createPrimitive(fieldName, "equals", []string{value}, []string{})
		primitives = append(primitives, primitive)
		
	case []interface{}:
		// Multiple values
		var values []string
		for _, v := range value {
			if str, ok := v.(string); ok {
				values = append(values, str)
			} else {
				values = append(values, fmt.Sprintf("%v", v))
			}
		}
		primitive := c.createPrimitive(fieldName, "equals", values, []string{})
		primitives = append(primitives, primitive)
		
	case map[string]interface{}:
		// Complex field with modifiers/operators
		for operator, operatorValue := range value {
			matchType, modifiers := c.parseOperator(operator)
			
			var values []string
			switch opVal := operatorValue.(type) {
			case string:
				values = []string{opVal}
			case []interface{}:
				for _, v := range opVal {
					values = append(values, fmt.Sprintf("%v", v))
				}
			default:
				values = []string{fmt.Sprintf("%v", opVal)}
			}
			
			primitive := c.createPrimitive(fieldName, matchType, values, modifiers)
			primitives = append(primitives, primitive)
		}
		
	default:
		// Convert to string
		primitive := c.createPrimitive(fieldName, "equals", []string{fmt.Sprintf("%v", value)}, []string{})
		primitives = append(primitives, primitive)
	}
	
	return primitives, nil
}

// parseOperator parses SIGMA operators and modifiers
func (c *Compiler) parseOperator(operator string) (string, []string) {
	var matchType string
	var modifiers []string
	
	// Split operator on pipes to extract modifiers
	parts := strings.Split(operator, "|")
	mainOp := parts[0]
	if len(parts) > 1 {
		modifiers = parts[1:]
	}
	
	// Map SIGMA operators to match types
	switch mainOp {
	case "contains":
		matchType = "contains"
	case "startswith":
		matchType = "startswith"
	case "endswith":
		matchType = "endswith"
	case "re":
		matchType = "regex"
	case "all":
		matchType = "all"
	default:
		matchType = "equals"
	}
	
	return matchType, modifiers
}

// createPrimitive creates a new primitive or returns existing one if deduplication is enabled
func (c *Compiler) createPrimitive(field, matchType string, values, modifiers []string) ir.PrimitiveID {
	primitive := ir.Primitive{
		Field:     field,
		MatchType: matchType,
		Values:    values,
		Modifiers: modifiers,
	}
	
	// Check for deduplication
	if c.config.EnablePrimitiveDeduplication {
		key := c.primitiveToKey(&primitive)
		if existingID, exists := c.primitiveMap[key]; exists {
			return existingID
		}
	}
	
	// Create new primitive
	primitiveID := c.nextPrimitiveID
	c.nextPrimitiveID++
	
	c.primitives = append(c.primitives, primitive)
	
	if c.config.EnablePrimitiveDeduplication {
		key := c.primitiveToKey(&primitive)
		c.primitiveMap[key] = primitiveID
	}
	
	return primitiveID
}

// primitiveToKey generates a unique key for a primitive
func (c *Compiler) primitiveToKey(primitive *ir.Primitive) string {
	var parts []string
	parts = append(parts, primitive.Field)
	parts = append(parts, primitive.MatchType)
	parts = append(parts, strings.Join(primitive.Values, "|"))
	parts = append(parts, strings.Join(primitive.Modifiers, "|"))
	return strings.Join(parts, "::")
}

// getOrCreateRuleID gets or creates a rule ID
func (c *Compiler) getOrCreateRuleID(ruleID, ruleTitle string) ir.RuleID {
	key := ruleID
	if key == "" {
		key = ruleTitle
	}
	
	if existingID, exists := c.ruleMap[key]; exists {
		return existingID
	}
	
	newRuleID := c.nextRuleID
	c.nextRuleID++
	c.ruleMap[key] = newRuleID
	
	return newRuleID
}

// validateRule validates a parsed SIGMA rule
func (c *Compiler) validateRule(rule *SigmaRule) error {
	if rule.Title == "" {
		return fmt.Errorf("rule must have a title")
	}
	
	if rule.Detection == nil {
		return fmt.Errorf("rule must have a detection section")
	}
	
	// Check for condition
	detection := rule.Detection
	if _, hasCondition := detection["condition"]; !hasCondition {
		return fmt.Errorf("detection section must have a condition")
	}
	
	return nil
}

// resetState resets the compiler state for a new compilation
func (c *Compiler) resetState() {
	c.primitiveMap = make(map[string]ir.PrimitiveID)
	c.primitives = make([]ir.Primitive, 0)
	c.nextPrimitiveID = 0
	c.ruleMap = make(map[string]ir.RuleID)
	c.nextRuleID = 0
	c.currentSelections = make(map[string][]ir.PrimitiveID)
}

// finalizeStatistics calculates final compilation statistics
func (c *Compiler) finalizeStatistics(result *CompilationResult) {
	result.Statistics.TotalPrimitives = len(c.primitives)
	result.Statistics.UniquePrimitives = len(c.primitiveMap)
	result.Statistics.DuplicatedPrimitives = result.Statistics.TotalPrimitives - result.Statistics.UniquePrimitives
	
	if result.Statistics.TotalRules > 0 {
		result.Statistics.AverageComplexity = float64(result.Statistics.ComplexConditions) / float64(result.Statistics.TotalRules)
	}
}

// GetPrimitives returns all compiled primitives
func (c *Compiler) GetPrimitives() []ir.Primitive {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	// Return a copy to avoid external modifications
	result := make([]ir.Primitive, len(c.primitives))
	copy(result, c.primitives)
	return result
}

// GetPrimitiveCount returns the number of compiled primitives
func (c *Compiler) GetPrimitiveCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.primitives)
}

// GetRuleCount returns the number of compiled rules
func (c *Compiler) GetRuleCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.ruleMap)
}

// BuildMatcher creates a matcher evaluator from compiled primitives
func (c *Compiler) BuildMatcher() (*matcher.MatcherEvaluator, error) {
	primitives := c.GetPrimitives()
	return matcher.QuickBuild(primitives)
}

// BuildMatcherWithBuilder creates a matcher using a custom builder
func (c *Compiler) BuildMatcherWithBuilder(builder *matcher.MatcherBuilder) (*matcher.MatcherEvaluator, error) {
	primitives := c.GetPrimitives()
	return builder.BuildEvaluator(primitives)
}

// Clear clears all compiled data
func (c *Compiler) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.resetState()
}

// Clone creates a copy of the compiler
func (c *Compiler) Clone() *Compiler {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	clone := NewCompilerWithConfig(c.config)
	clone.fieldMapping = c.fieldMapping.Clone()
	
	// Copy primitives
	for _, primitive := range c.primitives {
		clone.primitives = append(clone.primitives, primitive)
	}
	
	// Copy maps
	for k, v := range c.primitiveMap {
		clone.primitiveMap[k] = v
	}
	for k, v := range c.ruleMap {
		clone.ruleMap[k] = v
	}
	
	clone.nextPrimitiveID = c.nextPrimitiveID
	clone.nextRuleID = c.nextRuleID
	
	return clone
}
