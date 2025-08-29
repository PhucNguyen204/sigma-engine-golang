package matcher

import (
	"strings"
	"sync"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// FilterCompilationStats contains statistics from filter compilation
type FilterCompilationStats struct {
	TotalPrimitives      int
	LiteralPrimitives    int
	RegexPrimitives      int
	UniqueFields         int
	AverageSelectivity   float64
	EstimatedMemoryUsage int
}

// FilterIntegration helps collect patterns for external filter libraries
type FilterIntegration struct {
	// Patterns for different filter types
	AhoCorasickPatterns []string
	LiteralPatterns     map[string][]string // field -> patterns
	RegexPatterns       map[string][]string // field -> patterns
	
	// Statistics
	Stats FilterCompilationStats
	
	// Thread safety
	mutex sync.RWMutex
}

// NewFilterIntegration creates a new filter integration helper
func NewFilterIntegration() *FilterIntegration {
	return &FilterIntegration{
		AhoCorasickPatterns: make([]string, 0),
		LiteralPatterns:     make(map[string][]string),
		RegexPatterns:       make(map[string][]string),
		Stats:               FilterCompilationStats{},
	}
}

// AddPrimitive analyzes a primitive and extracts patterns for filtering
func (f *FilterIntegration) AddPrimitive(primitive *ir.Primitive) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	
	f.Stats.TotalPrimitives++
	
	switch primitive.MatchType {
	case "equals", "contains", "startswith", "endswith":
		f.addLiteralPrimitive(primitive)
	case "regex", "re":
		f.addRegexPrimitive(primitive)
	default:
		f.addLiteralPrimitive(primitive) // Default to literal
	}
}

// addLiteralPrimitive processes literal-based primitives
func (f *FilterIntegration) addLiteralPrimitive(primitive *ir.Primitive) {
	f.Stats.LiteralPrimitives++
	
	// Add to field-specific patterns
	if f.LiteralPatterns[primitive.Field] == nil {
		f.LiteralPatterns[primitive.Field] = make([]string, 0)
	}
	
	for _, value := range primitive.Values {
		// Apply modifiers to get final pattern
		finalValue := f.applyModifiers(value, primitive.Modifiers)
		
		f.LiteralPatterns[primitive.Field] = append(f.LiteralPatterns[primitive.Field], finalValue)
		f.AhoCorasickPatterns = append(f.AhoCorasickPatterns, finalValue)
	}
}

// addRegexPrimitive processes regex-based primitives
func (f *FilterIntegration) addRegexPrimitive(primitive *ir.Primitive) {
	f.Stats.RegexPrimitives++
	
	// Add to field-specific regex patterns
	if f.RegexPatterns[primitive.Field] == nil {
		f.RegexPatterns[primitive.Field] = make([]string, 0)
	}
	
	for _, pattern := range primitive.Values {
		f.RegexPatterns[primitive.Field] = append(f.RegexPatterns[primitive.Field], pattern)
	}
}

// applyModifiers applies modifiers to a value (simplified version)
func (f *FilterIntegration) applyModifiers(value string, modifiers []string) string {
	result := value
	
	for _, modifier := range modifiers {
		switch modifier {
		case "nocase", "ignore_case":
			result = strings.ToLower(result)
		case "trim":
			result = strings.TrimSpace(result)
		case "upper":
			result = strings.ToUpper(result)
		case "lower":
			result = strings.ToLower(result)
		}
	}
	
	return result
}

// GetAhoCorasickPatterns returns patterns suitable for AhoCorasick matching
func (f *FilterIntegration) GetAhoCorasickPatterns() []string {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	
	// Deduplicate patterns
	seen := make(map[string]bool)
	result := make([]string, 0)
	
	for _, pattern := range f.AhoCorasickPatterns {
		if !seen[pattern] {
			seen[pattern] = true
			result = append(result, pattern)
		}
	}
	
	return result
}

// GetLiteralPatternsByField returns literal patterns grouped by field
func (f *FilterIntegration) GetLiteralPatternsByField() map[string][]string {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	
	// Create a copy to avoid external modifications
	result := make(map[string][]string)
	for field, patterns := range f.LiteralPatterns {
		result[field] = make([]string, len(patterns))
		copy(result[field], patterns)
	}
	
	return result
}

// GetRegexPatternsByField returns regex patterns grouped by field
func (f *FilterIntegration) GetRegexPatternsByField() map[string][]string {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	
	// Create a copy to avoid external modifications
	result := make(map[string][]string)
	for field, patterns := range f.RegexPatterns {
		result[field] = make([]string, len(patterns))
		copy(result[field], patterns)
	}
	
	return result
}

// GetStatistics returns compilation statistics
func (f *FilterIntegration) GetStatistics() FilterCompilationStats {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	
	stats := f.Stats
	stats.UniqueFields = len(f.LiteralPatterns) + len(f.RegexPatterns)
	
	// Calculate average selectivity (simplified)
	if stats.TotalPrimitives > 0 {
		stats.AverageSelectivity = float64(stats.LiteralPrimitives) / float64(stats.TotalPrimitives)
	}
	
	// Estimate memory usage (simplified)
	memoryUsage := 0
	for _, patterns := range f.LiteralPatterns {
		for _, pattern := range patterns {
			memoryUsage += len(pattern)
		}
	}
	for _, patterns := range f.RegexPatterns {
		for _, pattern := range patterns {
			memoryUsage += len(pattern) * 2 // Regex patterns are more expensive
		}
	}
	stats.EstimatedMemoryUsage = memoryUsage
	
	return stats
}

// Clear resets all collected patterns and statistics
func (f *FilterIntegration) Clear() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	
	f.AhoCorasickPatterns = make([]string, 0)
	f.LiteralPatterns = make(map[string][]string)
	f.RegexPatterns = make(map[string][]string)
	f.Stats = FilterCompilationStats{}
}

// LiteralPrefilter provides fast literal-based pre-filtering
type LiteralPrefilter struct {
	patterns map[string]bool
	mutex    sync.RWMutex
}

// NewLiteralPrefilter creates a new literal pre-filter
func NewLiteralPrefilter(patterns []string) *LiteralPrefilter {
	patternMap := make(map[string]bool)
	for _, pattern := range patterns {
		patternMap[pattern] = true
	}
	
	return &LiteralPrefilter{
		patterns: patternMap,
	}
}

// MightMatch checks if a value might match any of the patterns
func (p *LiteralPrefilter) MightMatch(value string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	
	// Check exact match
	if p.patterns[value] {
		return true
	}
	
	// Check if value contains any pattern
	for pattern := range p.patterns {
		if strings.Contains(value, pattern) {
			return true
		}
	}
	
	return false
}

// GetPatternCount returns the number of patterns in the filter
func (p *LiteralPrefilter) GetPatternCount() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return len(p.patterns)
}

// CreatePrefilterFromIntegration creates a literal prefilter from filter integration
func CreatePrefilterFromIntegration(integration *FilterIntegration) *LiteralPrefilter {
	patterns := integration.GetAhoCorasickPatterns()
	return NewLiteralPrefilter(patterns)
}

// IsLiteralMatchType checks if a match type is suitable for literal filtering
func IsLiteralMatchType(matchType string) bool {
	switch matchType {
	case "equals", "contains", "startswith", "endswith", "in":
		return true
	default:
		return false
	}
}

// CalculateSelectivity estimates the selectivity of a pattern (lower = more selective)
func CalculateSelectivity(pattern string) float64 {
	// Simple heuristic: shorter patterns are less selective
	// This is a simplified calculation
	baseSelectivity := 1.0 / float64(len(pattern)+1)
	
	// Adjust for special characters (more selective)
	specialChars := 0
	for _, char := range pattern {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9')) {
			specialChars++
		}
	}
	
	selectivityBonus := float64(specialChars) * 0.1
	return baseSelectivity - selectivityBonus
}
