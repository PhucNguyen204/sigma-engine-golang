package matcher

import (
	"testing"
)

// TestAdvancedFeatures tests the newly implemented advanced features
func TestAdvancedFeatures(t *testing.T) {
	registry := GetDefaultRegistry()
	RegisterComprehensiveModifiers(registry)

	// Test CIDR matching
	t.Run("CIDR Matching", func(t *testing.T) {
		cidrMatcher := CreateCIDRMatch()
		
		// Test IPv4 CIDR
		result, err := cidrMatcher("192.168.1.100", []string{"192.168.1.0/24"}, nil)
		if err != nil {
			t.Fatalf("CIDR match failed: %v", err)
		}
		if !result {
			t.Errorf("Expected IP 192.168.1.100 to match CIDR 192.168.1.0/24")
		}
		
		// Test single IP
		result, err = cidrMatcher("192.168.1.100", []string{"192.168.1.100"}, nil)
		if err != nil {
			t.Fatalf("Single IP match failed: %v", err)
		}
		if !result {
			t.Errorf("Expected IP 192.168.1.100 to match single IP 192.168.1.100")
		}
	})

	// Test numeric range matching
	t.Run("Numeric Range Matching", func(t *testing.T) {
		rangeMatcher := CreateNumericRangeMatch()
		
		// Test range format "1-10"
		result, err := rangeMatcher("5", []string{"1-10"}, nil)
		if err != nil {
			t.Fatalf("Range match failed: %v", err)
		}
		if !result {
			t.Errorf("Expected 5 to be in range 1-10")
		}
		
		// Test greater than format ">5"
		result, err = rangeMatcher("7", []string{">5"}, nil)
		if err != nil {
			t.Fatalf("Greater than match failed: %v", err)
		}
		if !result {
			t.Errorf("Expected 7 to be >5")
		}
	})

	// Test fuzzy matching
	t.Run("Fuzzy Matching", func(t *testing.T) {
		fuzzyMatcher := CreateFuzzyMatch()
		
		// Test similar strings
		result, err := fuzzyMatcher("test", []string{"tset"}, nil)
		if err != nil {
			t.Fatalf("Fuzzy match failed: %v", err)
		}
		if !result {
			t.Errorf("Expected 'test' and 'tset' to be similar")
		}
	})

	// Test length matching
	t.Run("Length Matching", func(t *testing.T) {
		lengthMatcher := CreateLengthMatch()
		
		// Test exact length
		result, err := lengthMatcher("hello", []string{"5"}, nil)
		if err != nil {
			t.Fatalf("Length match failed: %v", err)
		}
		if !result {
			t.Errorf("Expected 'hello' to have length 5")
		}
		
		// Test length range
		result, err = lengthMatcher("hello", []string{"3-6"}, nil)
		if err != nil {
			t.Fatalf("Length range match failed: %v", err)
		}
		if !result {
			t.Errorf("Expected 'hello' length to be in range 3-6")
		}
	})
}

// TestRegexCache tests the regex caching functionality
func TestRegexCache(t *testing.T) {
	// Test basic caching
	t.Run("Basic Caching", func(t *testing.T) {
		cache := GetGlobalCache()
		pattern := `\d+`
		
		// First compilation
		re1, err1 := cache.GetOrCompile(pattern)
		if err1 != nil {
			t.Fatalf("Failed to compile regex: %v", err1)
		}
		
		// Second compilation should use cache
		re2, err2 := cache.GetOrCompile(pattern)
		if err2 != nil {
			t.Fatalf("Failed to get cached regex: %v", err2)
		}
		
		// Should be the same regex object (from cache)
		if re1 != re2 {
			t.Errorf("Expected cached regex to be the same object")
		}
		
		// Test that it works
		if !re1.MatchString("123") {
			t.Errorf("Regex should match digits")
		}
	})
	
	// Test cache statistics
	t.Run("Cache Statistics", func(t *testing.T) {
		cache := GetGlobalCache()
		stats := cache.GetStats()
		t.Logf("Cache stats: Hits=%d, Misses=%d, Size=%d", 
			stats.Hits, stats.Misses, stats.CurrentSize)
		
		if stats.CurrentSize < 0 {
			t.Errorf("Cache size should be non-negative")
		}
	})
}

// TestComprehensiveModifiers tests the comprehensive modifier system
func TestComprehensiveModifiers(t *testing.T) {
	registry := NewMatcherRegistry()
	RegisterComprehensiveModifiers(registry)
	
	// Test base64 decoding
	t.Run("Base64 Decode", func(t *testing.T) {
		modifier, exists := registry.GetModifier("base64_decode")
		if !exists {
			t.Fatal("base64_decode modifier not found")
		}
		
		// Test valid base64
		result, err := modifier("aGVsbG8=") // "hello" in base64
		if err != nil {
			t.Fatalf("Base64 decode failed: %v", err)
		}
		if result != "hello" {
			t.Errorf("Expected 'hello', got '%s'", result)
		}
	})
	
	// Test string transformations
	t.Run("String Transformations", func(t *testing.T) {
		// Test lowercase
		lowerMod, exists := registry.GetModifier("lower")
		if !exists {
			t.Fatal("lower modifier not found")
		}
		
		result, err := lowerMod("HELLO")
		if err != nil {
			t.Fatalf("Lowercase failed: %v", err)
		}
		if result != "hello" {
			t.Errorf("Expected 'hello', got '%s'", result)
		}
		
		// Test trim
		trimMod, exists := registry.GetModifier("trim_spaces")
		if !exists {
			t.Fatal("trim_spaces modifier not found")
		}
		
		result, err = trimMod("  hello  ")
		if err != nil {
			t.Fatalf("Trim failed: %v", err)
		}
		if result != "hello" {
			t.Errorf("Expected 'hello', got '%s'", result)
		}
	})
	
	// Test URL encoding/decoding
	t.Run("URL Encoding", func(t *testing.T) {
		encodeMod, exists := registry.GetModifier("url_encode")
		if !exists {
			t.Fatal("url_encode modifier not found")
		}
		
		result, err := encodeMod("hello world")
		if err != nil {
			t.Fatalf("URL encode failed: %v", err)
		}
		if result != "hello+world" {
			t.Errorf("Expected 'hello+world', got '%s'", result)
		}
		
		decodeMod, exists := registry.GetModifier("url_decode")
		if !exists {
			t.Fatal("url_decode modifier not found")
		}
		
		result, err = decodeMod("hello+world")
		if err != nil {
			t.Fatalf("URL decode failed: %v", err)
		}
		if result != "hello world" {
			t.Errorf("Expected 'hello world', got '%s'", result)
		}
	})
}

// TestFilterIntegration tests the pre-filtering functionality
func TestFilterIntegration(t *testing.T) {
	// Test filter integration
	t.Run("Filter Integration", func(t *testing.T) {
		integration := NewFilterIntegration()
		
		// Test basic functionality
		if integration == nil {
			t.Fatal("FilterIntegration should not be nil")
		}
		
		// Test stats
		stats := integration.GetStats()
		if stats.TotalPrimitives < 0 {
			t.Errorf("TotalPrimitives should be non-negative")
		}
		
		t.Logf("Filter stats: Total=%d, Literal=%d, Regex=%d", 
			stats.TotalPrimitives, stats.LiteralPrimitives, stats.RegexPrimitives)
	})
	
	// Test literal prefilter
	t.Run("Literal Prefilter", func(t *testing.T) {
		literals := []string{"test", "example", "sample"}
		prefilter := &LiteralPrefilter{
			Literals: literals,
		}
		
		// Test matching literal
		if !prefilter.ShouldProcess("This is a test string") {
			t.Errorf("Should process string containing 'test'")
		}
		
		// Test non-matching literal
		if prefilter.ShouldProcess("This string has no matches") {
			t.Errorf("Should not process string with no matches")
		}
	})
}

// TestCompilationHooks tests the compilation hook system
func TestCompilationHooks(t *testing.T) {
	manager := NewCompilationHookManager()
	
	// Test hook registration and execution
	t.Run("Hook Registration", func(t *testing.T) {
		called := false
		
		hook := func(ctx *CompilationContext) error {
			called = true
			t.Logf("Hook called for phase: %v", ctx.Phase)
			return nil
		}
		
		manager.AddHook(PrimitiveDiscovery, hook)
		
		// Create test context
		ctx := &CompilationContext{
			Phase:         PrimitiveDiscovery,
			LiteralValues: []string{"test"},
			IsLiteralOnly: true,
			FieldName:     "test_field",
			MatchType:     "contains",
		}
		
		err := manager.ExecutePhase(PrimitiveDiscovery, ctx)
		if err != nil {
			t.Fatalf("Hook execution failed: %v", err)
		}
		
		if !called {
			t.Errorf("Hook should have been called")
		}
	})
	
	// Test multiple hooks
	t.Run("Multiple Hooks", func(t *testing.T) {
		callCount := 0
		
		for i := 0; i < 3; i++ {
			hook := func(ctx *CompilationContext) error {
				callCount++
				return nil
			}
			manager.AddHook(CompilationStart, hook)
		}
		
		ctx := &CompilationContext{
			Phase:     CompilationStart,
			FieldName: "test_field",
		}
		
		err := manager.ExecutePhase(CompilationStart, ctx)
		if err != nil {
			t.Fatalf("Multiple hooks execution failed: %v", err)
		}
		
		if callCount != 3 {
			t.Errorf("Expected 3 hook calls, got %d", callCount)
		}
	})
}
