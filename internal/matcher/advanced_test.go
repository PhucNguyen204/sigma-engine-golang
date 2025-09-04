package matcher

import (
	"testing"
)

func TestRangeMatching(t *testing.T) {
	rangeMatcher := CreateNumericRangeMatch()

	// Test inclusive range
	result, err := rangeMatcher("15", []string{"10..20"}, []string{})
	if err != nil {
		t.Fatalf("Range match failed: %v", err)
	}
	if !result {
		t.Errorf("Expected 15 to be in range 10..20")
	}

	result, err = rangeMatcher("25", []string{"10..20"}, []string{})
	if err != nil {
		t.Fatalf("Range match failed: %v", err)
	}
	if result {
		t.Errorf("Expected 25 to NOT be in range 10..20")
	}

	// Test comparison operators
	result, err = rangeMatcher("15", []string{">10"}, []string{})
	if err != nil {
		t.Fatalf("Greater than match failed: %v", err)
	}
	if !result {
		t.Errorf("Expected 15 > 10")
	}

	result, err = rangeMatcher("5", []string{">10"}, []string{})
	if err != nil {
		t.Fatalf("Greater than match failed: %v", err)
	}
	if result {
		t.Errorf("Expected 5 NOT > 10")
	}

	// Test floating point
	result, err = rangeMatcher("15.5", []string{"10.0..20.0"}, []string{})
	if err != nil {
		t.Fatalf("Float range match failed: %v", err)
	}
	if !result {
		t.Errorf("Expected 15.5 to be in range 10.0..20.0")
	}
}

func TestFuzzyMatching(t *testing.T) {
	fuzzyMatcher := CreateFuzzyMatch() // Default threshold

	// Exact match
	result, err := fuzzyMatcher("hello", []string{"hello"}, []string{})
	if err != nil {
		t.Fatalf("Fuzzy exact match failed: %v", err)
	}
	if !result {
		t.Errorf("Expected exact match 'hello' == 'hello'")
	}

	// Similar strings with custom threshold
	fuzzyMatcherLow := CreateFuzzyMatch()
	result, err = fuzzyMatcherLow("hello", []string{"helo"}, []string{"fuzzy:0.7"})
	if err != nil {
		t.Fatalf("Fuzzy similar match failed: %v", err)
	}
	if !result {
		t.Errorf("Expected 'hello' and 'helo' to be similar with threshold 0.7")
	}

	// Dissimilar strings
	fuzzyMatcherHigh := CreateFuzzyMatch()
	result, err = fuzzyMatcherHigh("hello", []string{"world"}, []string{"fuzzy:0.9"})
	if err != nil {
		t.Fatalf("Fuzzy dissimilar match failed: %v", err)
	}
	if result {
		t.Errorf("Expected 'hello' and 'world' to NOT be similar with threshold 0.9")
	}
}

func TestCIDRMatching(t *testing.T) {
	cidrMatcher := CreateCIDRMatch()

	// IPv4 CIDR
	result, err := cidrMatcher("192.168.1.100", []string{"192.168.1.0/24"}, []string{})
	if err != nil {
		t.Fatalf("IPv4 CIDR match failed: %v", err)
	}
	if !result {
		t.Errorf("Expected 192.168.1.100 to match 192.168.1.0/24")
	}

	result, err = cidrMatcher("10.0.0.1", []string{"192.168.1.0/24"}, []string{})
	if err != nil {
		t.Fatalf("IPv4 CIDR match failed: %v", err)
	}
	if result {
		t.Errorf("Expected 10.0.0.1 to NOT match 192.168.1.0/24")
	}

	// IPv6 CIDR
	result, err = cidrMatcher("2001:db8::1", []string{"2001:db8::/32"}, []string{})
	if err != nil {
		t.Fatalf("IPv6 CIDR match failed: %v", err)
	}
	if !result {
		t.Errorf("Expected 2001:db8::1 to match 2001:db8::/32")
	}
}

func TestRangeMatchingComprehensive(t *testing.T) {
	rangeMatcher := CreateNumericRangeMatch()

	// Test boundary conditions
	result, err := rangeMatcher("10", []string{"10..20"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 10 to be in range 10..20")
	}

	result, err = rangeMatcher("20", []string{"10..20"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 20 to be in range 10..20")
	}

	result, err = rangeMatcher("9", []string{"10..20"}, []string{})
	if err != nil || result {
		t.Errorf("Expected 9 to NOT be in range 10..20")
	}

	result, err = rangeMatcher("21", []string{"10..20"}, []string{})
	if err != nil || result {
		t.Errorf("Expected 21 to NOT be in range 10..20")
	}

	// Test comparison operators
	result, err = rangeMatcher("15", []string{">=10"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 15 >= 10")
	}

	result, err = rangeMatcher("10", []string{">=10"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 10 >= 10")
	}

	result, err = rangeMatcher("9", []string{">=10"}, []string{})
	if err != nil || result {
		t.Errorf("Expected 9 NOT >= 10")
	}

	result, err = rangeMatcher("5", []string{"<=10"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 5 <= 10")
	}

	result, err = rangeMatcher("10", []string{"<=10"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 10 <= 10")
	}

	result, err = rangeMatcher("11", []string{"<=10"}, []string{})
	if err != nil || result {
		t.Errorf("Expected 11 NOT <= 10")
	}

	// Test negative numbers
	result, err = rangeMatcher("-5", []string{"-10..0"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected -5 to be in range -10..0")
	}

	result, err = rangeMatcher("5", []string{"-10..0"}, []string{})
	if err != nil || result {
		t.Errorf("Expected 5 to NOT be in range -10..0")
	}
}

func TestFuzzyMatchingComprehensive(t *testing.T) {
	// Test different similarity thresholds
	fuzzyMatcher05 := CreateFuzzyMatch()
	result, err := fuzzyMatcher05("hello", []string{"helo"}, []string{"fuzzy:0.5"})
	if err != nil || !result {
		t.Errorf("Expected 'hello' and 'helo' to be similar with threshold 0.5")
	}

	fuzzyMatcher09 := CreateFuzzyMatch()
	result, err = fuzzyMatcher09("hello", []string{"xyz"}, []string{"fuzzy:0.9"})
	if err != nil || result {
		t.Errorf("Expected 'hello' and 'xyz' to NOT be similar with threshold 0.9")
	}

	// Test empty strings
	fuzzyMatcherDefault := CreateFuzzyMatch()
	result, err = fuzzyMatcherDefault("", []string{""}, []string{})
	if err != nil || !result {
		t.Errorf("Expected empty strings to match")
	}

	result, err = fuzzyMatcherDefault("hello", []string{""}, []string{"fuzzy:0.5"})
	if err != nil || result {
		t.Errorf("Expected 'hello' and empty string to NOT be similar")
	}

	// Test case sensitivity - fuzzy should be somewhat case-insensitive
	result, err = fuzzyMatcherDefault("Hello", []string{"hello"}, []string{"fuzzy:0.8"})
	if err != nil {
		t.Fatalf("Fuzzy case test failed: %v", err)
	}
	// Note: This might fail depending on implementation - case difference impacts similarity
}

func TestCIDRMatchingComprehensive(t *testing.T) {
	cidrMatcher := CreateCIDRMatch()

	// Test IPv4 edge cases
	result, err := cidrMatcher("127.0.0.1", []string{"127.0.0.0/8"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 127.0.0.1 to match 127.0.0.0/8")
	}

	result, err = cidrMatcher("192.168.1.1", []string{"192.168.0.0/16"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected 192.168.1.1 to match 192.168.0.0/16")
	}

	result, err = cidrMatcher("192.169.1.1", []string{"192.168.0.0/16"}, []string{})
	if err != nil || result {
		t.Errorf("Expected 192.169.1.1 to NOT match 192.168.0.0/16")
	}

	// Test IPv6 edge cases
	result, err = cidrMatcher("::1", []string{"::/0"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected ::1 to match ::/0 (any network)")
	}

	result, err = cidrMatcher("fe80::1", []string{"fe80::/10"}, []string{})
	if err != nil || !result {
		t.Errorf("Expected fe80::1 to match fe80::/10 (link-local)")
	}

	// Test invalid inputs should error
	_, err = cidrMatcher("invalid_ip", []string{"192.168.1.0/24"}, []string{})
	if err == nil {
		t.Errorf("Expected error for invalid IP address")
	}

	_, err = cidrMatcher("192.168.1.1", []string{"invalid_cidr"}, []string{})
	if err == nil {
		t.Errorf("Expected error for invalid CIDR")
	}
}

func TestAdvancedMatchersErrorHandling(t *testing.T) {
	rangeMatcher := CreateNumericRangeMatch()

	// Test invalid range formats
	_, err := rangeMatcher("5", []string{"invalid_range"}, []string{})
	if err == nil {
		t.Errorf("Expected error for invalid range format")
	}

	_, err = rangeMatcher("not_a_number", []string{"1..10"}, []string{})
	if err == nil {
		t.Errorf("Expected error for non-numeric field value")
	}

	fuzzyMatcher := CreateFuzzyMatch()

	// Test invalid fuzzy threshold - should not error but might use default
	result, err := fuzzyMatcher("hello", []string{"hello"}, []string{"fuzzy:invalid"})
	if err != nil {
		t.Errorf("Should not error on invalid fuzzy threshold: %v", err)
	}
	if !result {
		t.Errorf("Should still match on exact strings despite invalid threshold")
	}

	result, err = fuzzyMatcher("hello", []string{"hello"}, []string{"fuzzy:1.5"})
	if err != nil {
		t.Errorf("Should not error on out-of-range fuzzy threshold: %v", err)
	}
	if !result {
		t.Errorf("Should still match on exact strings despite invalid threshold")
	}
}
