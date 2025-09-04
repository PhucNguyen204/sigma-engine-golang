package matcher

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Advanced matching functions for complex SIGMA patterns
// Includes CIDR network matching, numeric ranges, and fuzzy matching

// CreateCIDRMatch creates a CIDR network matching function
// Supports both IPv4 and IPv6 CIDR notation
func CreateCIDRMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		ip := net.ParseIP(fieldValue)
		if ip == nil {
			return false, fmt.Errorf("invalid IP address: %s", fieldValue)
		}

		for _, cidrStr := range values {
			_, network, err := net.ParseCIDR(cidrStr)
			if err != nil {
				// Try as single IP
				singleIP := net.ParseIP(cidrStr)
				if singleIP != nil {
					if ip.Equal(singleIP) {
						return true, nil
					}
				} else {
					// Neither valid CIDR nor valid IP
					return false, fmt.Errorf("invalid CIDR or IP address: %s", cidrStr)
				}
				continue
			}

			if network.Contains(ip) {
				return true, nil
			}
		}

		return false, nil
	}
}

// CreateNumericRangeMatch creates a numeric range matching function
// Supports formats like "1-10", "10..20", ">5", "<100", ">=10", "<=50"
func CreateNumericRangeMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		fieldNum, err := parseNumber(fieldValue)
		if err != nil {
			return false, fmt.Errorf("invalid numeric value: %s", fieldValue)
		}

		for _, rangeStr := range values {
			match, err := isInNumericRange(fieldNum, rangeStr)
			if err != nil {
				return false, fmt.Errorf("invalid range format: %s", rangeStr)
			}
			if match {
				return true, nil
			}
		}

		return false, nil
	}
}

// CreateFuzzyMatch creates a fuzzy string matching function
// Uses simple edit distance algorithm
func CreateFuzzyMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		threshold := 0.8 // Default similarity threshold

		// Check for threshold modifier
		for _, mod := range modifiers {
			if strings.HasPrefix(mod, "fuzzy:") {
				if t, err := strconv.ParseFloat(strings.TrimPrefix(mod, "fuzzy:"), 64); err == nil && t >= 0.0 && t <= 1.0 {
					threshold = t
				}
			} else if strings.HasPrefix(mod, "threshold=") {
				if t, err := strconv.ParseFloat(strings.TrimPrefix(mod, "threshold="), 64); err == nil && t >= 0.0 && t <= 1.0 {
					threshold = t
				}
			}
		}

		for _, pattern := range values {
			similarity := calculateSimilarity(fieldValue, pattern)
			if similarity >= threshold {
				return true, nil
			}
		}

		return false, nil
	}
}

// CreateLengthMatch creates a string length matching function
// Supports formats like "5", "5-10", ">10", "<5"
func CreateLengthMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		fieldLength := float64(len(fieldValue))

		for _, lengthStr := range values {
			match, err := isInNumericRange(fieldLength, lengthStr)
			if err != nil {
				continue
			}
			if match {
				return true, nil
			}
		}

		return false, nil
	}
}

// Helper functions

// parseNumber parses a string as a number (int or float)
func parseNumber(s string) (float64, error) {
	// Try integer first
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return float64(i), nil
	}

	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}

	return 0, fmt.Errorf("not a number: %s", s)
}

// isInNumericRange checks if a number is within a specified range
func isInNumericRange(value float64, rangeStr string) (bool, error) {
	rangeStr = strings.TrimSpace(rangeStr)

	// Handle comparison operators
	if strings.HasPrefix(rangeStr, ">=") {
		min, err := parseNumber(strings.TrimPrefix(rangeStr, ">="))
		return value >= min, err
	}
	if strings.HasPrefix(rangeStr, "<=") {
		max, err := parseNumber(strings.TrimPrefix(rangeStr, "<="))
		return value <= max, err
	}
	if strings.HasPrefix(rangeStr, ">") {
		min, err := parseNumber(strings.TrimPrefix(rangeStr, ">"))
		return value > min, err
	}
	if strings.HasPrefix(rangeStr, "<") {
		max, err := parseNumber(strings.TrimPrefix(rangeStr, "<"))
		return value < max, err
	}

	// Handle range formats: "1-10", "10..20", "10...20"
	if strings.Contains(rangeStr, "..") {
		// Rust-style inclusive range "10..20" or exclusive "10...20"
		var parts []string
		inclusive := true
		if strings.Contains(rangeStr, "...") {
			parts = strings.SplitN(rangeStr, "...", 2)
			inclusive = false
		} else {
			parts = strings.SplitN(rangeStr, "..", 2)
		}

		if len(parts) == 2 {
			min, err1 := parseNumber(parts[0])
			max, err2 := parseNumber(parts[1])
			if err1 == nil && err2 == nil {
				if inclusive {
					return value >= min && value <= max, nil
				} else {
					return value >= min && value < max, nil
				}
			}
		}
	} else if strings.Contains(rangeStr, "-") && !strings.HasPrefix(rangeStr, "-") {
		// Traditional range "1-10" (but not negative numbers like "-5")
		parts := strings.SplitN(rangeStr, "-", 2)
		if len(parts) == 2 {
			min, err1 := parseNumber(parts[0])
			max, err2 := parseNumber(parts[1])
			if err1 == nil && err2 == nil {
				return value >= min && value <= max, nil
			}
		}
	}

	// Handle exact match
	exact, err := parseNumber(rangeStr)
	if err != nil {
		return false, err
	}
	return value == exact, nil
}

// calculateSimilarity calculates similarity between two strings using simple algorithm
func calculateSimilarity(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}

	if len(s1) == 0 || len(s2) == 0 {
		return 0.0
	}

	// Simple Jaccard similarity using character bigrams
	bigrams1 := getBigrams(s1)
	bigrams2 := getBigrams(s2)

	intersection := 0
	for bigram := range bigrams1 {
		if bigrams2[bigram] {
			intersection++
		}
	}

	union := len(bigrams1) + len(bigrams2) - intersection
	if union == 0 {
		return 0.0
	}

	return float64(intersection) / float64(union)
}

// getBigrams extracts character bigrams from a string
func getBigrams(s string) map[string]bool {
	bigrams := make(map[string]bool)
	if len(s) < 2 {
		return bigrams
	}

	for i := 0; i < len(s)-1; i++ {
		bigram := s[i : i+2]
		bigrams[bigram] = true
	}

	return bigrams
}

// RegisterAdvancedMatchers registers all advanced matching functions
func RegisterAdvancedMatchers(registry *MatcherRegistry) {
	registry.RegisterMatcher("cidr", CreateCIDRMatch())
	registry.RegisterMatcher("network", CreateCIDRMatch()) // Alias
	registry.RegisterMatcher("range", CreateNumericRangeMatch())
	registry.RegisterMatcher("numeric_range", CreateNumericRangeMatch()) // Alias
	registry.RegisterMatcher("fuzzy", CreateFuzzyMatch())
	registry.RegisterMatcher("similar", CreateFuzzyMatch()) // Alias
	registry.RegisterMatcher("length", CreateLengthMatch())
}
