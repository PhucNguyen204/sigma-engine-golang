package matcher

import (
	"encoding/base64"
	"regexp"
	"strconv"
	"strings"
)

// RegisterDefaultMatchers registers all default match functions
func RegisterDefaultMatchers() {
	registry := GetDefaultRegistry()

	// Register comprehensive modifiers from modifiers.go
	RegisterComprehensiveModifiers(registry)

	// Exact match functions
	registry.RegisterMatcher("equals", CreateExactMatch())
	registry.RegisterMatcher("exact", CreateExactMatch())

	// String matching functions
	registry.RegisterMatcher("contains", CreateContainsMatch())
	registry.RegisterMatcher("startswith", CreateStartsWithMatch())
	registry.RegisterMatcher("endswith", CreateEndsWithMatch())

	// Pattern matching functions
	registry.RegisterMatcher("regex", CreateRegexMatch())
	registry.RegisterMatcher("re", CreateRegexMatch())

	// Advanced matching functions from advanced.go
	registry.RegisterMatcher("cidr", CreateCIDRMatch())
	registry.RegisterMatcher("range", CreateNumericRangeMatch())
	registry.RegisterMatcher("fuzzy", CreateFuzzyMatch())
	registry.RegisterMatcher("length", CreateLengthMatch())

	// Wildcard matching functions
	registry.RegisterMatcher("glob", CreateGlobMatch())
	registry.RegisterMatcher("wildcard", CreateGlobMatch())
}

// Helper function for numeric comparisons
func CreateNumericComparator(compare func(float64, float64) bool) func(fieldValues, ruleValues []string) bool {
	return func(fieldValues, ruleValues []string) bool {
		for _, fieldValue := range fieldValues {
			for _, ruleValue := range ruleValues {
				if fv, err1 := strconv.ParseFloat(fieldValue, 64); err1 == nil {
					if rv, err2 := strconv.ParseFloat(ruleValue, 64); err2 == nil {
						if compare(fv, rv) {
							return true
						}
					}
				}
			}
		}
		return false
	}
}

// RegisterDefaults registers both default matchers and modifiers
func RegisterDefaults() {
	RegisterDefaultMatchers()
}

// CreateExactMatch creates an exact string match function
func CreateExactMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		for _, value := range values {
			if fieldValue == value {
				return true, nil
			}
		}
		return false, nil
	}
}

// CreateContainsMatch creates a substring match function
func CreateContainsMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		for _, value := range values {
			if strings.Contains(fieldValue, value) {
				return true, nil
			}
		}
		return false, nil
	}
}

// CreateStartsWithMatch creates a prefix match function
func CreateStartsWithMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		for _, value := range values {
			if strings.HasPrefix(fieldValue, value) {
				return true, nil
			}
		}
		return false, nil
	}
}

// CreateEndsWithMatch creates a suffix match function
func CreateEndsWithMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		for _, value := range values {
			if strings.HasSuffix(fieldValue, value) {
				return true, nil
			}
		}
		return false, nil
	}
}

// CreateRegexMatch creates a regular expression match function
func CreateRegexMatch() MatchFn {
	// Use a simple regex cache to avoid recompiling
	regexCache := make(map[string]*regexp.Regexp)

	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		for _, pattern := range values {
			// Check cache first
			regex, exists := regexCache[pattern]
			if !exists {
				var err error
				regex, err = regexp.Compile(pattern)
				if err != nil {
					return false, err
				}
				regexCache[pattern] = regex
			}

			if regex.MatchString(fieldValue) {
				return true, nil
			}
		}
		return false, nil
	}
}

// CreateGlobMatch creates a glob/wildcard match function
func CreateGlobMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		for _, pattern := range values {
			matched, err := globMatch(pattern, fieldValue)
			if err != nil {
				return false, err
			}
			if matched {
				return true, nil
			}
		}
		return false, nil
	}
}

// globMatch implements simple glob pattern matching
// Supports * (any characters) and ? (single character)
func globMatch(pattern, text string) (bool, error) {
	// Convert glob pattern to regex
	regexPattern := globToRegex(pattern)
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return false, err
	}
	return regex.MatchString(text), nil
}

// globToRegex converts a glob pattern to a regex pattern
func globToRegex(glob string) string {
	var result strings.Builder
	result.WriteString("^")

	for i, char := range glob {
		switch char {
		case '*':
			result.WriteString(".*")
		case '?':
			result.WriteString(".")
		case '.', '+', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			result.WriteString("\\")
			result.WriteRune(char)
		default:
			result.WriteRune(char)
		}
		_ = i // Suppress unused variable warning
	}

	result.WriteString("$")
	return result.String()
}

// CreateLowercaseModifier creates a lowercase transformation modifier
func CreateLowercaseModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.ToLower(input), nil
	}
}

// CreateUppercaseModifier creates an uppercase transformation modifier
func CreateUppercaseModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.ToUpper(input), nil
	}
}

// CreateTrimModifier creates a whitespace trimming modifier
func CreateTrimModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.TrimSpace(input), nil
	}
}

// CreateBase64DecodeModifier creates a Base64 decoding modifier
func CreateBase64DecodeModifier() ModifierFn {
	return func(input string) (string, error) {
		decoded, err := base64.StdEncoding.DecodeString(input)
		if err != nil {
			// Try URL encoding if standard fails
			decoded, err = base64.URLEncoding.DecodeString(input)
			if err != nil {
				return "", err
			}
		}
		return string(decoded), nil
	}
}

// Advanced matchers for specialized use cases

// CreateCaseInsensitiveMatch creates a case-insensitive exact match function
func CreateCaseInsensitiveMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		lowercaseField := strings.ToLower(fieldValue)
		for _, value := range values {
			if lowercaseField == strings.ToLower(value) {
				return true, nil
			}
		}
		return false, nil
	}
}

// CreateCaseInsensitiveContains creates a case-insensitive substring match function
func CreateCaseInsensitiveContains() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		lowercaseField := strings.ToLower(fieldValue)
		for _, value := range values {
			if strings.Contains(lowercaseField, strings.ToLower(value)) {
				return true, nil
			}
		}
		return false, nil
	}
}

// CreateNumericMatch creates a numeric comparison match function
func CreateNumericMatch() MatchFn {
	return func(fieldValue string, values []string, modifiers []string) (bool, error) {
		// Simple numeric string comparison for now
		// Could be extended to handle ranges, operators, etc.
		for _, value := range values {
			if fieldValue == value {
				return true, nil
			}
		}
		return false, nil
	}
}

// Advanced modifiers

// CreateJsonExtractModifier creates a JSON field extraction modifier
func CreateJsonExtractModifier(fieldPath string) ModifierFn {
	return func(input string) (string, error) {
		// Simple JSON field extraction - could be enhanced with proper JSON parsing
		// For now, return the input as-is
		return input, nil
	}
}

// CreateRegexExtractModifier creates a regex group extraction modifier
func CreateRegexExtractModifier(pattern string, groupIndex int) ModifierFn {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		// Return a modifier that always returns an error
		return func(input string) (string, error) {
			return "", err
		}
	}

	return func(input string) (string, error) {
		matches := regex.FindStringSubmatch(input)
		if len(matches) <= groupIndex {
			return "", nil // No match or group not found
		}
		return matches[groupIndex], nil
	}
}
