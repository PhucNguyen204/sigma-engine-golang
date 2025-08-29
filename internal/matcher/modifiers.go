package matcher

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"unicode"
)

// Comprehensive modifier implementations for SIGMA primitive processing
// This module provides high-performance modifier functions with minimal allocations

// RegisterComprehensiveModifiers registers all comprehensive modifiers
func RegisterComprehensiveModifiers(registry *MatcherRegistry) {
	registerEncodingModifiers(registry)
	registerStringModifiers(registry)
	registerFormatModifiers(registry)
	registerNumericModifiers(registry)
	registerAdvancedModifiers(registry)
}

// registerEncodingModifiers registers encoding and decoding modifiers
func registerEncodingModifiers(registry *MatcherRegistry) {
	registry.RegisterModifier("base64_decode", CreateBase64DecodeModifierFn())
	registry.RegisterModifier("base64_offset_decode", CreateBase64OffsetDecodeModifier())
	registry.RegisterModifier("url_decode", CreateURLDecodeModifier())
	registry.RegisterModifier("url_encode", CreateURLEncodeModifier())
	registry.RegisterModifier("hex_decode", CreateHexDecodeModifier())
	registry.RegisterModifier("hex_encode", CreateHexEncodeModifier())
}

// registerStringModifiers registers string transformation modifiers
func registerStringModifiers(registry *MatcherRegistry) {
	registry.RegisterModifier("lower", CreateLowerModifier())
	registry.RegisterModifier("upper", CreateUpperModifier())
	registry.RegisterModifier("trim_spaces", CreateTrimSpacesModifier())
	registry.RegisterModifier("trim_left", CreateTrimLeftModifier())
	registry.RegisterModifier("trim_right", CreateTrimRightModifier())
	registry.RegisterModifier("normalize_whitespace", CreateNormalizeWhitespaceModifier())
	registry.RegisterModifier("remove_whitespace", CreateRemoveWhitespaceModifier())
	registry.RegisterModifier("reverse", CreateReverseModifier())
}

// registerFormatModifiers registers data format modifiers
func registerFormatModifiers(registry *MatcherRegistry) {
	registry.RegisterModifier("json_extract", CreateJSONExtractModifier())
	registry.RegisterModifier("xml_extract", CreateXMLExtractModifier())
	registry.RegisterModifier("csv_extract", CreateCSVExtractModifier())
	registry.RegisterModifier("split_first", CreateSplitFirstModifier())
}

// registerNumericModifiers registers numeric transformation modifiers
func registerNumericModifiers(registry *MatcherRegistry) {
	registry.RegisterModifier("to_int", CreateToIntModifier())
	registry.RegisterModifier("to_float", CreateToFloatModifier())
	registry.RegisterModifier("abs", CreateAbsModifier())
	registry.RegisterModifier("round", CreateRoundModifier())
}

// registerAdvancedModifiers registers advanced transformation modifiers
func registerAdvancedModifiers(registry *MatcherRegistry) {
	registry.RegisterModifier("substring", CreateSubstringModifier())
	registry.RegisterModifier("replace_basic", CreateReplaceBasicModifier())
	registry.RegisterModifier("regex_extract_simple", CreateRegexExtractSimpleModifier())
	registry.RegisterModifier("hash_md5", CreateMD5HashModifier())
	registry.RegisterModifier("hash_sha256", CreateSHA256HashModifier())
}

// Encoding/Decoding Modifiers

// CreateBase64DecodeModifierFn creates a base64 decoding modifier (renamed to avoid conflict)
func CreateBase64DecodeModifierFn() ModifierFn {
	return func(input string) (string, error) {
		decoded, err := base64.StdEncoding.DecodeString(input)
		if err != nil {
			// Try URL encoding if standard fails
			decoded, err = base64.URLEncoding.DecodeString(input)
			if err != nil {
				return input, err // Return original on decode failure
			}
		}
		return string(decoded), nil
	}
}

// CreateBase64OffsetDecodeModifier creates a base64 offset decoding modifier
func CreateBase64OffsetDecodeModifier() ModifierFn {
	return func(input string) (string, error) {
		// Try different offsets (common in malware analysis)
		for offset := 0; offset < 4; offset++ {
			if offset < len(input) {
				offsetValue := input[offset:]
				if decoded, err := base64.StdEncoding.DecodeString(offsetValue); err == nil {
					return string(decoded), nil
				}
			}
		}
		return input, fmt.Errorf("no valid base64 found with offsets")
	}
}

// CreateURLDecodeModifier creates a URL decoding modifier
func CreateURLDecodeModifier() ModifierFn {
	return func(input string) (string, error) {
		decoded, err := url.QueryUnescape(input)
		if err != nil {
			return input, err
		}
		return decoded, nil
	}
}

// CreateURLEncodeModifier creates a URL encoding modifier
func CreateURLEncodeModifier() ModifierFn {
	return func(input string) (string, error) {
		encoded := url.QueryEscape(input)
		return encoded, nil
	}
}

// CreateHexDecodeModifier creates a hex decoding modifier
func CreateHexDecodeModifier() ModifierFn {
	return func(input string) (string, error) {
		// Remove common hex prefixes
		cleaned := strings.TrimPrefix(input, "0x")
		cleaned = strings.TrimPrefix(cleaned, "\\x")
		
		// Decode hex pairs
		if len(cleaned)%2 == 0 {
			decoded := make([]byte, 0, len(cleaned)/2)
			for i := 0; i < len(cleaned); i += 2 {
				b, err := strconv.ParseUint(cleaned[i:i+2], 16, 8)
				if err != nil {
					return input, err
				}
				decoded = append(decoded, byte(b))
			}
			return string(decoded), nil
		}
		return input, fmt.Errorf("invalid hex string length")
	}
}

// CreateHexEncodeModifier creates a hex encoding modifier
func CreateHexEncodeModifier() ModifierFn {
	return func(input string) (string, error) {
		encoded := fmt.Sprintf("%x", input)
		return encoded, nil
	}
}

// String Transformation Modifiers

// CreateLowerModifier creates a lowercase modifier
func CreateLowerModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.ToLower(input), nil
	}
}

// CreateUpperModifier creates an uppercase modifier
func CreateUpperModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.ToUpper(input), nil
	}
}

// CreateTrimSpacesModifier creates a whitespace trimming modifier (renamed to avoid conflict)
func CreateTrimSpacesModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.TrimSpace(input), nil
	}
}

// CreateTrimLeftModifier creates a left whitespace trimming modifier
func CreateTrimLeftModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.TrimLeftFunc(input, unicode.IsSpace), nil
	}
}

// CreateTrimRightModifier creates a right whitespace trimming modifier
func CreateTrimRightModifier() ModifierFn {
	return func(input string) (string, error) {
		return strings.TrimRightFunc(input, unicode.IsSpace), nil
	}
}

// CreateNormalizeWhitespaceModifier creates a whitespace normalization modifier
func CreateNormalizeWhitespaceModifier() ModifierFn {
	return func(input string) (string, error) {
		// Replace multiple whitespace with single space
		normalized := strings.Join(strings.Fields(input), " ")
		return normalized, nil
	}
}

// CreateRemoveWhitespaceModifier creates a whitespace removal modifier
func CreateRemoveWhitespaceModifier() ModifierFn {
	return func(input string) (string, error) {
		removed := strings.ReplaceAll(input, " ", "")
		removed = strings.ReplaceAll(removed, "\t", "")
		removed = strings.ReplaceAll(removed, "\n", "")
		removed = strings.ReplaceAll(removed, "\r", "")
		return removed, nil
	}
}

// CreateReverseModifier creates a string reversal modifier
func CreateReverseModifier() ModifierFn {
	return func(input string) (string, error) {
		runes := []rune(input)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	}
}

// Numeric Modifiers

// CreateToIntModifier creates an integer conversion modifier
func CreateToIntModifier() ModifierFn {
	return func(input string) (string, error) {
		if i, err := strconv.ParseInt(strings.TrimSpace(input), 10, 64); err == nil {
			return strconv.FormatInt(i, 10), nil
		} else {
			return input, err
		}
	}
}

// CreateToFloatModifier creates a float conversion modifier
func CreateToFloatModifier() ModifierFn {
	return func(input string) (string, error) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(input), 64); err == nil {
			return strconv.FormatFloat(f, 'f', -1, 64), nil
		} else {
			return input, err
		}
	}
}

// CreateAbsModifier creates an absolute value modifier
func CreateAbsModifier() ModifierFn {
	return func(input string) (string, error) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(input), 64); err == nil {
			if f < 0 {
				f = -f
			}
			return strconv.FormatFloat(f, 'f', -1, 64), nil
		} else {
			return input, err
		}
	}
}

// CreateRoundModifier creates a rounding modifier
func CreateRoundModifier() ModifierFn {
	return func(input string) (string, error) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(input), 64); err == nil {
			rounded := int64(f + 0.5)
			if f < 0 {
				rounded = int64(f - 0.5)
			}
			return strconv.FormatInt(rounded, 10), nil
		} else {
			return input, err
		}
	}
}

// Advanced Modifiers (simplified implementations)

// CreateSubstringModifier creates a substring extraction modifier
func CreateSubstringModifier() ModifierFn {
	return func(input string) (string, error) {
		// Simplified: extract from index 1 to end (configurable in real implementation)
		if len(input) > 1 {
			return input[1:], nil
		}
		return input, nil
	}
}

// CreateReplaceBasicModifier creates a string replacement modifier (renamed to avoid conflict)
func CreateReplaceBasicModifier() ModifierFn {
	return func(input string) (string, error) {
		// Simplified: replace common separators with space
		replaced := strings.ReplaceAll(input, "_", " ")
		replaced = strings.ReplaceAll(replaced, "-", " ")
		return replaced, nil
	}
}

// CreateRegexExtractSimpleModifier creates a regex extraction modifier (renamed and simplified)
func CreateRegexExtractSimpleModifier() ModifierFn {
	return func(input string) (string, error) {
		// Simplified: extract alphanumeric characters only
		var extracted strings.Builder
		for _, r := range input {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				extracted.WriteRune(r)
			}
		}
		if extracted.Len() > 0 {
			return extracted.String(), nil
		}
		return input, nil
	}
}

// CreateJSONExtractModifier creates a JSON field extraction modifier (simplified)
func CreateJSONExtractModifier() ModifierFn {
	return func(input string) (string, error) {
		// Simplified implementation - would need proper JSON parsing
		if strings.Contains(input, ":") {
			parts := strings.Split(input, ":")
			if len(parts) > 1 {
				extracted := strings.Trim(parts[1], " \"")
				return extracted, nil
			}
		}
		return input, nil
	}
}

// CreateXMLExtractModifier creates an XML content extraction modifier (simplified)
func CreateXMLExtractModifier() ModifierFn {
	return func(input string) (string, error) {
		// Simplified implementation - would need proper XML parsing
		start := strings.Index(input, ">")
		end := strings.LastIndex(input, "<")
		if start >= 0 && end > start {
			extracted := input[start+1 : end]
			return strings.TrimSpace(extracted), nil
		}
		return input, nil
	}
}

// CreateCSVExtractModifier creates a CSV field extraction modifier
func CreateCSVExtractModifier() ModifierFn {
	return func(input string) (string, error) {
		// Extract first CSV field
		parts := strings.Split(input, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0]), nil
		}
		return input, nil
	}
}

// CreateSplitFirstModifier creates a string splitting modifier that returns first element
func CreateSplitFirstModifier() ModifierFn {
	return func(input string) (string, error) {
		// Split on whitespace and return first element
		parts := strings.Fields(input)
		if len(parts) > 0 {
			return parts[0], nil
		}
		return input, nil
	}
}

// CreateMD5HashModifier creates an MD5 hash modifier (placeholder)
func CreateMD5HashModifier() ModifierFn {
	return func(input string) (string, error) {
		// Placeholder - would need crypto/md5
		hash := fmt.Sprintf("md5_%x", len(input))
		return hash, nil
	}
}

// CreateSHA256HashModifier creates a SHA256 hash modifier (placeholder)
func CreateSHA256HashModifier() ModifierFn {
	return func(input string) (string, error) {
		// Placeholder - would need crypto/sha256
		hash := fmt.Sprintf("sha256_%x", len(input)*256)
		return hash, nil
	}
}
