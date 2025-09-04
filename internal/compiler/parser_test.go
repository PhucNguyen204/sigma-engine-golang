package compiler

import (
	"testing"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

func createTestSelectionMap() map[string][]ir.PrimitiveID {
	return map[string][]ir.PrimitiveID{
		"selection1": {0},
		"selection2": {1},
		"selection3": {2},
	}
}

// TestTokenizeSimpleIdentifier matches Rust test_tokenize_simple_identifier
func TestTokenizeSimpleIdentifier(t *testing.T) {
	tokens, err := TokenizeCondition("selection1")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 1 {
		t.Errorf("Expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Type != TokenIdentifier || tokens[0].Value != "selection1" {
		t.Errorf("Expected identifier 'selection1', got %v", tokens[0])
	}
}

// TestTokenizeAndExpression matches Rust test_tokenize_and_expression
func TestTokenizeAndExpression(t *testing.T) {
	tokens, err := TokenizeCondition("selection1 and selection2")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenIdentifier {
		t.Errorf("Expected identifier, got %v", tokens[0].Type)
	}
	if tokens[1].Type != TokenAnd {
		t.Errorf("Expected AND, got %v", tokens[1].Type)
	}
	if tokens[2].Type != TokenIdentifier {
		t.Errorf("Expected identifier, got %v", tokens[2].Type)
	}
}

// TestTokenizeOrExpression matches Rust test_tokenize_or_expression
func TestTokenizeOrExpression(t *testing.T) {
	tokens, err := TokenizeCondition("selection1 or selection2")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenIdentifier {
		t.Errorf("Expected identifier, got %v", tokens[0].Type)
	}
	if tokens[1].Type != TokenOr {
		t.Errorf("Expected OR, got %v", tokens[1].Type)
	}
	if tokens[2].Type != TokenIdentifier {
		t.Errorf("Expected identifier, got %v", tokens[2].Type)
	}
}

// TestTokenizeNotExpression matches Rust test_tokenize_not_expression
func TestTokenizeNotExpression(t *testing.T) {
	tokens, err := TokenizeCondition("not selection1")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 2 {
		t.Errorf("Expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenNot {
		t.Errorf("Expected NOT, got %v", tokens[0].Type)
	}
	if tokens[1].Type != TokenIdentifier {
		t.Errorf("Expected identifier, got %v", tokens[1].Type)
	}
}

// TestTokenizeParentheses matches Rust test_tokenize_parentheses
func TestTokenizeParentheses(t *testing.T) {
	tokens, err := TokenizeCondition("(selection1 and selection2)")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 5 {
		t.Errorf("Expected 5 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenLeftParen {
		t.Errorf("Expected left paren, got %v", tokens[0].Type)
	}
	if tokens[4].Type != TokenRightParen {
		t.Errorf("Expected right paren, got %v", tokens[4].Type)
	}
}

// TestTokenizeNumbers matches Rust test_tokenize_numbers
func TestTokenizeNumbers(t *testing.T) {
	tokens, err := TokenizeCondition("2 of selection*")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenNumber || tokens[0].Number != 2 {
		t.Errorf("Expected number 2, got %v", tokens[0])
	}
	if tokens[1].Type != TokenOf {
		t.Errorf("Expected OF, got %v", tokens[1].Type)
	}
	if tokens[2].Type != TokenWildcard {
		t.Errorf("Expected wildcard, got %v", tokens[2].Type)
	}
}

// TestTokenizeWildcard matches Rust test_tokenize_wildcard
func TestTokenizeWildcard(t *testing.T) {
	tokens, err := TokenizeCondition("selection*")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 1 {
		t.Errorf("Expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Type != TokenWildcard || tokens[0].Value != "selection*" {
		t.Errorf("Expected wildcard 'selection*', got %v", tokens[0])
	}
}

// TestTokenizeAllOfThem matches Rust test_tokenize_all_of_them
func TestTokenizeAllOfThem(t *testing.T) {
	tokens, err := TokenizeCondition("all of them")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenAll {
		t.Errorf("Expected ALL, got %v", tokens[0].Type)
	}
	if tokens[1].Type != TokenOf {
		t.Errorf("Expected OF, got %v", tokens[1].Type)
	}
	if tokens[2].Type != TokenThem {
		t.Errorf("Expected THEM, got %v", tokens[2].Type)
	}
}

// TestTokenizeOneOfThem matches Rust test_tokenize_one_of_them
func TestTokenizeOneOfThem(t *testing.T) {
	tokens, err := TokenizeCondition("1 of them")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenNumber || tokens[0].Number != 1 {
		t.Errorf("Expected number 1, got %v", tokens[0])
	}
	if tokens[1].Type != TokenOf {
		t.Errorf("Expected OF, got %v", tokens[1].Type)
	}
	if tokens[2].Type != TokenThem {
		t.Errorf("Expected THEM, got %v", tokens[2].Type)
	}
}

// TestTokenizeInvalidCharacter matches Rust test_tokenize_invalid_character
func TestTokenizeInvalidCharacter(t *testing.T) {
	_, err := TokenizeCondition("selection1 @ selection2")
	if err == nil {
		t.Error("Expected error for invalid character")
	}
	if err != nil && !contains(err.Error(), "Unexpected character") {
		t.Errorf("Expected 'Unexpected character' error, got: %v", err)
	}
}

// TestTokenizeWhitespaceHandling matches Rust test_tokenize_whitespace_handling
func TestTokenizeWhitespaceHandling(t *testing.T) {
	tokens, err := TokenizeCondition("  selection1   and   selection2  ")
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	if len(tokens) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(tokens))
	}
}

// TestParseSimpleIdentifier matches Rust test_parse_simple_identifier
func TestParseSimpleIdentifier(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenIdentifier, Value: "selection1"},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if identifier, ok := ast.(*Identifier); !ok || identifier.Name != "selection1" {
		t.Errorf("Expected identifier 'selection1', got %v", ast)
	}
}

// TestParseAndExpression matches Rust test_parse_and_expression
func TestParseAndExpression(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenIdentifier, Value: "selection1"},
		{Type: TokenAnd},
		{Type: TokenIdentifier, Value: "selection2"},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if _, ok := ast.(*And); !ok {
		t.Errorf("Expected AND expression, got %T", ast)
	}
}

// TestParseOrExpression matches Rust test_parse_or_expression
func TestParseOrExpression(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenIdentifier, Value: "selection1"},
		{Type: TokenOr},
		{Type: TokenIdentifier, Value: "selection2"},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if _, ok := ast.(*Or); !ok {
		t.Errorf("Expected OR expression, got %T", ast)
	}
}

// TestParseNotExpression matches Rust test_parse_not_expression
func TestParseNotExpression(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenNot},
		{Type: TokenIdentifier, Value: "selection1"},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if _, ok := ast.(*Not); !ok {
		t.Errorf("Expected NOT expression, got %T", ast)
	}
}

// TestParseParentheses matches Rust test_parse_parentheses
func TestParseParentheses(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenLeftParen},
		{Type: TokenIdentifier, Value: "selection1"},
		{Type: TokenAnd},
		{Type: TokenIdentifier, Value: "selection2"},
		{Type: TokenRightParen},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if _, ok := ast.(*And); !ok {
		t.Errorf("Expected AND expression, got %T", ast)
	}
}

// TestParseAllOfThem matches Rust test_parse_all_of_them
func TestParseAllOfThem(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenAll},
		{Type: TokenOf},
		{Type: TokenThem},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if _, ok := ast.(*AllOfThem); !ok {
		t.Errorf("Expected AllOfThem expression, got %T", ast)
	}
}

// TestParseOneOfThem matches Rust test_parse_one_of_them
func TestParseOneOfThem(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenNumber, Number: 1},
		{Type: TokenOf},
		{Type: TokenThem},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if _, ok := ast.(*OneOfThem); !ok {
		t.Errorf("Expected OneOfThem expression, got %T", ast)
	}
}

// TestParseCountOfPattern matches Rust test_parse_count_of_pattern
func TestParseCountOfPattern(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenNumber, Number: 2},
		{Type: TokenOf},
		{Type: TokenWildcard, Value: "selection*"},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if countAst, ok := ast.(*CountOfPattern); !ok || countAst.Count != 2 || countAst.Pattern != "selection*" {
		t.Errorf("Expected CountOfPattern(2, 'selection*'), got %v", ast)
	}
}

// TestParseAllOfPattern matches Rust test_parse_all_of_pattern
func TestParseAllOfPattern(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenAll},
		{Type: TokenOf},
		{Type: TokenWildcard, Value: "selection*"},
	}
	selectionMap := createTestSelectionMap()

	ast, err := ParseTokens(tokens, selectionMap)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if allAst, ok := ast.(*AllOfPattern); !ok || allAst.Pattern != "selection*" {
		t.Errorf("Expected AllOfPattern('selection*'), got %v", ast)
	}
}

// TestParseEmptyTokens matches Rust test_parse_empty_tokens
func TestParseEmptyTokens(t *testing.T) {
	tokens := []TokenValue{}
	selectionMap := createTestSelectionMap()

	_, err := ParseTokens(tokens, selectionMap)
	if err == nil {
		t.Error("Expected error for empty tokens")
	}
	if err != nil && !contains(err.Error(), "empty condition") {
		t.Errorf("Expected 'empty condition' error, got: %v", err)
	}
}

// TestParseMissingClosingParenthesis matches Rust test_parse_missing_closing_parenthesis
func TestParseMissingClosingParenthesis(t *testing.T) {
	tokens := []TokenValue{
		{Type: TokenLeftParen},
		{Type: TokenIdentifier, Value: "selection1"},
		{Type: TokenAnd},
		{Type: TokenIdentifier, Value: "selection2"},
		// Missing RightParen
	}
	selectionMap := createTestSelectionMap()

	_, err := ParseTokens(tokens, selectionMap)
	if err == nil {
		t.Error("Expected error for missing closing parenthesis")
	}
	if err != nil && !contains(err.Error(), "expected closing parenthesis") {
		t.Errorf("Expected 'expected closing parenthesis' error, got: %v", err)
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
