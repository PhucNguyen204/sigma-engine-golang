package compiler

import (
	"fmt"
	"strconv"
	"strings"
)

// TokenType represents different types of tokens in SIGMA conditions
type TokenType int

const (
	TokenUnknown TokenType = iota
	TokenIdentifier
	TokenKeyword
	TokenOperator
	TokenLeftParen
	TokenRightParen
	TokenPipe
	TokenNumber
	TokenString
	TokenEOF
)

// Token represents a single token in the condition expression
type Token struct {
	Type     TokenType
	Value    string
	Position int
}

// String returns a string representation of the token
func (t Token) String() string {
	return fmt.Sprintf("Token{type=%v, value='%s', pos=%d}", t.Type, t.Value, t.Position)
}

// Lexer tokenizes SIGMA condition expressions
type Lexer struct {
	input    string
	position int
	current  rune
}

// NewLexer creates a new lexer for the given input
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:    input,
		position: 0,
	}
	l.readChar()
	return l
}

// readChar reads the next character and advances position
func (l *Lexer) readChar() {
	if l.position >= len(l.input) {
		l.current = 0 // EOF
	} else {
		l.current = rune(l.input[l.position])
	}
	l.position++
}

// peekChar returns the next character without advancing position
func (l *Lexer) peekChar() rune {
	if l.position >= len(l.input) {
		return 0
	}
	return rune(l.input[l.position])
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.current == ' ' || l.current == '\t' || l.current == '\n' || l.current == '\r' {
		l.readChar()
	}
}

// readIdentifier reads an identifier or keyword
func (l *Lexer) readIdentifier() string {
	start := l.position - 1
	for isLetter(l.current) || isDigit(l.current) || l.current == '_' {
		l.readChar()
	}
	return l.input[start : l.position-1]
}

// readNumber reads a numeric literal
func (l *Lexer) readNumber() string {
	start := l.position - 1
	for isDigit(l.current) {
		l.readChar()
	}
	return l.input[start : l.position-1]
}

// readString reads a quoted string literal
func (l *Lexer) readString() string {
	start := l.position // Skip opening quote
	l.readChar()
	
	for l.current != '"' && l.current != 0 {
		if l.current == '\\' {
			l.readChar() // Skip escape character
		}
		l.readChar()
	}
	
	if l.current == '"' {
		result := l.input[start : l.position-1]
		l.readChar() // Skip closing quote
		return result
	}
	
	// Unterminated string
	return l.input[start : l.position-1]
}

// NextToken returns the next token in the input
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	
	pos := l.position - 1
	
	switch l.current {
	case 0:
		return Token{TokenEOF, "", pos}
	case '(':
		l.readChar()
		return Token{TokenLeftParen, "(", pos}
	case ')':
		l.readChar()
		return Token{TokenRightParen, ")", pos}
	case '|':
		l.readChar()
		return Token{TokenPipe, "|", pos}
	case '"':
		value := l.readString()
		return Token{TokenString, value, pos}
	default:
		if isLetter(l.current) {
			value := l.readIdentifier()
			tokenType := lookupIdentifierType(value)
			return Token{tokenType, value, pos}
		} else if isDigit(l.current) {
			value := l.readNumber()
			return Token{TokenNumber, value, pos}
		} else {
			// Handle operators and unknown characters
			ch := l.current
			l.readChar()
			return Token{TokenUnknown, string(ch), pos}
		}
	}
}

// TokenizeAll returns all tokens from the input
func (l *Lexer) TokenizeAll() []Token {
	var tokens []Token
	
	for {
		token := l.NextToken()
		tokens = append(tokens, token)
		if token.Type == TokenEOF {
			break
		}
	}
	
	return tokens
}

// lookupIdentifierType determines if an identifier is a keyword or regular identifier
func lookupIdentifierType(ident string) TokenType {
	keywords := map[string]TokenType{
		"and":   TokenKeyword,
		"or":    TokenKeyword,
		"not":   TokenKeyword,
		"AND":   TokenKeyword,
		"OR":    TokenKeyword,
		"NOT":   TokenKeyword,
		"of":    TokenKeyword,
		"all":   TokenKeyword,
		"them": TokenKeyword,
	}
	
	if tokenType, ok := keywords[ident]; ok {
		return tokenType
	}
	
	return TokenIdentifier
}

// isLetter checks if a character is a letter
func isLetter(ch rune) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}

// isDigit checks if a character is a digit
func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

// ASTNode represents a node in the abstract syntax tree
type ASTNode interface {
	String() string
	Accept(visitor ASTVisitor) interface{}
}

// ASTVisitor defines the visitor pattern interface for AST traversal
type ASTVisitor interface {
	VisitIdentifier(node *IdentifierNode) interface{}
	VisitBinaryOp(node *BinaryOpNode) interface{}
	VisitUnaryOp(node *UnaryOpNode) interface{}
	VisitGrouping(node *GroupingNode) interface{}
	VisitQuantifier(node *QuantifierNode) interface{}
}

// IdentifierNode represents an identifier (selection name)
type IdentifierNode struct {
	Name string
}

func (n *IdentifierNode) String() string {
	return n.Name
}

func (n *IdentifierNode) Accept(visitor ASTVisitor) interface{} {
	return visitor.VisitIdentifier(n)
}

// BinaryOpNode represents a binary operation (AND, OR)
type BinaryOpNode struct {
	Left     ASTNode
	Operator string
	Right    ASTNode
}

func (n *BinaryOpNode) String() string {
	return fmt.Sprintf("(%s %s %s)", n.Left.String(), n.Operator, n.Right.String())
}

func (n *BinaryOpNode) Accept(visitor ASTVisitor) interface{} {
	return visitor.VisitBinaryOp(n)
}

// UnaryOpNode represents a unary operation (NOT)
type UnaryOpNode struct {
	Operator string
	Operand  ASTNode
}

func (n *UnaryOpNode) String() string {
	return fmt.Sprintf("(%s %s)", n.Operator, n.Operand.String())
}

func (n *UnaryOpNode) Accept(visitor ASTVisitor) interface{} {
	return visitor.VisitUnaryOp(n)
}

// GroupingNode represents parenthesized expressions
type GroupingNode struct {
	Expression ASTNode
}

func (n *GroupingNode) String() string {
	return fmt.Sprintf("(%s)", n.Expression.String())
}

func (n *GroupingNode) Accept(visitor ASTVisitor) interface{} {
	return visitor.VisitGrouping(n)
}

// QuantifierNode represents quantified expressions (1 of them, all of them)
type QuantifierNode struct {
	Count      int    // -1 for "all"
	Selections []string
}

func (n *QuantifierNode) String() string {
	if n.Count == -1 {
		return fmt.Sprintf("all of (%s)", strings.Join(n.Selections, ", "))
	}
	return fmt.Sprintf("%d of (%s)", n.Count, strings.Join(n.Selections, ", "))
}

func (n *QuantifierNode) Accept(visitor ASTVisitor) interface{} {
	return visitor.VisitQuantifier(n)
}

// Parser parses SIGMA condition expressions into AST
type Parser struct {
	lexer   *Lexer
	current Token
	peek    Token
}

// NewParser creates a new parser for the given input
func NewParser(input string) *Parser {
	p := &Parser{
		lexer: NewLexer(input),
	}
	
	// Read two tokens to initialize current and peek
	p.nextToken()
	p.nextToken()
	
	return p
}

// nextToken advances to the next token
func (p *Parser) nextToken() {
	p.current = p.peek
	p.peek = p.lexer.NextToken()
}

// expectToken checks if current token matches expected type and advances
func (p *Parser) expectToken(expected TokenType) error {
	if p.current.Type != expected {
		return fmt.Errorf("expected token type %v, got %v at position %d",
			expected, p.current.Type, p.current.Position)
	}
	p.nextToken()
	return nil
}

// Parse parses the input into an AST
func (p *Parser) Parse() (ASTNode, error) {
	return p.parseExpression()
}

// parseExpression parses a complete expression
func (p *Parser) parseExpression() (ASTNode, error) {
	return p.parseOrExpression()
}

// parseOrExpression parses OR expressions (lowest precedence)
func (p *Parser) parseOrExpression() (ASTNode, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}
	
	for p.current.Type == TokenKeyword && strings.ToLower(p.current.Value) == "or" {
		operator := p.current.Value
		p.nextToken()
		
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		
		left = &BinaryOpNode{
			Left:     left,
			Operator: operator,
			Right:    right,
		}
	}
	
	return left, nil
}

// parseAndExpression parses AND expressions
func (p *Parser) parseAndExpression() (ASTNode, error) {
	left, err := p.parseUnaryExpression()
	if err != nil {
		return nil, err
	}
	
	for p.current.Type == TokenKeyword && strings.ToLower(p.current.Value) == "and" {
		operator := p.current.Value
		p.nextToken()
		
		right, err := p.parseUnaryExpression()
		if err != nil {
			return nil, err
		}
		
		left = &BinaryOpNode{
			Left:     left,
			Operator: operator,
			Right:    right,
		}
	}
	
	return left, nil
}

// parseUnaryExpression parses NOT expressions
func (p *Parser) parseUnaryExpression() (ASTNode, error) {
	if p.current.Type == TokenKeyword && strings.ToLower(p.current.Value) == "not" {
		operator := p.current.Value
		p.nextToken()
		
		operand, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		
		return &UnaryOpNode{
			Operator: operator,
			Operand:  operand,
		}, nil
	}
	
	return p.parsePrimaryExpression()
}

// parsePrimaryExpression parses primary expressions (identifiers, groups, quantifiers)
func (p *Parser) parsePrimaryExpression() (ASTNode, error) {
	switch p.current.Type {
	case TokenIdentifier:
		return p.parseIdentifierOrQuantifier()
	case TokenLeftParen:
		return p.parseGrouping()
	case TokenNumber:
		return p.parseQuantifier()
	default:
		return nil, fmt.Errorf("unexpected token %s at position %d",
			p.current.Value, p.current.Position)
	}
}

// parseIdentifierOrQuantifier parses identifier or checks for quantifier patterns
func (p *Parser) parseIdentifierOrQuantifier() (ASTNode, error) {
	name := p.current.Value
	p.nextToken()
	
	// Check for "of" keyword (quantifier pattern)
	if p.current.Type == TokenKeyword && strings.ToLower(p.current.Value) == "of" {
		// This is a quantifier starting with "all"
		if strings.ToLower(name) == "all" {
			return p.parseQuantifierRest(-1)
		}
		
		// Try to parse as a number
		if count, err := strconv.Atoi(name); err == nil {
			return p.parseQuantifierRest(count)
		}
		
		return nil, fmt.Errorf("invalid quantifier: %s", name)
	}
	
	return &IdentifierNode{Name: name}, nil
}

// parseQuantifier parses numeric quantifiers
func (p *Parser) parseQuantifier() (ASTNode, error) {
	countStr := p.current.Value
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, fmt.Errorf("invalid number: %s", countStr)
	}
	
	p.nextToken()
	
	if p.current.Type == TokenKeyword && strings.ToLower(p.current.Value) == "of" {
		return p.parseQuantifierRest(count)
	}
	
	return nil, fmt.Errorf("expected 'of' after number in quantifier")
}

// parseQuantifierRest parses the rest of a quantifier expression
func (p *Parser) parseQuantifierRest(count int) (ASTNode, error) {
	// Expect "of" keyword
	if err := p.expectToken(TokenKeyword); err != nil {
		return nil, err
	}
	
	// Parse selection list or "them"
	var selections []string
	
	if p.current.Type == TokenKeyword && strings.ToLower(p.current.Value) == "them" {
		selections = []string{"them"}
		p.nextToken()
	} else if p.current.Type == TokenLeftParen {
		// Parse parenthesized selection list
		p.nextToken() // consume '('
		
		for p.current.Type == TokenIdentifier {
			selections = append(selections, p.current.Value)
			p.nextToken()
			
			// Skip commas if present
			if p.current.Type == TokenUnknown && p.current.Value == "," {
				p.nextToken()
			}
		}
		
		if err := p.expectToken(TokenRightParen); err != nil {
			return nil, err
		}
	} else {
		// Single selection
		if p.current.Type != TokenIdentifier {
			return nil, fmt.Errorf("expected selection name at position %d", p.current.Position)
		}
		selections = []string{p.current.Value}
		p.nextToken()
	}
	
	return &QuantifierNode{
		Count:      count,
		Selections: selections,
	}, nil
}

// parseGrouping parses parenthesized expressions
func (p *Parser) parseGrouping() (ASTNode, error) {
	if err := p.expectToken(TokenLeftParen); err != nil {
		return nil, err
	}
	
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	
	if err := p.expectToken(TokenRightParen); err != nil {
		return nil, err
	}
	
	return &GroupingNode{Expression: expr}, nil
}

// ParseCondition is a convenience function to parse a SIGMA condition string
func ParseCondition(condition string) (ASTNode, error) {
	parser := NewParser(condition)
	return parser.Parse()
}

// ValidateCondition validates a SIGMA condition syntax
func ValidateCondition(condition string) error {
	_, err := ParseCondition(condition)
	return err
}

// NormalizeCondition normalizes a SIGMA condition by parsing and regenerating it
func NormalizeCondition(condition string) (string, error) {
	ast, err := ParseCondition(condition)
	if err != nil {
		return "", err
	}
	return ast.String(), nil
}

// ExtractSelections extracts all selection names referenced in a condition
func ExtractSelections(condition string) ([]string, error) {
	ast, err := ParseCondition(condition)
	if err != nil {
		return nil, err
	}
	
	extractor := &SelectionExtractor{
		selections: make(map[string]bool),
	}
	
	ast.Accept(extractor)
	
	var result []string
	for selection := range extractor.selections {
		if selection != "them" { // Exclude special keywords
			result = append(result, selection)
		}
	}
	
	return result, nil
}

// SelectionExtractor implements ASTVisitor to extract selection names
type SelectionExtractor struct {
	selections map[string]bool
}

func (e *SelectionExtractor) VisitIdentifier(node *IdentifierNode) interface{} {
	e.selections[node.Name] = true
	return nil
}

func (e *SelectionExtractor) VisitBinaryOp(node *BinaryOpNode) interface{} {
	node.Left.Accept(e)
	node.Right.Accept(e)
	return nil
}

func (e *SelectionExtractor) VisitUnaryOp(node *UnaryOpNode) interface{} {
	node.Operand.Accept(e)
	return nil
}

func (e *SelectionExtractor) VisitGrouping(node *GroupingNode) interface{} {
	node.Expression.Accept(e)
	return nil
}

func (e *SelectionExtractor) VisitQuantifier(node *QuantifierNode) interface{} {
	for _, selection := range node.Selections {
		e.selections[selection] = true
	}
	return nil
}

// IsValidCondition checks if a condition string has valid SIGMA syntax
func IsValidCondition(condition string) bool {
	return ValidateCondition(condition) == nil
}

// SimplifyCondition attempts to simplify boolean expressions in conditions
func SimplifyCondition(condition string) (string, error) {
	// This is a placeholder for condition simplification logic
	// In a full implementation, this would apply boolean algebra rules
	return NormalizeCondition(condition)
}

// ConditionComplexity estimates the complexity of a condition expression
func ConditionComplexity(condition string) (int, error) {
	ast, err := ParseCondition(condition)
	if err != nil {
		return 0, err
	}
	
	calculator := &ComplexityCalculator{}
	result := ast.Accept(calculator)
	
	if complexity, ok := result.(int); ok {
		return complexity, nil
	}
	
	return 0, fmt.Errorf("failed to calculate complexity")
}

// ComplexityCalculator implements ASTVisitor to calculate expression complexity
type ComplexityCalculator struct{}

func (c *ComplexityCalculator) VisitIdentifier(node *IdentifierNode) interface{} {
	return 1
}

func (c *ComplexityCalculator) VisitBinaryOp(node *BinaryOpNode) interface{} {
	left := node.Left.Accept(c).(int)
	right := node.Right.Accept(c).(int)
	return left + right + 1
}

func (c *ComplexityCalculator) VisitUnaryOp(node *UnaryOpNode) interface{} {
	operand := node.Operand.Accept(c).(int)
	return operand + 1
}

func (c *ComplexityCalculator) VisitGrouping(node *GroupingNode) interface{} {
	return node.Expression.Accept(c)
}

func (c *ComplexityCalculator) VisitQuantifier(node *QuantifierNode) interface{} {
	return len(node.Selections) + 1
}
