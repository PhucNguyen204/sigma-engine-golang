// Package compiler provides SIGMA condition expression parsing.
//
// This module provides tokenization and parsing of SIGMA condition expressions
// into an Abstract Syntax Tree (AST) for bytecode generation.
package compiler

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// Token represents tokens in a SIGMA condition expression.
type Token int

const (
	TokenIdentifier Token = iota
	TokenAnd
	TokenOr
	TokenNot
	TokenLeftParen
	TokenRightParen
	TokenOf
	TokenThem
	TokenAll
	TokenNumber
	TokenWildcard
)

// TokenValue represents a token with its associated value.
type TokenValue struct {
	Type   Token
	Value  string
	Number uint32
}

// ConditionAst represents the AST for SIGMA condition expressions.
type ConditionAst interface {
	String() string
}

// Identifier represents an identifier node.
type Identifier struct {
	Name string
}

func (i *Identifier) String() string {
	return i.Name
}

// And represents an AND operation.
type And struct {
	Left  ConditionAst
	Right ConditionAst
}

func (a *And) String() string {
	return fmt.Sprintf("(%s and %s)", a.Left.String(), a.Right.String())
}

// Or represents an OR operation.
type Or struct {
	Left  ConditionAst
	Right ConditionAst
}

func (o *Or) String() string {
	return fmt.Sprintf("(%s or %s)", o.Left.String(), o.Right.String())
}

// Not represents a NOT operation.
type Not struct {
	Operand ConditionAst
}

func (n *Not) String() string {
	return fmt.Sprintf("not %s", n.Operand.String())
}

// OneOfThem represents "1 of them".
type OneOfThem struct{}

func (o *OneOfThem) String() string {
	return "1 of them"
}

// AllOfThem represents "all of them".
type AllOfThem struct{}

func (a *AllOfThem) String() string {
	return "all of them"
}

// OneOfPattern represents "1 of pattern".
type OneOfPattern struct {
	Pattern string
}

func (o *OneOfPattern) String() string {
	return fmt.Sprintf("1 of %s", o.Pattern)
}

// AllOfPattern represents "all of pattern".
type AllOfPattern struct {
	Pattern string
}

func (a *AllOfPattern) String() string {
	return fmt.Sprintf("all of %s", a.Pattern)
}

// CountOfPattern represents "N of pattern".
type CountOfPattern struct {
	Count   uint32
	Pattern string
}

func (c *CountOfPattern) String() string {
	return fmt.Sprintf("%d of %s", c.Count, c.Pattern)
}

// ConditionParser represents a recursive descent parser for SIGMA conditions.
type ConditionParser struct {
	tokens       []TokenValue
	position     int
	selectionMap map[string][]ir.PrimitiveID
}

// NewConditionParser creates a new condition parser.
func NewConditionParser(tokens []TokenValue, selectionMap map[string][]ir.PrimitiveID) *ConditionParser {
	return &ConditionParser{
		tokens:       tokens,
		position:     0,
		selectionMap: selectionMap,
	}
}

func (p *ConditionParser) currentToken() *TokenValue {
	if p.position < len(p.tokens) {
		return &p.tokens[p.position]
	}
	return nil
}

func (p *ConditionParser) advance() *TokenValue {
	token := p.currentToken()
	if token != nil {
		p.position++
	}
	return token
}

// ParseOrExpression parses OR expressions (lowest precedence).
func (p *ConditionParser) ParseOrExpression() (ConditionAst, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.currentToken() != nil && p.currentToken().Type == TokenOr {
		p.advance()
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		left = &Or{Left: left, Right: right}
	}

	return left, nil
}

// parseAndExpression parses AND expressions (medium precedence).
func (p *ConditionParser) parseAndExpression() (ConditionAst, error) {
	left, err := p.parseNotExpression()
	if err != nil {
		return nil, err
	}

	for p.currentToken() != nil && p.currentToken().Type == TokenAnd {
		p.advance()
		right, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}
		left = &And{Left: left, Right: right}
	}

	return left, nil
}

// parseNotExpression parses NOT expressions (highest precedence).
func (p *ConditionParser) parseNotExpression() (ConditionAst, error) {
	if p.currentToken() != nil && p.currentToken().Type == TokenNot {
		p.advance()
		operand, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &Not{Operand: operand}, nil
	}
	return p.parsePrimary()
}

// parsePrimary parses primary expressions.
func (p *ConditionParser) parsePrimary() (ConditionAst, error) {
	token := p.currentToken()
	if token == nil {
		return nil, fmt.Errorf("unexpected end of tokens")
	}

	switch token.Type {
	case TokenLeftParen:
		p.advance()
		expr, err := p.ParseOrExpression()
		if err != nil {
			return nil, err
		}
		if p.currentToken() == nil || p.currentToken().Type != TokenRightParen {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		p.advance()
		return expr, nil

	case TokenIdentifier:
		name := token.Value
		p.advance()

		if _, exists := p.selectionMap[name]; exists {
			return &Identifier{Name: name}, nil
		}
		return nil, fmt.Errorf("unknown selection identifier: %s", name)

	case TokenNumber:
		count := token.Number
		p.advance()

		if p.currentToken() == nil || p.currentToken().Type != TokenOf {
			return nil, fmt.Errorf("expected 'of' after number")
		}
		p.advance()

		nextToken := p.currentToken()
		if nextToken == nil {
			return nil, fmt.Errorf("expected 'them' or pattern after 'of'")
		}

		switch nextToken.Type {
		case TokenThem:
			p.advance()
			if count == 1 {
				return &OneOfThem{}, nil
			}
			return nil, fmt.Errorf("only '1 of them' is supported")

		case TokenWildcard:
			pattern := nextToken.Value
			p.advance()
			return &CountOfPattern{Count: count, Pattern: pattern}, nil

		default:
			return nil, fmt.Errorf("expected 'them' or pattern after 'of'")
		}

	case TokenAll:
		p.advance()

		if p.currentToken() == nil || p.currentToken().Type != TokenOf {
			return nil, fmt.Errorf("expected 'of' after 'all'")
		}
		p.advance()

		nextToken := p.currentToken()
		if nextToken == nil {
			return nil, fmt.Errorf("expected 'them' or pattern after 'of'")
		}

		switch nextToken.Type {
		case TokenThem:
			p.advance()
			return &AllOfThem{}, nil

		case TokenWildcard:
			pattern := nextToken.Value
			p.advance()
			return &AllOfPattern{Pattern: pattern}, nil

		default:
			return nil, fmt.Errorf("expected 'them' or pattern after 'of'")
		}

	default:
		return nil, fmt.Errorf("unexpected token in condition")
	}
}

// TokenizeCondition tokenizes a SIGMA condition string.
func TokenizeCondition(condition string) ([]TokenValue, error) {
	var tokens []TokenValue
	runes := []rune(condition)
	i := 0

	for i < len(runes) {
		ch := runes[i]

		// Skip whitespace
		if unicode.IsSpace(ch) {
			i++
			continue
		}

		switch ch {
		case '(':
			tokens = append(tokens, TokenValue{Type: TokenLeftParen})
			i++

		case ')':
			tokens = append(tokens, TokenValue{Type: TokenRightParen})
			i++

		default:
			if unicode.IsDigit(ch) {
				// Parse number
				start := i
				for i < len(runes) && unicode.IsDigit(runes[i]) {
					i++
				}
				numberStr := string(runes[start:i])
				if num, err := strconv.ParseUint(numberStr, 10, 32); err == nil {
					tokens = append(tokens, TokenValue{Type: TokenNumber, Number: uint32(num)})
				}

			} else if unicode.IsLetter(ch) || ch == '_' {
				// Parse identifier/keyword
				start := i
				for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_' || runes[i] == '*') {
					i++
				}
				identifier := string(runes[start:i])

				switch identifier {
				case "and":
					tokens = append(tokens, TokenValue{Type: TokenAnd})
				case "or":
					tokens = append(tokens, TokenValue{Type: TokenOr})
				case "not":
					tokens = append(tokens, TokenValue{Type: TokenNot})
				case "of":
					tokens = append(tokens, TokenValue{Type: TokenOf})
				case "them":
					tokens = append(tokens, TokenValue{Type: TokenThem})
				case "all":
					tokens = append(tokens, TokenValue{Type: TokenAll})
				default:
					if strings.Contains(identifier, "*") {
						tokens = append(tokens, TokenValue{Type: TokenWildcard, Value: identifier})
					} else {
						tokens = append(tokens, TokenValue{Type: TokenIdentifier, Value: identifier})
					}
				}

			} else {
				return nil, fmt.Errorf("unexpected character in condition: '%c'", ch)
			}
		}
	}

	return tokens, nil
}

// ParseTokens parses tokens into an AST.
func ParseTokens(tokens []TokenValue, selectionMap map[string][]ir.PrimitiveID) (ConditionAst, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty condition")
	}

	parser := NewConditionParser(tokens, selectionMap)
	return parser.ParseOrExpression()
}
