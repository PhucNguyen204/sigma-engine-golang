package compiler

import (
	"fmt"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/dag"
	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// DAGCodegen handles generating DAG structures from compiled SIGMA rules
type DAGCodegen struct {
	compiler *Compiler

	// DAG state
	currentDAG *dag.CompiledDag
	nodeIDMap  map[string]dag.NodeId
	nextNodeID dag.NodeId

	// Temporary state during generation
	selectionNodes map[string]dag.NodeId
	parser         *Parser
}

// NewDAGCodegen creates a new DAG code generator
func NewDAGCodegen(compiler *Compiler) *DAGCodegen {
	return &DAGCodegen{
		compiler:       compiler,
		nodeIDMap:      make(map[string]dag.NodeId),
		nextNodeID:     0,
		selectionNodes: make(map[string]dag.NodeId),
		parser:         nil, // Will be created when needed
	}
}

// GenerateDAG generates a DAG from compiled SIGMA rules and their conditions
func (g *DAGCodegen) GenerateDAG(rules []SigmaRule) (*dag.CompiledDag, error) {
	g.currentDAG = dag.NewCompiledDag()
	g.resetState()

	// Process each rule
	for _, rule := range rules {
		err := g.processRule(&rule)
		if err != nil {
			return nil, fmt.Errorf("failed to process rule %s: %w", rule.ID, err)
		}
	}

	return g.currentDAG, nil
}

// resetState clears internal state for new DAG generation
func (g *DAGCodegen) resetState() {
	g.nodeIDMap = make(map[string]dag.NodeId)
	g.nextNodeID = 0
	g.selectionNodes = make(map[string]dag.NodeId)
}

// processRule processes a single SIGMA rule and adds it to the DAG
func (g *DAGCodegen) processRule(rule *SigmaRule) error {
	// Extract condition from detection map
	conditionValue, exists := rule.Detection["condition"]
	if !exists {
		return fmt.Errorf("rule %s has no condition in detection", rule.ID)
	}

	condition, ok := conditionValue.(string)
	if !ok {
		return fmt.Errorf("rule %s condition is not a string", rule.ID)
	}

	// Parse the condition
	parser := NewParser(condition)
	ast, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse rule condition: %w", err)
	}

	// Generate selection nodes for this rule
	err = g.generateSelectionNodes(rule)
	if err != nil {
		return fmt.Errorf("failed to generate selection nodes: %w", err)
	}

	// Generate DAG from the condition AST
	conditionNodeID, err := g.generateFromAST(ast)
	if err != nil {
		return fmt.Errorf("failed to generate condition DAG: %w", err)
	}

	// Create result node for this rule
	ruleID := ir.RuleID(hashString(rule.ID))
	resultNodeType := dag.NewResultNodeType(ruleID)
	resultNode := dag.NewDagNode(g.getNextNodeID(), resultNodeType)
	resultNode.AddDependency(conditionNodeID)

	resultNodeID := g.currentDAG.AddNode(*resultNode)
	g.currentDAG.RuleResults[ruleID] = resultNodeID

	return nil
}

// generateSelectionNodes creates DAG nodes for all selections in a rule
func (g *DAGCodegen) generateSelectionNodes(rule *SigmaRule) error {
	// Iterate through detection map to find selections
	for key, value := range rule.Detection {
		// Skip special keys like "condition"
		if key == "condition" {
			continue
		}

		selectionNodeID := g.getNextNodeID()

		// Convert selection value to map
		selectionMap, ok := value.(map[string]interface{})
		if !ok {
			// Handle simple string selections or other types
			continue
		}

		// Generate primitives for this selection
		primitives, err := g.processSigmaSelection(selectionMap)
		if err != nil {
			return fmt.Errorf("failed to process selection %s: %w", key, err)
		}

		if len(primitives) == 1 {
			// Single primitive - create primitive node directly
			primitive := primitives[0]
			primitiveID := ir.PrimitiveID(primitive.Hash())

			primNodeType := dag.NewPrimitiveNodeType(primitiveID)
			primNode := dag.NewDagNode(selectionNodeID, primNodeType)
			g.currentDAG.AddNode(*primNode)
		} else if len(primitives) > 1 {
			// Multiple primitives - create OR node with primitive dependencies
			orNodeType := dag.NewLogicalNodeType(dag.LogicalOr)
			orNode := dag.NewDagNode(selectionNodeID, orNodeType)

			for _, primitive := range primitives {
				primitiveID := ir.PrimitiveID(primitive.Hash())
				primNodeID := g.getNextNodeID()

				primNodeType := dag.NewPrimitiveNodeType(primitiveID)
				primNode := dag.NewDagNode(primNodeID, primNodeType)
				g.currentDAG.AddNode(*primNode)

				orNode.AddDependency(primNodeID)
			}

			g.currentDAG.AddNode(*orNode)
		}

		g.selectionNodes[key] = selectionNodeID
	}

	return nil
}

// processSigmaSelection converts a SIGMA selection to primitives
func (g *DAGCodegen) processSigmaSelection(selection map[string]interface{}) ([]*ir.Primitive, error) {
	var primitives []*ir.Primitive

	for field, value := range selection {
		switch v := value.(type) {
		case string:
			// Single value
			primitive := ir.NewPrimitive(field, "equals", []string{v}, []string{})
			primitives = append(primitives, primitive)
		case []interface{}:
			// Multiple values (OR condition)
			var values []string
			for _, item := range v {
				if str, ok := item.(string); ok {
					values = append(values, str)
				}
			}
			if len(values) > 0 {
				primitive := ir.NewPrimitive(field, "in", values, []string{})
				primitives = append(primitives, primitive)
			}
		case map[string]interface{}:
			// Complex field with modifiers/operators
			for op, opValue := range v {
				switch op {
				case "contains":
					if str, ok := opValue.(string); ok {
						primitive := ir.NewPrimitive(field, "contains", []string{str}, []string{})
						primitives = append(primitives, primitive)
					}
				case "startswith":
					if str, ok := opValue.(string); ok {
						primitive := ir.NewPrimitive(field, "startswith", []string{str}, []string{})
						primitives = append(primitives, primitive)
					}
				case "endswith":
					if str, ok := opValue.(string); ok {
						primitive := ir.NewPrimitive(field, "endswith", []string{str}, []string{})
						primitives = append(primitives, primitive)
					}
				default:
					// Default to equals
					if str, ok := opValue.(string); ok {
						primitive := ir.NewPrimitive(field, "equals", []string{str}, []string{})
						primitives = append(primitives, primitive)
					}
				}
			}
		}
	}

	return primitives, nil
}

// generateFromAST recursively generates DAG nodes from an AST node
func (g *DAGCodegen) generateFromAST(node ASTNode) (dag.NodeId, error) {
	switch n := node.(type) {
	case *IdentifierNode:
		return g.generateIdentifierNode(n)
	case *BinaryOpNode:
		return g.generateBinaryOpNode(n)
	case *UnaryOpNode:
		return g.generateUnaryOpNode(n)
	case *QuantifierNode:
		return g.generateQuantifierNode(n)
	default:
		return 0, fmt.Errorf("unknown AST node type: %T", node)
	}
}

// generateIdentifierNode creates a DAG node for an identifier (selection reference)
func (g *DAGCodegen) generateIdentifierNode(node *IdentifierNode) (dag.NodeId, error) {
	// Look up the selection node
	if nodeID, exists := g.selectionNodes[node.Name]; exists {
		return nodeID, nil
	}

	return 0, fmt.Errorf("undefined selection: %s", node.Name)
}

// generateBinaryOpNode creates a DAG node for a binary operation (AND, OR)
func (g *DAGCodegen) generateBinaryOpNode(node *BinaryOpNode) (dag.NodeId, error) {
	leftNodeID, err := g.generateFromAST(node.Left)
	if err != nil {
		return 0, err
	}

	rightNodeID, err := g.generateFromAST(node.Right)
	if err != nil {
		return 0, err
	}

	var nodeType dag.NodeType
	switch node.Operator {
	case "and":
		nodeType = dag.NewLogicalNodeType(dag.LogicalAnd)
	case "or":
		nodeType = dag.NewLogicalNodeType(dag.LogicalOr)
	default:
		return 0, fmt.Errorf("unknown binary operator: %s", node.Operator)
	}

	dagNode := dag.NewDagNode(g.getNextNodeID(), nodeType)
	dagNode.AddDependency(leftNodeID)
	dagNode.AddDependency(rightNodeID)

	return g.currentDAG.AddNode(*dagNode), nil
}

// generateUnaryOpNode creates a DAG node for a unary operation (NOT)
func (g *DAGCodegen) generateUnaryOpNode(node *UnaryOpNode) (dag.NodeId, error) {
	operandNodeID, err := g.generateFromAST(node.Operand)
	if err != nil {
		return 0, err
	}

	var nodeType dag.NodeType
	switch node.Operator {
	case "not":
		nodeType = dag.NewLogicalNodeType(dag.LogicalNot)
	default:
		return 0, fmt.Errorf("unknown unary operator: %s", node.Operator)
	}

	dagNode := dag.NewDagNode(g.getNextNodeID(), nodeType)
	dagNode.AddDependency(operandNodeID)

	return g.currentDAG.AddNode(*dagNode), nil
}

// generateQuantifierNode creates a DAG node for a quantifier (all, any, count)
func (g *DAGCodegen) generateQuantifierNode(node *QuantifierNode) (dag.NodeId, error) {
	if len(node.Selections) == 0 {
		return 0, fmt.Errorf("quantifier node requires at least one selection")
	}

	var childNodes []dag.NodeId
	for _, selName := range node.Selections {
		if nodeID, exists := g.selectionNodes[selName]; exists {
			childNodes = append(childNodes, nodeID)
		} else {
			return 0, fmt.Errorf("undefined selection in quantifier: %s", selName)
		}
	}

	var nodeType dag.NodeType

	if node.Count == -1 {
		// "all" - logical AND
		nodeType = dag.NewLogicalNodeType(dag.LogicalAnd)
	} else if node.Count == 1 {
		// "1 of" - logical OR
		nodeType = dag.NewLogicalNodeType(dag.LogicalOr)
	} else {
		// "N of" - for now, treat as AND (simplified)
		// TODO: Implement proper counting logic
		nodeType = dag.NewLogicalNodeType(dag.LogicalAnd)
	}

	dagNode := dag.NewDagNode(g.getNextNodeID(), nodeType)

	for _, childNodeID := range childNodes {
		dagNode.AddDependency(childNodeID)
	}

	return g.currentDAG.AddNode(*dagNode), nil
}

// getNextNodeID returns the next available node ID
func (g *DAGCodegen) getNextNodeID() dag.NodeId {
	id := g.nextNodeID
	g.nextNodeID++
	return id
}

// hashString creates a simple hash from a string for ID generation
func hashString(s string) uint32 {
	h := uint32(0)
	for _, c := range s {
		h = h*31 + uint32(c)
	}
	return h
}
