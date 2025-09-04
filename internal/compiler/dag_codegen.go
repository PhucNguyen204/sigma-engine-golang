package compiler

import (
	"fmt"
	"strings"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/dag"
	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// DagCodegenContext represents the context for DAG generation from AST
type DagCodegenContext struct {
	// Nodes being constructed
	nodes []dag.DagNode
	// Next available node ID
	nextNodeID dag.NodeId
	// Mapping from primitive IDs to their DAG nodes
	primitiveNodes map[ir.PrimitiveID]dag.NodeId
	// Current rule being compiled
	currentRuleID ir.RuleID
}

// NewDagCodegenContext creates a new DAG codegen context
func NewDagCodegenContext(ruleID ir.RuleID) *DagCodegenContext {
	return &DagCodegenContext{
		nodes:          make([]dag.DagNode, 0),
		nextNodeID:     0,
		primitiveNodes: make(map[ir.PrimitiveID]dag.NodeId),
		currentRuleID:  ruleID,
	}
}

// getOrCreatePrimitiveNode creates a new primitive node or reuses existing one
func (ctx *DagCodegenContext) getOrCreatePrimitiveNode(primitiveID ir.PrimitiveID) dag.NodeId {
	if existingNodeID, exists := ctx.primitiveNodes[primitiveID]; exists {
		return existingNodeID
	}

	nodeID := ctx.nextNodeID
	ctx.nextNodeID++

	nodeType := dag.NewPrimitiveNodeType(primitiveID)
	node := dag.NewDagNode(nodeID, nodeType)
	ctx.nodes = append(ctx.nodes, *node)
	ctx.primitiveNodes[primitiveID] = nodeID

	return nodeID
}

// createLogicalNode creates a new logical node
func (ctx *DagCodegenContext) createLogicalNode(operation dag.LogicalOp) dag.NodeId {
	nodeID := ctx.nextNodeID
	ctx.nextNodeID++

	nodeType := dag.NewLogicalNodeType(operation)
	node := dag.NewDagNode(nodeID, nodeType)
	ctx.nodes = append(ctx.nodes, *node)

	return nodeID
}

// createResultNode creates a new result node
func (ctx *DagCodegenContext) createResultNode(ruleID ir.RuleID) dag.NodeId {
	nodeID := ctx.nextNodeID
	ctx.nextNodeID++

	nodeType := dag.NewResultNodeType(ruleID)
	node := dag.NewDagNode(nodeID, nodeType)
	ctx.nodes = append(ctx.nodes, *node)

	return nodeID
}

// addDependency adds a dependency relationship between nodes
func (ctx *DagCodegenContext) addDependency(dependentID, dependencyID dag.NodeId) {
	// Add dependency to dependent node
	if int(dependentID) < len(ctx.nodes) {
		ctx.nodes[dependentID].AddDependency(dependencyID)
	}

	// Add dependent to dependency node
	if int(dependencyID) < len(ctx.nodes) {
		ctx.nodes[dependencyID].AddDependent(dependentID)
	}
}

// generateDagRecursive generates DAG nodes from AST recursively
func (ctx *DagCodegenContext) generateDagRecursive(
	ast ConditionAst,
	selectionMap map[string][]ir.PrimitiveID,
) (dag.NodeId, error) {
	switch node := ast.(type) {
	case *Identifier:
		// Look up the selection in the selection map
		primitiveIDs, exists := selectionMap[node.Name]
		if !exists {
			return 0, fmt.Errorf("unknown selection: %s", node.Name)
		}

		if len(primitiveIDs) == 0 {
			return 0, fmt.Errorf("empty selection: %s", node.Name)
		}

		if len(primitiveIDs) == 1 {
			// Single primitive - create or reuse primitive node
			return ctx.getOrCreatePrimitiveNode(primitiveIDs[0]), nil
		} else {
			// Multiple primitives - create AND node for implicit AND behavior
			// According to SIGMA spec, multiple fields in a selection are combined with AND logic
			andNode := ctx.createLogicalNode(dag.LogicalAnd)
			for _, primitiveID := range primitiveIDs {
				primitiveNode := ctx.getOrCreatePrimitiveNode(primitiveID)
				ctx.addDependency(andNode, primitiveNode)
			}
			return andNode, nil
		}

	case *And:
		leftNode, err := ctx.generateDagRecursive(node.Left, selectionMap)
		if err != nil {
			return 0, err
		}
		rightNode, err := ctx.generateDagRecursive(node.Right, selectionMap)
		if err != nil {
			return 0, err
		}
		andNode := ctx.createLogicalNode(dag.LogicalAnd)
		ctx.addDependency(andNode, leftNode)
		ctx.addDependency(andNode, rightNode)
		return andNode, nil

	case *Or:
		leftNode, err := ctx.generateDagRecursive(node.Left, selectionMap)
		if err != nil {
			return 0, err
		}
		rightNode, err := ctx.generateDagRecursive(node.Right, selectionMap)
		if err != nil {
			return 0, err
		}
		orNode := ctx.createLogicalNode(dag.LogicalOr)
		ctx.addDependency(orNode, leftNode)
		ctx.addDependency(orNode, rightNode)
		return orNode, nil

	case *Not:
		operandNode, err := ctx.generateDagRecursive(node.Operand, selectionMap)
		if err != nil {
			return 0, err
		}
		notNode := ctx.createLogicalNode(dag.LogicalNot)
		ctx.addDependency(notNode, operandNode)
		return notNode, nil

	case *OneOfThem:
		// Create OR node for all primitives in all selections
		orNode := ctx.createLogicalNode(dag.LogicalOr)
		hasPrimitives := false

		for _, primitiveIDs := range selectionMap {
			for _, primitiveID := range primitiveIDs {
				primitiveNode := ctx.getOrCreatePrimitiveNode(primitiveID)
				ctx.addDependency(orNode, primitiveNode)
				hasPrimitives = true
			}
		}

		if !hasPrimitives {
			return 0, fmt.Errorf("no primitives found for 'one of them'")
		}

		return orNode, nil

	case *AllOfThem:
		// Create AND node for all primitives in all selections
		andNode := ctx.createLogicalNode(dag.LogicalAnd)
		hasPrimitives := false

		for _, primitiveIDs := range selectionMap {
			for _, primitiveID := range primitiveIDs {
				primitiveNode := ctx.getOrCreatePrimitiveNode(primitiveID)
				ctx.addDependency(andNode, primitiveNode)
				hasPrimitives = true
			}
		}

		if !hasPrimitives {
			return 0, fmt.Errorf("no primitives found for 'all of them'")
		}

		return andNode, nil

	case *OneOfPattern:
		// Find selections matching the pattern and create OR node
		orNode := ctx.createLogicalNode(dag.LogicalOr)
		hasMatches := false

		for selectionName, primitiveIDs := range selectionMap {
			if strings.Contains(selectionName, node.Pattern) {
				for _, primitiveID := range primitiveIDs {
					primitiveNode := ctx.getOrCreatePrimitiveNode(primitiveID)
					ctx.addDependency(orNode, primitiveNode)
					hasMatches = true
				}
			}
		}

		if !hasMatches {
			return 0, fmt.Errorf("no selections found matching pattern: %s", node.Pattern)
		}

		return orNode, nil

	case *AllOfPattern:
		// Find selections matching the pattern and create AND node
		andNode := ctx.createLogicalNode(dag.LogicalAnd)
		hasMatches := false

		for selectionName, primitiveIDs := range selectionMap {
			if strings.Contains(selectionName, node.Pattern) {
				for _, primitiveID := range primitiveIDs {
					primitiveNode := ctx.getOrCreatePrimitiveNode(primitiveID)
					ctx.addDependency(andNode, primitiveNode)
					hasMatches = true
				}
			}
		}

		if !hasMatches {
			return 0, fmt.Errorf("no selections found matching pattern: %s", node.Pattern)
		}

		return andNode, nil

	case *CountOfPattern:
		// For now, treat count patterns as "one of pattern"
		// TODO: Implement proper count logic
		orNode := ctx.createLogicalNode(dag.LogicalOr)
		hasMatches := false

		for selectionName, primitiveIDs := range selectionMap {
			if strings.Contains(selectionName, node.Pattern) {
				for _, primitiveID := range primitiveIDs {
					primitiveNode := ctx.getOrCreatePrimitiveNode(primitiveID)
					ctx.addDependency(orNode, primitiveNode)
					hasMatches = true
				}
			}
		}

		if !hasMatches {
			return 0, fmt.Errorf("no selections found matching pattern: %s", node.Pattern)
		}

		return orNode, nil

	default:
		return 0, fmt.Errorf("unknown AST node type: %T", node)
	}
}

// finalize finalizes DAG generation by creating result node
func (ctx *DagCodegenContext) finalize(conditionRoot dag.NodeId) *DagGenerationResult {
	// Create result node and connect it to the condition root
	resultNode := ctx.createResultNode(ctx.currentRuleID)
	ctx.addDependency(resultNode, conditionRoot)

	return &DagGenerationResult{
		Nodes:          ctx.nodes,
		PrimitiveNodes: ctx.primitiveNodes,
		ResultNodeID:   resultNode,
		RuleID:         ctx.currentRuleID,
	}
}

// DagGenerationResult represents the result of DAG generation from AST
type DagGenerationResult struct {
	// Generated DAG nodes
	Nodes []dag.DagNode
	// Mapping from primitive IDs to their DAG nodes
	PrimitiveNodes map[ir.PrimitiveID]dag.NodeId
	// ID of the result node for this rule
	ResultNodeID dag.NodeId
	// Rule ID
	RuleID ir.RuleID
}

// GenerateDagFromAst generates DAG nodes from a SIGMA condition AST
func GenerateDagFromAst(
	ast ConditionAst,
	selectionMap map[string][]ir.PrimitiveID,
	ruleID ir.RuleID,
) (*DagGenerationResult, error) {
	ctx := NewDagCodegenContext(ruleID)
	conditionRoot, err := ctx.generateDagRecursive(ast, selectionMap)
	if err != nil {
		return nil, err
	}
	return ctx.finalize(conditionRoot), nil
}
