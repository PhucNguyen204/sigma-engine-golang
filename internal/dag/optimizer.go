package dag

import (
	"fmt"
	"sort"
	"strings"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
	"github.com/PhucNguyen204/sigma-engine-golang/pkg/errors"
)

type DagOptimizer struct {
	enableCSE             bool
	enableDCE             bool
	enableConstantFolding bool
}

func NewDagOptimizer() *DagOptimizer {
	return &DagOptimizer{
		enableCSE:             true,
		enableDCE:             true,
		enableConstantFolding: true,
	}
}

func (opt *DagOptimizer) WithCSE(enable bool) *DagOptimizer {
	opt.enableCSE = enable
	return opt
}

func (opt *DagOptimizer) WithDCE(enable bool) *DagOptimizer {
	opt.enableDCE = enable
	return opt
}

func (opt *DagOptimizer) WithConstantFolding(enable bool) *DagOptimizer {
	opt.enableConstantFolding = enable
	return opt
}


func (opt *DagOptimizer) Optimize(dag *CompiledDag) (*CompiledDag, error) {
 	optimizedDag := opt.copyDag(dag)

	// Perform optimization passes in order
	var err error

	if opt.enableConstantFolding {
		optimizedDag, err = opt.constantFolding(optimizedDag)
		if err != nil {
			return nil, err
		}
	}

	if opt.enableCSE {
		optimizedDag, err = opt.commonSubexpressionElimination(optimizedDag)
		if err != nil {
			return nil, err
		}
	}

	if opt.enableDCE {
		optimizedDag, err = opt.deadCodeElimination(optimizedDag)
		if err != nil {
			return nil, err
		}
	}

	optimizedDag, err = opt.rebuildExecutionOrderOptimized(optimizedDag)
	if err != nil {
		return nil, err
	}

	return optimizedDag, nil
}

func (opt *DagOptimizer) copyDag(dag *CompiledDag) *CompiledDag {
	// Copy nodes
	nodesCopy := make([]DagNode, len(dag.Nodes))
	for i, node := range dag.Nodes {
		nodesCopy[i] = DagNode{
			ID:           node.ID,
			NodeType:     node.NodeType, // Shallow copy is fine for NodeType
			Dependencies: make([]NodeId, len(node.Dependencies)),
			Dependents:   make([]NodeId, len(node.Dependents)),
			CachedResult: node.CachedResult,
		}
		copy(nodesCopy[i].Dependencies, node.Dependencies)
		copy(nodesCopy[i].Dependents, node.Dependents)
	}

	// Copy execution order
	executionOrderCopy := make([]NodeId, len(dag.ExecutionOrder))
	copy(executionOrderCopy, dag.ExecutionOrder)

	// Copy primitive map
	primitiveMapCopy := make(map[ir.PrimitiveID]NodeId)
	for k, v := range dag.PrimitiveMap {
		primitiveMapCopy[k] = v
	}

	// Copy rule results
	ruleResultsCopy := make(map[ir.RuleID]NodeId)
	for k, v := range dag.RuleResults {
		ruleResultsCopy[k] = v
	}

	return &CompiledDag{
		Nodes:            nodesCopy,
		ExecutionOrder:   executionOrderCopy,
		PrimitiveMap:     primitiveMapCopy,
		RuleResults:      ruleResultsCopy,
		ResultBufferSize: dag.ResultBufferSize,
	}
}

// topologicalSort - Perform basic topological sort for DAG
func (opt *DagOptimizer) topologicalSort(dag *CompiledDag) ([]NodeId, error) {
	inDegree := make(map[NodeId]int)
	var queue []NodeId
	var result []NodeId

	// Initialize in-degrees
	for _, node := range dag.Nodes {
		inDegree[node.ID] = 0
	}

	// Calculate in-degrees
	for _, node := range dag.Nodes {
		for range node.Dependencies {
			inDegree[node.ID]++
		}
	}

	// Find nodes with no dependencies
	for nodeId, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeId)
		}
	}

	// Process nodes in topological order
	for len(queue) > 0 {
		nodeId := queue[0]
		queue = queue[1:]
		result = append(result, nodeId)

		// Find node in DAG
		var currentNode *DagNode
		for i := range dag.Nodes {
			if dag.Nodes[i].ID == nodeId {
				currentNode = &dag.Nodes[i]
				break
			}
		}

		if currentNode != nil {
			for _, dependentId := range currentNode.Dependents {
				inDegree[dependentId]--
				if inDegree[dependentId] == 0 {
					queue = append(queue, dependentId)
				}
			}
		}
	}

	if len(result) != len(dag.Nodes) {
		return nil, errors.NewCompilationError("Cycle detected in DAG")
	}

	return result, nil
}

// markReachable - Mark all nodes reachable from given node
func (opt *DagOptimizer) markReachable(nodeId NodeId, dag *CompiledDag, reachable map[NodeId]bool) {
	if reachable[nodeId] {
		return // Already visited
	}

	reachable[nodeId] = true

	// Find node in DAG
	for _, node := range dag.Nodes {
		if node.ID == nodeId {
			// Mark all dependencies as reachable
			for _, depId := range node.Dependencies {
				opt.markReachable(depId, dag, reachable)
			}
			break
		}
	}
}

// deadCodeElimination - Remove nodes that don't contribute to any rule result
func (opt *DagOptimizer) deadCodeElimination(dag *CompiledDag) (*CompiledDag, error) {
	reachable := make(map[NodeId]bool)

	// Mark all result nodes as reachable
	for _, resultNodeId := range dag.RuleResults {
		opt.markReachable(resultNodeId, dag, reachable)
	}

	// Remove unreachable nodes
	var newNodes []DagNode
	for _, node := range dag.Nodes {
		if reachable[node.ID] {
			newNodes = append(newNodes, node)
		}
	}
	dag.Nodes = newNodes

	newPrimitiveMap := make(map[ir.PrimitiveID]NodeId)
	for k, v := range dag.PrimitiveMap {
		if reachable[v] {
			newPrimitiveMap[k] = v
		}
	}
	dag.PrimitiveMap = newPrimitiveMap

	// Update rule results - remove references to deleted nodes
	newRuleResults := make(map[ir.RuleID]NodeId)
	for k, v := range dag.RuleResults {
		if reachable[v] {
			newRuleResults[k] = v
		}
	}
	dag.RuleResults = newRuleResults

	return dag, nil
}

// buildExpressionSignature - Build signature string for CSE
func (opt *DagOptimizer) buildExpressionSignature(node *DagNode, dag *CompiledDag) string {
	switch node.NodeType.Type {
	case "Primitive":
		if node.NodeType.PrimitiveId != nil {
			return fmt.Sprintf("P%d", *node.NodeType.PrimitiveId)
		}
		return "P_UNKNOWN"

	case "Logical":
		if node.NodeType.Operation == nil {
			return "L_UNKNOWN"
		}

		// Collect dependency signatures
		var depSignatures []string
		for _, depId := range node.Dependencies {
			for _, depNode := range dag.Nodes {
				if depNode.ID == depId {
					depSig := opt.buildExpressionSignature(&depNode, dag)
					depSignatures = append(depSignatures, depSig)
					break
				}
			}
		}

		// Sort dependencies for canonical representation
		sort.Strings(depSignatures)

		switch *node.NodeType.Operation {
		case LogicalAnd:
			return fmt.Sprintf("AND(%s)", strings.Join(depSignatures, ","))
		case LogicalOr:
			return fmt.Sprintf("OR(%s)", strings.Join(depSignatures, ","))
		case LogicalNot:
			return fmt.Sprintf("NOT(%s)", strings.Join(depSignatures, ","))
		default:
			return "L_UNKNOWN"
		}

	case "Result":
		if node.NodeType.RuleId != nil {
			// Result nodes should never be merged - each rule needs its own
			return fmt.Sprintf("R%d", *node.NodeType.RuleId)
		}
		return "R_UNKNOWN"

	case "Prefilter":
		if node.NodeType.PrefilterID != nil && node.NodeType.PatternCount != nil {
			return fmt.Sprintf("F%d:%d", *node.NodeType.PrefilterID, *node.NodeType.PatternCount)
		}
		return "F_UNKNOWN"

	default:
		return "UNKNOWN"
	}
}

// commonSubexpressionElimination - Perform CSE optimization
func (opt *DagOptimizer) commonSubexpressionElimination(dag *CompiledDag) (*CompiledDag, error) {
	changed := true
	iterations := 0
	const maxIterations = 5

	// Iterate until no more changes
	for changed && iterations < maxIterations {
		changed = false
		iterations++

		expressionMap := make(map[string]NodeId)
		nodeMapping := make(map[NodeId]NodeId)

		// Build expression signatures for each node (excluding result nodes)
		for _, node := range dag.Nodes {
			if node.NodeType.Type == "Result" {
				continue // Don't merge result nodes
			}

			signature := opt.buildExpressionSignature(&node, dag)

			if existingNodeId, exists := expressionMap[signature]; exists {
				// Found a duplicate expression - map this node to the existing one
				if node.ID != existingNodeId {
					nodeMapping[node.ID] = existingNodeId
					changed = true
				}
			} else {
				// First occurrence of this expression
				expressionMap[signature] = node.ID
			}
		}

		// Apply node mappings to eliminate duplicates
		if len(nodeMapping) > 0 {
			var err error
			dag, err = opt.applyNodeMapping(dag, nodeMapping)
			if err != nil {
				return nil, err
			}
		}
	}

	return dag, nil
}

func (opt *DagOptimizer) applyNodeMapping(dag *CompiledDag, nodeMapping map[NodeId]NodeId) (*CompiledDag, error) {
	nodesToRemove := make(map[NodeId]bool)
	for nodeId := range nodeMapping {
		nodesToRemove[nodeId] = true
	}

	var newNodes []DagNode
	for _, node := range dag.Nodes {
		if !nodesToRemove[node.ID] {
			newNodes = append(newNodes, node)
		}
	}

	for i := range newNodes {
		node := &newNodes[i]
		var newDependencies []NodeId
		for _, depId := range node.Dependencies {
			mappedId := nodeMapping[depId]
			if mappedId == 0 {
				mappedId = depId 
			}
			found := false
			for _, existingDep := range newDependencies {
				if existingDep == mappedId {
					found = true
					break
				}
			}
			if !found {
				newDependencies = append(newDependencies, mappedId)
			}
		}
		node.Dependencies = newDependencies

		var newDependents []NodeId
		for _, depId := range node.Dependents {
			mappedId := nodeMapping[depId]
			if mappedId == 0 {
				mappedId = depId // Use original if no mapping
			}
			// Remove duplicates
			found := false
			for _, existingDep := range newDependents {
				if existingDep == mappedId {
					found = true
					break
				}
			}
			if !found {
				newDependents = append(newDependents, mappedId)
			}
		}
		node.Dependents = newDependents
	}

	dag.Nodes = newNodes

	for k, v := range dag.PrimitiveMap {
		if mappedId, exists := nodeMapping[v]; exists {
			dag.PrimitiveMap[k] = mappedId
		}
	}

	for k, v := range dag.RuleResults {
		if mappedId, exists := nodeMapping[v]; exists {
			dag.RuleResults[k] = mappedId
		}
	}

	return dag, nil
}

// estimateNodeSelectivity - Estimate selectivity for execution order optimization
func (opt *DagOptimizer) estimateNodeSelectivity(dag *CompiledDag, nodeId NodeId) float64 {
	for _, node := range dag.Nodes {
		if node.ID == nodeId {
			switch node.NodeType.Type {
			case "Primitive":
				if node.NodeType.PrimitiveId != nil {
					// Estimate selectivity based on primitive characteristics
					// Lower IDs = more selective (heuristic)
					return 0.1 + (float64(*node.NodeType.PrimitiveId) * 0.1)
				}
				return 0.5

			case "Logical":
				if node.NodeType.Operation != nil {
					switch *node.NodeType.Operation {
					case LogicalAnd:
						return 0.3 // AND operations are more selective
					case LogicalOr:
						return 0.7 // OR operations are less selective
					case LogicalNot:
						return 0.5 // NOT operations have medium selectivity
					}
				}
				return 0.5

			case "Result":
				return 1.0 // Result nodes should be executed last

			case "Prefilter":
				return 0.01 // Prefilter nodes should be executed first

			default:
				return 0.5
			}
		}
	}
	return 0.5 // Default selectivity for unknown nodes
}

// rebuildExecutionOrderOptimized - Rebuild execution order with selectivity optimization
func (opt *DagOptimizer) rebuildExecutionOrderOptimized(dag *CompiledDag) (*CompiledDag, error) {
	// First get the basic topological order
	basicOrder, err := opt.topologicalSort(dag)
	if err != nil {
		return nil, err
	}

	// Then optimize the order within topological constraints
	optimizedOrder, err := opt.optimizeExecutionOrder(dag, basicOrder)
	if err != nil {
		return nil, err
	}

	dag.ExecutionOrder = optimizedOrder
	return dag, nil
}

// optimizeExecutionOrder - Optimize execution order to prioritize high-selectivity primitives
func (opt *DagOptimizer) optimizeExecutionOrder(dag *CompiledDag, basicOrder []NodeId) ([]NodeId, error) {
	var optimizedOrder []NodeId
	remainingNodes := make(map[NodeId]bool)
	processedNodes := make(map[NodeId]bool)

	for _, nodeId := range basicOrder {
		remainingNodes[nodeId] = true
	}

	// Process nodes in waves, respecting dependencies
	for len(remainingNodes) > 0 {
		// Find nodes that can be executed (all dependencies satisfied)
		var readyNodes []NodeId
		for nodeId := range remainingNodes {
			canExecute := true
			for _, node := range dag.Nodes {
				if node.ID == nodeId {
					for _, depId := range node.Dependencies {
						if !processedNodes[depId] {
							canExecute = false
							break
						}
					}
					break
				}
			}
			if canExecute {
				readyNodes = append(readyNodes, nodeId)
			}
		}

		if len(readyNodes) == 0 {
			// This shouldn't happen with a valid DAG
			break
		}

		// Sort ready nodes by estimated selectivity (most selective first)
		sort.Slice(readyNodes, func(i, j int) bool {
			selectivityA := opt.estimateNodeSelectivity(dag, readyNodes[i])
			selectivityB := opt.estimateNodeSelectivity(dag, readyNodes[j])
			return selectivityA < selectivityB // Lower selectivity = higher priority
		})

		// Add ready nodes to execution order
		for _, nodeId := range readyNodes {
			optimizedOrder = append(optimizedOrder, nodeId)
			delete(remainingNodes, nodeId)
			processedNodes[nodeId] = true
		}
	}

	return optimizedOrder, nil
}

// constantFolding - Perform constant folding optimization
func (opt *DagOptimizer) constantFolding(dag *CompiledDag) (*CompiledDag, error) {
	changed := true
	iterations := 0
	const maxIterations = 10 // Prevent infinite loops

	// Iterate until no more changes or max iterations reached
	for changed && iterations < maxIterations {
		changed = false
		iterations++

		// Find nodes that can be constant folded
		var nodesToFold []struct {
			nodeId        NodeId
			constantValue bool
		}

		for _, node := range dag.Nodes {
			if node.NodeType.Type == "Logical" {
				if constantResult := opt.evaluateConstantExpression(&node, dag); constantResult != nil {
					nodesToFold = append(nodesToFold, struct {
						nodeId        NodeId
						constantValue bool
					}{node.ID, *constantResult})
				}
			}
		}

		// Apply constant folding
		for _, fold := range nodesToFold {
			if opt.foldNodeToConstant(dag, fold.nodeId, fold.constantValue) {
				changed = true
			}
		}
	}

	return dag, nil
}

// evaluateConstantExpression - Evaluate a logical expression if all operands are constants
func (opt *DagOptimizer) evaluateConstantExpression(node *DagNode, dag *CompiledDag) *bool {
	if node.NodeType.Type != "Logical" || node.NodeType.Operation == nil {
		return nil
	}

	var operandValues []bool

	// Check if all dependencies are constant
	for _, depId := range node.Dependencies {
		for _, depNode := range dag.Nodes {
			if depNode.ID == depId {
				if depNode.CachedResult != nil {
					operandValues = append(operandValues, *depNode.CachedResult)
				} else {
					// Not all operands are constant
					return nil
				}
				break
			}
		}
	}

	// Evaluate the logical operation
	switch *node.NodeType.Operation {
	case LogicalAnd:
		result := true
		for _, val := range operandValues {
			result = result && val
		}
		return &result

	case LogicalOr:
		result := false
		for _, val := range operandValues {
			result = result || val
		}
		return &result

	case LogicalNot:
		if len(operandValues) == 1 {
			result := !operandValues[0]
			return &result
		}
		return nil // Invalid NOT operation

	default:
		return nil
	}
}

// foldNodeToConstant - Fold a node to a constant value
func (opt *DagOptimizer) foldNodeToConstant(dag *CompiledDag, nodeId NodeId, constantValue bool) bool {
	// Find the node to fold
	for i := range dag.Nodes {
		if dag.Nodes[i].ID == nodeId {
			// Cache the constant result
			dag.Nodes[i].CachedResult = &constantValue

			// Clear dependencies since this is now a constant
			dag.Nodes[i].Dependencies = nil

			return true
		}
	}

	return false
}
