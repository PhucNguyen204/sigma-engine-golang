package dag

import (
	"fmt"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
	"github.com/PhucNguyen204/sigma-engine-golang/pkg/errors"
)

type DagEvaluationResult struct {
	MatchedRules         []ir.RuleID
	NodesEvaluated       int
	PrimitiveEvaluations int
}

func NewDagEvaluationResult() *DagEvaluationResult {
	return &DagEvaluationResult{
		MatchedRules:         make([]ir.RuleID, 0),
		NodesEvaluated:       0,
		PrimitiveEvaluations: 0,
	}
}

type DagEvaluator struct {
	dag                  *CompiledDag
	nodeResults          map[uint32]bool
	fastResults          []bool
	nodesEvaluated       int
	primitiveEvaluations int
	prefilterHits        int
	prefilterMisses      int
}

func NewDagEvaluatorWithPrimitives(dag *CompiledDag) *DagEvaluator {
	return &DagEvaluator{
		dag:                  dag,
		nodeResults:          make(map[uint32]bool),
		fastResults:          make([]bool, len(dag.Nodes)),
		nodesEvaluated:       0,
		primitiveEvaluations: 0,
		prefilterHits:        0,
		prefilterMisses:      0,
	}
}

func NewDagEvaluatorWithPrimitivesAndPrefilter(dag *CompiledDag) *DagEvaluator {
	// TODO: Add prefilter parameter when implemented
	return NewDagEvaluatorWithPrimitives(dag)
}

func (eval *DagEvaluator) Evaluate(event map[string]interface{}) (*DagEvaluationResult, error) {
	// Early termination with prefilter if available (TODO: implement later)
	// if eval.prefilter != nil {
	//     if !eval.prefilter.Matches(event) {
	//         eval.prefilterMisses++
	//         return &DagEvaluationResult{
	//             MatchedRules:         make([]ir.RuleID, 0),
	//             NodesEvaluated:       1,
	//             PrimitiveEvaluations: 0,
	//         }, nil
	//     }
	//     eval.prefilterHits++
	// }

	// Ultra-fast path for single primitive rules (most common case)
	if len(eval.dag.RuleResults) == 1 && len(eval.dag.Nodes) <= 3 {
		return eval.evaluateSinglePrimitiveFast(event)
	}

	// Use fast-path for small DAGs to avoid HashMap overhead
	if len(eval.dag.Nodes) <= 32 {
		return eval.evaluateFastPath(event)
	} else {
		return eval.evaluateStandardPath(event)
	}
}

func (eval *DagEvaluator) reset() {
	eval.nodesEvaluated = 0
	eval.primitiveEvaluations = 0

	// Clear maps/slices
	for k := range eval.nodeResults {
		delete(eval.nodeResults, k)
	}

	for i := range eval.fastResults {
		eval.fastResults[i] = false
	}
}

func (eval *DagEvaluator) evaluateLogicalOperation(operation LogicalOp, dependencies []NodeId) bool {
	switch operation {
	case LogicalAnd:
		// AND: tất cả dependencies phải true
		for _, depId := range dependencies {
			if result, exists := eval.nodeResults[uint32(depId)]; !exists || !result {
				return false
			}
		}
		return len(dependencies) > 0 // AND với 0 dependencies = false

	case LogicalOr:
		// OR: ít nhất một dependency phải true
		for _, depId := range dependencies {
			if result, exists := eval.nodeResults[uint32(depId)]; exists && result {
				return true
			}
		}
		return false

	case LogicalNot:
		// NOT: chỉ có 1 dependency, đảo ngược kết quả
		if len(dependencies) == 1 {
			if result, exists := eval.nodeResults[uint32(dependencies[0])]; exists {
				return !result
			}
		}
		return false

	default:
		return false
	}
}
func (eval *DagEvaluator) evaluateLogicalOperationFast(operation LogicalOp, dependencies []NodeId) bool {
	switch operation {
	case LogicalAnd:
		// AND: tất cả dependencies phải true
		for _, depId := range dependencies {
			if int(depId) >= len(eval.fastResults) || !eval.fastResults[depId] {
				return false
			}
		}
		return len(dependencies) > 0

	case LogicalOr:
		// OR: ít nhất một dependency phải true
		for _, depId := range dependencies {
			if int(depId) < len(eval.fastResults) && eval.fastResults[depId] {
				return true
			}
		}
		return false

	case LogicalNot:
		// NOT: chỉ có 1 dependency, đảo ngược kết quả
		if len(dependencies) == 1 && int(dependencies[0]) < len(eval.fastResults) {
			return !eval.fastResults[dependencies[0]]
		}
		return false

	default:
		return false
	}
}

func (eval *DagEvaluator) evaluatePrimitive(primitiveId ir.PrimitiveID, event map[string]interface{}) (bool, error) {
	eval.primitiveEvaluations++

	// TODO: Implement actual primitive matching when CompiledPrimitive is ready
	// For now, return false as placeholder
	//
	// Logic sẽ là:
	// 1. Lấy CompiledPrimitive từ eval.primitives[primitiveId]
	// 2. Áp dụng field matching logic
	// 3. Trả về true/false

	return false, nil
}

func (eval *DagEvaluator) evaluateNode(nodeId uint32, event map[string]interface{}) (bool, error) {
	node := eval.dag.GetNode(NodeId(nodeId))
	if node == nil {
		return false, errors.NewExecutionError(fmt.Sprintf("Node not found: %d", nodeId))
	}

	switch node.NodeType.Type {
	case "Primitive":
		if node.NodeType.PrimitiveId != nil {
			return eval.evaluatePrimitive(*node.NodeType.PrimitiveId, event)
		}
		return false, nil

	case "Logical":
		if node.NodeType.Operation != nil {
			return eval.evaluateLogicalOperation(*node.NodeType.Operation, node.Dependencies), nil
		}
		return false, nil

	case "Result":
		// Result node: trả về kết quả của dependency đầu tiên
		if len(node.Dependencies) == 1 {
			if result, exists := eval.nodeResults[uint32(node.Dependencies[0])]; exists {
				return result, nil
			}
		}
		return false, nil

	case "Prefilter":
		// Prefilter nodes đã được handle ở đầu evaluation
		// Nếu đến đây thì prefilter đã pass
		return true, nil

	default:
		return false, nil
	}
}

func (eval *DagEvaluator) evaluateNodeFast(nodeId uint32, event map[string]interface{}) (bool, error) {
	node := eval.dag.GetNode(NodeId(nodeId))
	if node == nil {
		return false, errors.NewExecutionError(fmt.Sprintf("Node not found: %d", nodeId))
	}

	switch node.NodeType.Type {
	case "Primitive":
		if node.NodeType.PrimitiveId != nil {
			return eval.evaluatePrimitive(*node.NodeType.PrimitiveId, event)
		}
		return false, nil

	case "Logical":
		if node.NodeType.Operation != nil {
			return eval.evaluateLogicalOperationFast(*node.NodeType.Operation, node.Dependencies), nil
		}
		return false, nil

	case "Result":
		// Result node: trả về kết quả từ fastResults
		if len(node.Dependencies) == 1 {
			depId := int(node.Dependencies[0])
			if depId < len(eval.fastResults) {
				return eval.fastResults[depId], nil
			}
		}
		return false, nil

	case "Prefilter":
		return true, nil

	default:
		return false, nil
	}
}

func (eval *DagEvaluator) evaluateStandardPath(event map[string]interface{}) (*DagEvaluationResult, error) {
	eval.reset()

	// Evaluate nodes in topological order
	for _, nodeId := range eval.dag.ExecutionOrder {
		result, err := eval.evaluateNode(uint32(nodeId), event)
		if err != nil {
			return nil, err
		}
		eval.nodeResults[uint32(nodeId)] = result
		eval.nodesEvaluated++
	}

	// Collect matched rules
	var matchedRules []ir.RuleID
	for ruleId, resultNodeId := range eval.dag.RuleResults {
		if result, exists := eval.nodeResults[uint32(resultNodeId)]; exists && result {
			matchedRules = append(matchedRules, ruleId)
		}
	}

	return &DagEvaluationResult{
		MatchedRules:         matchedRules,
		NodesEvaluated:       eval.nodesEvaluated,
		PrimitiveEvaluations: eval.primitiveEvaluations,
	}, nil
}

// evaluateFastPath - Fast-path evaluation for small DAGs using slice
func (eval *DagEvaluator) evaluateFastPath(event map[string]interface{}) (*DagEvaluationResult, error) {
	eval.reset()

	// Evaluate nodes in topological order
	for _, nodeId := range eval.dag.ExecutionOrder {
		result, err := eval.evaluateNodeFast(uint32(nodeId), event)
		if err != nil {
			return nil, err
		}
		if int(nodeId) < len(eval.fastResults) {
			eval.fastResults[nodeId] = result
		}
		eval.nodesEvaluated++
	}

	// Collect matched rules
	var matchedRules []ir.RuleID
	for ruleId, resultNodeId := range eval.dag.RuleResults {
		if int(resultNodeId) < len(eval.fastResults) && eval.fastResults[resultNodeId] {
			matchedRules = append(matchedRules, ruleId)
		}
	}

	return &DagEvaluationResult{
		MatchedRules:         matchedRules,
		NodesEvaluated:       eval.nodesEvaluated,
		PrimitiveEvaluations: eval.primitiveEvaluations,
	}, nil
}

// evaluateSinglePrimitiveFast - Ultra-fast evaluation for single primitive rules
func (eval *DagEvaluator) evaluateSinglePrimitiveFast(event map[string]interface{}) (*DagEvaluationResult, error) {
	eval.reset()

	// Lấy rule duy nhất
	var ruleId ir.RuleID
	var resultNodeId NodeId
	for rid, rnid := range eval.dag.RuleResults {
		ruleId = rid
		resultNodeId = rnid
		break
	}

	resultNode := eval.dag.GetNode(resultNodeId)
	if resultNode == nil || resultNode.NodeType.Type != "Result" {
		return eval.evaluateStandardPath(event) // fallback
	}

	if len(resultNode.Dependencies) == 1 {
		primitiveNodeId := resultNode.Dependencies[0]
		primitiveNode := eval.dag.GetNode(primitiveNodeId)

		if primitiveNode != nil && primitiveNode.NodeType.Type == "Primitive" && primitiveNode.NodeType.PrimitiveId != nil {
			eval.nodesEvaluated = 2
			result, err := eval.evaluatePrimitive(*primitiveNode.NodeType.PrimitiveId, event)
			if err != nil {
				return nil, err
			}

			var matchedRules []ir.RuleID
			if result {
				matchedRules = append(matchedRules, ruleId)
			}

			return &DagEvaluationResult{
				MatchedRules:         matchedRules,
				NodesEvaluated:       eval.nodesEvaluated,
				PrimitiveEvaluations: eval.primitiveEvaluations,
			}, nil
		}
	}

	// Fallback to standard evaluation
	return eval.evaluateStandardPath(event)
}
