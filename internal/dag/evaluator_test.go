package dag

import (
	"testing"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

func createTestDagForEvaluator() *CompiledDag {
	// Create a simple DAG: primitive -> result
	primitiveNode := NewDagNode(0, NewPrimitiveNodeType(ir.PrimitiveID(0)))
	resultNode := NewDagNode(1, NewResultNodeType(ir.RuleID(1)))

	primitiveMap := make(map[ir.PrimitiveID]NodeId)
	primitiveMap[ir.PrimitiveID(0)] = NodeId(0)

	ruleResults := make(map[ir.RuleID]NodeId)
	ruleResults[ir.RuleID(1)] = NodeId(1)

	return &CompiledDag{
		Nodes:            []DagNode{*primitiveNode, *resultNode},
		ExecutionOrder:   []NodeId{0, 1},
		PrimitiveMap:     primitiveMap,
		RuleResults:      ruleResults,
		ResultBufferSize: 2,
	}
}

func TestDagEvaluationResultCreation(t *testing.T) {
	result := NewDagEvaluationResult()

	if len(result.MatchedRules) != 0 {
		t.Errorf("Expected empty matched rules, got %d", len(result.MatchedRules))
	}
	if result.NodesEvaluated != 0 {
		t.Errorf("Expected 0 nodes evaluated, got %d", result.NodesEvaluated)
	}
	if result.PrimitiveEvaluations != 0 {
		t.Errorf("Expected 0 primitive evaluations, got %d", result.PrimitiveEvaluations)
	}
}

func TestDagEvaluatorCreation(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	if len(evaluator.fastResults) != len(dag.Nodes) {
		t.Errorf("Expected fast results length %d, got %d", len(dag.Nodes), len(evaluator.fastResults))
	}
	if evaluator.nodesEvaluated != 0 {
		t.Errorf("Expected 0 nodes evaluated, got %d", evaluator.nodesEvaluated)
	}
	if evaluator.primitiveEvaluations != 0 {
		t.Errorf("Expected 0 primitive evaluations, got %d", evaluator.primitiveEvaluations)
	}
}

func TestDagEvaluatorReset(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Simulate some state
	evaluator.nodeResults[0] = true
	evaluator.fastResults[0] = true
	evaluator.nodesEvaluated = 5
	evaluator.primitiveEvaluations = 3

	// Reset and verify
	evaluator.reset()

	if len(evaluator.nodeResults) != 0 {
		t.Errorf("Expected empty node results after reset, got %d", len(evaluator.nodeResults))
	}
	if evaluator.fastResults[0] {
		t.Error("Expected fast results to be reset to false")
	}
	if evaluator.nodesEvaluated != 0 {
		t.Errorf("Expected 0 nodes evaluated after reset, got %d", evaluator.nodesEvaluated)
	}
	if evaluator.primitiveEvaluations != 0 {
		t.Errorf("Expected 0 primitive evaluations after reset, got %d", evaluator.primitiveEvaluations)
	}
}

func TestEvaluatePrimitiveNotFound(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	event := map[string]interface{}{
		"field1": "value1",
	}

	// Test with placeholder implementation (should return false, nil)
	result, err := evaluator.evaluatePrimitive(ir.PrimitiveID(0), event)
	if err != nil {
		t.Errorf("Expected no error with placeholder implementation, got %v", err)
	}
	if result {
		t.Error("Expected false from placeholder primitive evaluation")
	}
}

func TestEvaluateLogicalOperationAndSuccess(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up dependencies
	evaluator.nodeResults[0] = true
	evaluator.nodeResults[1] = true

	result := evaluator.evaluateLogicalOperation(LogicalAnd, []NodeId{0, 1})
	if !result {
		t.Error("Expected AND operation with all true to return true")
	}
}

func TestEvaluateLogicalOperationAndFailure(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up dependencies with one false
	evaluator.nodeResults[0] = true
	evaluator.nodeResults[1] = false

	result := evaluator.evaluateLogicalOperation(LogicalAnd, []NodeId{0, 1})
	if result {
		t.Error("Expected AND operation with one false to return false")
	}
}

func TestEvaluateLogicalOperationOrSuccess(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up dependencies with one true
	evaluator.nodeResults[0] = false
	evaluator.nodeResults[1] = true

	result := evaluator.evaluateLogicalOperation(LogicalOr, []NodeId{0, 1})
	if !result {
		t.Error("Expected OR operation with one true to return true")
	}
}

func TestEvaluateLogicalOperationOrFailure(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up dependencies with both false
	evaluator.nodeResults[0] = false
	evaluator.nodeResults[1] = false

	result := evaluator.evaluateLogicalOperation(LogicalOr, []NodeId{0, 1})
	if result {
		t.Error("Expected OR operation with all false to return false")
	}
}

func TestEvaluateLogicalOperationNotSuccess(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up dependency
	evaluator.nodeResults[0] = false

	result := evaluator.evaluateLogicalOperation(LogicalNot, []NodeId{0})
	if !result {
		t.Error("Expected NOT operation with false to return true")
	}
}

func TestEvaluateLogicalOperationNotFailure(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up dependency
	evaluator.nodeResults[0] = true

	result := evaluator.evaluateLogicalOperation(LogicalNot, []NodeId{0})
	if result {
		t.Error("Expected NOT operation with true to return false")
	}
}

func TestEvaluateLogicalOperationFastAndSuccess(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up fast results
	evaluator.fastResults[0] = true
	evaluator.fastResults[1] = true

	result := evaluator.evaluateLogicalOperationFast(LogicalAnd, []NodeId{0, 1})
	if !result {
		t.Error("Expected fast AND operation with all true to return true")
	}
}

func TestEvaluateLogicalOperationFastAndFailure(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up fast results with one false
	evaluator.fastResults[0] = true
	evaluator.fastResults[1] = false

	result := evaluator.evaluateLogicalOperationFast(LogicalAnd, []NodeId{0, 1})
	if result {
		t.Error("Expected fast AND operation with one false to return false")
	}
}

func TestEvaluateLogicalOperationFastOrSuccess(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up fast results with one true
	evaluator.fastResults[0] = false
	evaluator.fastResults[1] = true

	result := evaluator.evaluateLogicalOperationFast(LogicalOr, []NodeId{0, 1})
	if !result {
		t.Error("Expected fast OR operation with one true to return true")
	}
}

func TestEvaluateLogicalOperationFastOrFailure(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up fast results with both false
	evaluator.fastResults[0] = false
	evaluator.fastResults[1] = false

	result := evaluator.evaluateLogicalOperationFast(LogicalOr, []NodeId{0, 1})
	if result {
		t.Error("Expected fast OR operation with all false to return false")
	}
}

func TestEvaluateLogicalOperationFastNotSuccess(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up fast results
	evaluator.fastResults[0] = false

	result := evaluator.evaluateLogicalOperationFast(LogicalNot, []NodeId{0})
	if !result {
		t.Error("Expected fast NOT operation with false to return true")
	}
}

func TestEvaluateLogicalOperationFastNotFailure(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	// Set up fast results
	evaluator.fastResults[0] = true

	result := evaluator.evaluateLogicalOperationFast(LogicalNot, []NodeId{0})
	if result {
		t.Error("Expected fast NOT operation with true to return false")
	}
}

func TestEvaluateEmptyEvent(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	event := make(map[string]interface{})

	result, err := evaluator.Evaluate(event)
	if err != nil {
		t.Errorf("Expected no error evaluating empty event, got %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
	if len(result.MatchedRules) != 0 {
		t.Errorf("Expected no matched rules for empty event, got %d", len(result.MatchedRules))
	}
}

func TestEvaluateSimpleEvent(t *testing.T) {
	dag := createTestDagForEvaluator()
	evaluator := NewDagEvaluatorWithPrimitives(dag)

	event := map[string]interface{}{
		"field1": "value1",
		"field2": "value2",
	}

	result, err := evaluator.Evaluate(event)
	if err != nil {
		t.Errorf("Expected no error evaluating simple event, got %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
	// With placeholder primitive implementation, no rules should match
	if len(result.MatchedRules) != 0 {
		t.Errorf("Expected no matched rules with placeholder implementation, got %d", len(result.MatchedRules))
	}
}
