package dag

import (
	"testing"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// createTestDag creates a simple test DAG for testing
func createTestDag() *CompiledDag {
	dag := NewCompiledDag()

	// Add primitive nodes
	primitive1 := NewDagNode(0, NewPrimitiveNodeType(0))
	primitive1.Dependents = []NodeId{2}

	primitive2 := NewDagNode(1, NewPrimitiveNodeType(1))
	primitive2.Dependents = []NodeId{2}

	dag.Nodes = append(dag.Nodes, *primitive1)
	dag.Nodes = append(dag.Nodes, *primitive2)

	// Add logical node
	logical := NewDagNode(2, NewLogicalNodeType(LogicalAnd))
	logical.Dependencies = []NodeId{0, 1}
	logical.Dependents = []NodeId{3}
	dag.Nodes = append(dag.Nodes, *logical)

	// Add result node
	result := NewDagNode(3, NewResultNodeType(1))
	result.Dependencies = []NodeId{2}
	dag.Nodes = append(dag.Nodes, *result)

	// Set up maps
	dag.PrimitiveMap[0] = 0
	dag.PrimitiveMap[1] = 1
	dag.RuleResults[1] = 3
	dag.ExecutionOrder = []NodeId{0, 1, 2, 3}

	return dag
}

func TestDagOptimizerCreation(t *testing.T) {
	optimizer := NewDagOptimizer()

	if !optimizer.enableCSE {
		t.Error("Expected CSE to be enabled by default")
	}
	if !optimizer.enableDCE {
		t.Error("Expected DCE to be enabled by default")
	}
	if !optimizer.enableConstantFolding {
		t.Error("Expected constant folding to be enabled by default")
	}
}

func TestDagOptimizerConfiguration(t *testing.T) {
	optimizer := NewDagOptimizer().
		WithCSE(false).
		WithDCE(false).
		WithConstantFolding(false)

	if optimizer.enableCSE {
		t.Error("Expected CSE to be disabled")
	}
	if optimizer.enableDCE {
		t.Error("Expected DCE to be disabled")
	}
	if optimizer.enableConstantFolding {
		t.Error("Expected constant folding to be disabled")
	}
}

func TestDagOptimizerPartialConfiguration(t *testing.T) {
	optimizer := NewDagOptimizer().
		WithCSE(false).
		WithConstantFolding(true)

	if optimizer.enableCSE {
		t.Error("Expected CSE to be disabled")
	}
	if !optimizer.enableDCE {
		t.Error("Expected DCE to remain default (enabled)")
	}
	if !optimizer.enableConstantFolding {
		t.Error("Expected constant folding to be enabled")
	}
}

func TestOptimizeEmptyDag(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	optimized, err := optimizer.Optimize(dag)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(optimized.Nodes) != 0 {
		t.Error("Expected empty DAG to remain empty")
	}
	if len(optimized.ExecutionOrder) != 0 {
		t.Error("Expected empty execution order")
	}
}

func TestOptimizeSimpleDag(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := createTestDag()

	optimized, err := optimizer.Optimize(dag)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(optimized.Nodes) == 0 {
		t.Error("Expected optimized DAG to have nodes")
	}
	if len(optimized.ExecutionOrder) == 0 {
		t.Error("Expected optimized DAG to have execution order")
	}
}

func TestBuildExpressionSignaturePrimitive(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()
	primitiveId := ir.PrimitiveID(42)
	node := NewDagNode(0, NewPrimitiveNodeType(primitiveId))

	signature := optimizer.buildExpressionSignature(node, dag)
	expected := "P42"

	if signature != expected {
		t.Errorf("Expected signature %s, got %s", expected, signature)
	}
}

func TestBuildExpressionSignatureLogicalAnd(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	// Add dependency nodes
	dag.Nodes = append(dag.Nodes, *NewDagNode(0, NewPrimitiveNodeType(1)))
	dag.Nodes = append(dag.Nodes, *NewDagNode(1, NewPrimitiveNodeType(2)))

	// Create AND node with dependencies
	andNode := NewDagNode(2, NewLogicalNodeType(LogicalAnd))
	andNode.Dependencies = []NodeId{0, 1}

	signature := optimizer.buildExpressionSignature(andNode, dag)

	if !contains(signature, "AND(") {
		t.Errorf("Expected signature to start with AND(, got %s", signature)
	}
	if !contains(signature, "P1") {
		t.Errorf("Expected signature to contain P1, got %s", signature)
	}
	if !contains(signature, "P2") {
		t.Errorf("Expected signature to contain P2, got %s", signature)
	}
}

func TestBuildExpressionSignatureLogicalOr(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	dag.Nodes = append(dag.Nodes, *NewDagNode(0, NewPrimitiveNodeType(1)))
	dag.Nodes = append(dag.Nodes, *NewDagNode(1, NewPrimitiveNodeType(2)))

	orNode := NewDagNode(2, NewLogicalNodeType(LogicalOr))
	orNode.Dependencies = []NodeId{0, 1}

	signature := optimizer.buildExpressionSignature(orNode, dag)

	if !contains(signature, "OR(") {
		t.Errorf("Expected signature to start with OR(, got %s", signature)
	}
	if !contains(signature, "P1") {
		t.Errorf("Expected signature to contain P1, got %s", signature)
	}
	if !contains(signature, "P2") {
		t.Errorf("Expected signature to contain P2, got %s", signature)
	}
}

func TestBuildExpressionSignatureLogicalNot(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	dag.Nodes = append(dag.Nodes, *NewDagNode(0, NewPrimitiveNodeType(1)))

	notNode := NewDagNode(1, NewLogicalNodeType(LogicalNot))
	notNode.Dependencies = []NodeId{0}

	signature := optimizer.buildExpressionSignature(notNode, dag)

	if !contains(signature, "NOT(") {
		t.Errorf("Expected signature to start with NOT(, got %s", signature)
	}
	if !contains(signature, "P1") {
		t.Errorf("Expected signature to contain P1, got %s", signature)
	}
}

func TestBuildExpressionSignatureResult(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()
	ruleId := ir.RuleID(123)
	node := NewDagNode(0, NewResultNodeType(ruleId))

	signature := optimizer.buildExpressionSignature(node, dag)
	expected := "R123"

	if signature != expected {
		t.Errorf("Expected signature %s, got %s", expected, signature)
	}
}

func TestEvaluateConstantExpressionAndTrue(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	// Create nodes with cached results
	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	trueResult := true
	node1.CachedResult = &trueResult

	node2 := NewDagNode(1, NewPrimitiveNodeType(2))
	node2.CachedResult = &trueResult

	dag.Nodes = append(dag.Nodes, *node1)
	dag.Nodes = append(dag.Nodes, *node2)

	andNode := NewDagNode(2, NewLogicalNodeType(LogicalAnd))
	andNode.Dependencies = []NodeId{0, 1}

	result := optimizer.evaluateConstantExpression(andNode, dag)
	if result == nil || *result != true {
		t.Error("Expected AND(true, true) to evaluate to true")
	}
}

func TestEvaluateConstantExpressionAndFalse(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	trueResult := true
	node1.CachedResult = &trueResult

	node2 := NewDagNode(1, NewPrimitiveNodeType(2))
	falseResult := false
	node2.CachedResult = &falseResult

	dag.Nodes = append(dag.Nodes, *node1)
	dag.Nodes = append(dag.Nodes, *node2)

	andNode := NewDagNode(2, NewLogicalNodeType(LogicalAnd))
	andNode.Dependencies = []NodeId{0, 1}

	result := optimizer.evaluateConstantExpression(andNode, dag)
	if result == nil || *result != false {
		t.Error("Expected AND(true, false) to evaluate to false")
	}
}

func TestEvaluateConstantExpressionOrTrue(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	falseResult := false
	node1.CachedResult = &falseResult

	node2 := NewDagNode(1, NewPrimitiveNodeType(2))
	trueResult := true
	node2.CachedResult = &trueResult

	dag.Nodes = append(dag.Nodes, *node1)
	dag.Nodes = append(dag.Nodes, *node2)

	orNode := NewDagNode(2, NewLogicalNodeType(LogicalOr))
	orNode.Dependencies = []NodeId{0, 1}

	result := optimizer.evaluateConstantExpression(orNode, dag)
	if result == nil || *result != true {
		t.Error("Expected OR(false, true) to evaluate to true")
	}
}

func TestEvaluateConstantExpressionOrFalse(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	falseResult := false
	node1.CachedResult = &falseResult
	node2 := NewDagNode(1, NewPrimitiveNodeType(2))
	node2.CachedResult = &falseResult

	dag.Nodes = append(dag.Nodes, *node1)
	dag.Nodes = append(dag.Nodes, *node2)

	orNode := NewDagNode(2, NewLogicalNodeType(LogicalOr))
	orNode.Dependencies = []NodeId{0, 1}

	result := optimizer.evaluateConstantExpression(orNode, dag)
	if result == nil || *result != false {
		t.Error("Expected OR(false, false) to evaluate to false")
	}
}

func TestEvaluateConstantExpressionNotTrue(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	falseResult := false
	node1.CachedResult = &falseResult
	dag.Nodes = append(dag.Nodes, *node1)

	notNode := NewDagNode(1, NewLogicalNodeType(LogicalNot))
	notNode.Dependencies = []NodeId{0}

	result := optimizer.evaluateConstantExpression(notNode, dag)
	if result == nil || *result != true {
		t.Error("Expected NOT(false) to evaluate to true")
	}
}

func TestEvaluateConstantExpressionNotFalse(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	trueResult := true
	node1.CachedResult = &trueResult
	dag.Nodes = append(dag.Nodes, *node1)

	notNode := NewDagNode(1, NewLogicalNodeType(LogicalNot))
	notNode.Dependencies = []NodeId{0}

	result := optimizer.evaluateConstantExpression(notNode, dag)
	if result == nil || *result != false {
		t.Error("Expected NOT(true) to evaluate to false")
	}
}

func TestEvaluateConstantExpressionNotInvalid(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	trueResult := true
	node1.CachedResult = &trueResult

	node2 := NewDagNode(1, NewPrimitiveNodeType(2))
	falseResult := false
	node2.CachedResult = &falseResult

	dag.Nodes = append(dag.Nodes, *node1)
	dag.Nodes = append(dag.Nodes, *node2)

	// Invalid: NOT with multiple dependencies
	notNode := NewDagNode(2, NewLogicalNodeType(LogicalNot))
	notNode.Dependencies = []NodeId{0, 1}

	result := optimizer.evaluateConstantExpression(notNode, dag)
	if result != nil {
		t.Error("Expected NOT with multiple dependencies to return nil")
	}
}

func TestEvaluateConstantExpressionNonConstantDependency(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node1 := NewDagNode(0, NewPrimitiveNodeType(1))
	trueResult := true
	node1.CachedResult = &trueResult

	node2 := NewDagNode(1, NewPrimitiveNodeType(2))
	// No cached result

	dag.Nodes = append(dag.Nodes, *node1)
	dag.Nodes = append(dag.Nodes, *node2)

	andNode := NewDagNode(2, NewLogicalNodeType(LogicalAnd))
	andNode.Dependencies = []NodeId{0, 1}

	result := optimizer.evaluateConstantExpression(andNode, dag)
	if result != nil {
		t.Error("Expected expression with non-constant dependency to return nil")
	}
}

func TestEvaluateConstantExpressionNonLogicalNode(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()
	node := NewDagNode(0, NewPrimitiveNodeType(1))

	result := optimizer.evaluateConstantExpression(node, dag)
	if result != nil {
		t.Error("Expected non-logical node to return nil")
	}
}

func TestFoldNodeToConstant(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	node := NewDagNode(0, NewLogicalNodeType(LogicalAnd))
	node.Dependencies = []NodeId{1, 2}
	dag.Nodes = append(dag.Nodes, *node)

	result := optimizer.foldNodeToConstant(dag, 0, true)
	if !result {
		t.Error("Expected fold operation to succeed")
	}

	foldedNode := dag.GetNode(0)
	if foldedNode == nil {
		t.Fatal("Expected node to exist after folding")
	}
	if foldedNode.CachedResult == nil || *foldedNode.CachedResult != true {
		t.Error("Expected cached result to be true")
	}
	if len(foldedNode.Dependencies) != 0 {
		t.Error("Expected dependencies to be cleared")
	}
}

func TestFoldNodeToConstantNonexistent(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	result := optimizer.foldNodeToConstant(dag, 999, true)
	if result {
		t.Error("Expected fold operation on nonexistent node to fail")
	}
}

func TestMarkReachable(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := createTestDag()
	reachable := make(map[NodeId]bool)

	optimizer.markReachable(3, dag, reachable) // Start from result node

	// Should mark all nodes as reachable since they're all connected
	if !reachable[3] {
		t.Error("Expected result node to be reachable")
	}
	if !reachable[2] {
		t.Error("Expected logical node to be reachable")
	}
	if !reachable[0] {
		t.Error("Expected primitive 1 to be reachable")
	}
	if !reachable[1] {
		t.Error("Expected primitive 2 to be reachable")
	}
}

func TestMarkReachableAlreadyProcessed(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := createTestDag()
	reachable := make(map[NodeId]bool)

	// Pre-mark a node
	reachable[2] = true

	optimizer.markReachable(2, dag, reachable)

	// Should still contain the node
	if !reachable[2] {
		t.Error("Expected pre-marked node to remain reachable")
	}
}

func TestTopologicalSortSimple(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := createTestDag()

	order, err := optimizer.topologicalSort(dag)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should have all nodes
	if len(order) != 4 {
		t.Errorf("Expected 4 nodes in order, got %d", len(order))
	}

	// Find positions
	pos0 := findPosition(order, 0)
	pos1 := findPosition(order, 1)
	pos2 := findPosition(order, 2)
	pos3 := findPosition(order, 3)

	// Primitives should come before logical node
	if pos0 >= pos2 {
		t.Error("Expected primitive 0 to come before logical node 2")
	}
	if pos1 >= pos2 {
		t.Error("Expected primitive 1 to come before logical node 2")
	}
	if pos2 >= pos3 {
		t.Error("Expected logical node 2 to come before result node 3")
	}
}

func TestTopologicalSortEmptyDag(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := NewCompiledDag()

	order, err := optimizer.topologicalSort(dag)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(order) != 0 {
		t.Error("Expected empty order for empty DAG")
	}
}

func TestEstimateNodeSelectivity(t *testing.T) {
	optimizer := NewDagOptimizer()
	dag := createTestDag()

	// Test primitive selectivity
	primitiveSelectivity := optimizer.estimateNodeSelectivity(dag, 0)
	if primitiveSelectivity <= 0 {
		t.Error("Expected positive selectivity for primitive node")
	}

	// Test logical AND selectivity
	andSelectivity := optimizer.estimateNodeSelectivity(dag, 2)
	if andSelectivity != 0.3 {
		t.Errorf("Expected AND selectivity of 0.3, got %f", andSelectivity)
	}

	// Test result selectivity
	resultSelectivity := optimizer.estimateNodeSelectivity(dag, 3)
	if resultSelectivity != 1.0 {
		t.Errorf("Expected result selectivity of 1.0, got %f", resultSelectivity)
	}
}

// Helper functions
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func findPosition(slice []NodeId, value NodeId) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}
	return -1
}
