package dag

import (
	"testing"
)

func createTestDagForTypes() *CompiledDag {
	dag := NewCompiledDag()

	// Add primitive nodes
	primitive1 := NewDagNode(0, NewPrimitiveNodeType(0))
	primitive1.AddDependent(2)
	primitive2 := NewDagNode(1, NewPrimitiveNodeType(1))
	primitive2.AddDependent(2)
	dag.AddNode(*primitive1)
	dag.AddNode(*primitive2)

	// Add logical node
	logicalNode := NewDagNode(2, NewLogicalNodeType(LogicalAnd))
	logicalNode.AddDependency(0)
	logicalNode.AddDependency(1)
	logicalNode.AddDependent(3)
	dag.AddNode(*logicalNode)

	// Add result node
	resultNode := NewDagNode(3, NewResultNodeType(1))
	resultNode.AddDependency(2)
	dag.AddNode(*resultNode)

	dag.PrimitiveMap[0] = 0
	dag.PrimitiveMap[1] = 1
	dag.RuleResults[1] = 3
	dag.ExecutionOrder = []NodeId{0, 1, 2, 3}

	return dag
}

func TestLogicalOpString(t *testing.T) {
	tests := []struct {
		op       LogicalOp
		expected string
	}{
		{LogicalAnd, "AND"},
		{LogicalOr, "OR"},
		{LogicalNot, "NOT"},
	}

	for _, test := range tests {
		if test.op.String() != test.expected {
			t.Errorf("LogicalOp.String() = %v, want %v", test.op.String(), test.expected)
		}
	}
}

func TestNodeTypeConstructors(t *testing.T) {
	// Test primitive node type
	primitiveType := NewPrimitiveNodeType(42)
	if primitiveType.Type != "Primitive" {
		t.Errorf("Expected Type = 'Primitive', got %v", primitiveType.Type)
	}
	if primitiveType.PrimitiveId == nil || *primitiveType.PrimitiveId != 42 {
		t.Errorf("Expected PrimitiveId = 42, got %v", primitiveType.PrimitiveId)
	}

	// Test logical node type
	logicalType := NewLogicalNodeType(LogicalAnd)
	if logicalType.Type != "Logical" {
		t.Errorf("Expected Type = 'Logical', got %v", logicalType.Type)
	}
	if logicalType.Operation == nil || *logicalType.Operation != LogicalAnd {
		t.Errorf("Expected Operation = LogicalAnd, got %v", logicalType.Operation)
	}

	// Test result node type
	resultType := NewResultNodeType(123)
	if resultType.Type != "Result" {
		t.Errorf("Expected Type = 'Result', got %v", resultType.Type)
	}
	if resultType.RuleId == nil || *resultType.RuleId != 123 {
		t.Errorf("Expected RuleId = 123, got %v", resultType.RuleId)
	}

	// Test prefilter node type
	prefilterType := NewPrefilterNodeType(1, 5)
	if prefilterType.Type != "Prefilter" {
		t.Errorf("Expected Type = 'Prefilter', got %v", prefilterType.Type)
	}
	if prefilterType.PrefilterID == nil || *prefilterType.PrefilterID != 1 {
		t.Errorf("Expected PrefilterID = 1, got %v", prefilterType.PrefilterID)
	}
	if prefilterType.PatternCount == nil || *prefilterType.PatternCount != 5 {
		t.Errorf("Expected PatternCount = 5, got %v", prefilterType.PatternCount)
	}
}

func TestDagNodeCreation(t *testing.T) {
	node := NewDagNode(42, NewPrimitiveNodeType(1))

	if node.ID != 42 {
		t.Errorf("Expected ID = 42, got %v", node.ID)
	}
	if node.NodeType.Type != "Primitive" {
		t.Errorf("Expected NodeType.Type = 'Primitive', got %v", node.NodeType.Type)
	}
	if len(node.Dependencies) != 0 {
		t.Errorf("Expected empty dependencies, got %v", node.Dependencies)
	}
	if len(node.Dependents) != 0 {
		t.Errorf("Expected empty dependents, got %v", node.Dependents)
	}
	if node.CachedResult != nil {
		t.Errorf("Expected nil CachedResult, got %v", node.CachedResult)
	}
}

func TestDagNodeAddDependency(t *testing.T) {
	node := NewDagNode(1, NewLogicalNodeType(LogicalAnd))

	node.AddDependency(10)
	if len(node.Dependencies) != 1 || node.Dependencies[0] != 10 {
		t.Errorf("Expected dependencies = [10], got %v", node.Dependencies)
	}

	node.AddDependency(20)
	if len(node.Dependencies) != 2 {
		t.Errorf("Expected 2 dependencies, got %v", len(node.Dependencies))
	}

	// Adding duplicate should not change anything
	node.AddDependency(10)
	if len(node.Dependencies) != 2 {
		t.Errorf("Expected 2 dependencies after duplicate add, got %v", len(node.Dependencies))
	}
}

func TestDagNodeAddDependent(t *testing.T) {
	node := NewDagNode(1, NewPrimitiveNodeType(1))

	node.AddDependent(10)
	if len(node.Dependents) != 1 || node.Dependents[0] != 10 {
		t.Errorf("Expected dependents = [10], got %v", node.Dependents)
	}

	node.AddDependent(20)
	if len(node.Dependents) != 2 {
		t.Errorf("Expected 2 dependents, got %v", len(node.Dependents))
	}

	// Adding duplicate should not change anything
	node.AddDependent(10)
	if len(node.Dependents) != 2 {
		t.Errorf("Expected 2 dependents after duplicate add, got %v", len(node.Dependents))
	}
}

func TestDagNodeClearCache(t *testing.T) {
	node := NewDagNode(1, NewPrimitiveNodeType(1))

	// Initially no cache
	if node.CachedResult != nil {
		t.Errorf("Expected nil CachedResult initially, got %v", node.CachedResult)
	}

	// Set cache
	result := true
	node.CachedResult = &result
	if node.CachedResult == nil || *node.CachedResult != true {
		t.Errorf("Expected CachedResult = true, got %v", node.CachedResult)
	}

	// Clear cache
	node.ClearCache()
	if node.CachedResult != nil {
		t.Errorf("Expected nil CachedResult after clear, got %v", node.CachedResult)
	}
}

func TestDagNodeIsLeaf(t *testing.T) {
	node := NewDagNode(1, NewPrimitiveNodeType(1))

	// Initially is leaf (no dependencies)
	if !node.IsLeaf() {
		t.Error("Expected node to be leaf initially")
	}

	// Add dependency
	node.AddDependency(10)
	if node.IsLeaf() {
		t.Error("Expected node to not be leaf after adding dependency")
	}

	// Clear dependencies
	node.Dependencies = node.Dependencies[:0]
	if !node.IsLeaf() {
		t.Error("Expected node to be leaf after clearing dependencies")
	}
}

func TestDagNodeIsRoot(t *testing.T) {
	node := NewDagNode(1, NewResultNodeType(1))

	// Initially is root (no dependents)
	if !node.IsRoot() {
		t.Error("Expected node to be root initially")
	}

	// Add dependent
	node.AddDependent(10)
	if node.IsRoot() {
		t.Error("Expected node to not be root after adding dependent")
	}

	// Clear dependents
	node.Dependents = node.Dependents[:0]
	if !node.IsRoot() {
		t.Error("Expected node to be root after clearing dependents")
	}
}

func TestCompiledDagCreation(t *testing.T) {
	dag := NewCompiledDag()

	if len(dag.Nodes) != 0 {
		t.Errorf("Expected empty nodes, got %v", len(dag.Nodes))
	}
	if len(dag.ExecutionOrder) != 0 {
		t.Errorf("Expected empty execution order, got %v", len(dag.ExecutionOrder))
	}
	if len(dag.PrimitiveMap) != 0 {
		t.Errorf("Expected empty primitive map, got %v", len(dag.PrimitiveMap))
	}
	if len(dag.RuleResults) != 0 {
		t.Errorf("Expected empty rule results, got %v", len(dag.RuleResults))
	}
	if dag.ResultBufferSize != 0 {
		t.Errorf("Expected ResultBufferSize = 0, got %v", dag.ResultBufferSize)
	}
}

func TestCompiledDagAddNode(t *testing.T) {
	dag := NewCompiledDag()
	node := NewDagNode(42, NewPrimitiveNodeType(1))

	returnedId := dag.AddNode(*node)
	if returnedId != 42 {
		t.Errorf("Expected returned ID = 42, got %v", returnedId)
	}
	if len(dag.Nodes) != 1 {
		t.Errorf("Expected 1 node, got %v", len(dag.Nodes))
	}
	if dag.ResultBufferSize != 1 {
		t.Errorf("Expected ResultBufferSize = 1, got %v", dag.ResultBufferSize)
	}
	if dag.Nodes[0].ID != 42 {
		t.Errorf("Expected node ID = 42, got %v", dag.Nodes[0].ID)
	}
}

func TestCompiledDagGetNode(t *testing.T) {
	dag := NewCompiledDag()
	node := NewDagNode(0, NewPrimitiveNodeType(1))
	dag.AddNode(*node)

	// Valid node ID
	retrievedNode := dag.GetNode(0)
	if retrievedNode == nil {
		t.Error("Expected to retrieve node with ID 0")
	}
	if retrievedNode != nil && retrievedNode.ID != 0 {
		t.Errorf("Expected retrieved node ID = 0, got %v", retrievedNode.ID)
	}

	// Invalid node ID
	if dag.GetNode(1) != nil {
		t.Error("Expected nil for invalid node ID 1")
	}
	if dag.GetNode(999) != nil {
		t.Error("Expected nil for invalid node ID 999")
	}
}

func TestCompiledDagNodeCount(t *testing.T) {
	dag := NewCompiledDag()
	if dag.NodeCount() != 0 {
		t.Errorf("Expected node count = 0, got %v", dag.NodeCount())
	}

	dag.AddNode(*NewDagNode(0, NewPrimitiveNodeType(1)))
	if dag.NodeCount() != 1 {
		t.Errorf("Expected node count = 1, got %v", dag.NodeCount())
	}

	dag.AddNode(*NewDagNode(1, NewLogicalNodeType(LogicalAnd)))
	if dag.NodeCount() != 2 {
		t.Errorf("Expected node count = 2, got %v", dag.NodeCount())
	}
}

func TestCompiledDagValidateSuccess(t *testing.T) {
	dag := createTestDagForTypes()
	if err := dag.Validate(); err != nil {
		t.Errorf("Expected validation to succeed, got error: %v", err)
	}
}

func TestCompiledDagValidateExecutionOrderMismatch(t *testing.T) {
	dag := createTestDagForTypes()
	dag.ExecutionOrder = dag.ExecutionOrder[:len(dag.ExecutionOrder)-1] // Remove one element

	err := dag.Validate()
	if err == nil {
		t.Error("Expected validation to fail")
	}
	if err != nil && !containsString(err.Error(), "Execution order length mismatch") {
		t.Errorf("Expected error about execution order mismatch, got: %v", err)
	}
}

func TestCompiledDagValidateInvalidDependency(t *testing.T) {
	dag := NewCompiledDag()
	node := NewDagNode(0, NewLogicalNodeType(LogicalAnd))
	node.AddDependency(999) // Invalid dependency
	dag.AddNode(*node)
	dag.ExecutionOrder = []NodeId{0}

	err := dag.Validate()
	if err == nil {
		t.Error("Expected validation to fail")
	}
	if err != nil && !containsString(err.Error(), "Invalid dependency") {
		t.Errorf("Expected error about invalid dependency, got: %v", err)
	}
}

func TestCompiledDagValidateInvalidResultNode(t *testing.T) {
	dag := NewCompiledDag()
	dag.AddNode(*NewDagNode(0, NewPrimitiveNodeType(1)))
	dag.RuleResults[1] = 999 // Invalid result node
	dag.ExecutionOrder = []NodeId{0}

	err := dag.Validate()
	if err == nil {
		t.Error("Expected validation to fail")
	}
	if err != nil && !containsString(err.Error(), "Invalid result node") {
		t.Errorf("Expected error about invalid result node, got: %v", err)
	}
}

func TestCompiledDagClearCache(t *testing.T) {
	dag := createTestDagForTypes()

	// Set some cached results
	result1 := true
	result2 := false
	dag.Nodes[0].CachedResult = &result1
	dag.Nodes[1].CachedResult = &result2

	// Verify cache is set
	if dag.Nodes[0].CachedResult == nil || *dag.Nodes[0].CachedResult != true {
		t.Error("Expected first node to have cached result true")
	}
	if dag.Nodes[1].CachedResult == nil || *dag.Nodes[1].CachedResult != false {
		t.Error("Expected second node to have cached result false")
	}

	// Clear cache
	dag.ClearCache()

	// Verify cache is cleared
	for i, node := range dag.Nodes {
		if node.CachedResult != nil {
			t.Errorf("Expected node %d to have nil cached result after clear", i)
		}
	}
}

func TestCompiledDagStatistics(t *testing.T) {
	dag := createTestDagForTypes()
	stats := dag.Statistics()

	if stats.TotalNodes != 4 {
		t.Errorf("Expected TotalNodes = 4, got %v", stats.TotalNodes)
	}
	if stats.PrimitiveNodes != 2 {
		t.Errorf("Expected PrimitiveNodes = 2, got %v", stats.PrimitiveNodes)
	}
	if stats.LogicalNodes != 1 {
		t.Errorf("Expected LogicalNodes = 1, got %v", stats.LogicalNodes)
	}
	if stats.ResultNodes != 1 {
		t.Errorf("Expected ResultNodes = 1, got %v", stats.ResultNodes)
	}
	if stats.AvgFanout <= 0.0 {
		t.Errorf("Expected AvgFanout > 0, got %v", stats.AvgFanout)
	}
	if stats.EstimatedMemoryBytes <= 0 {
		t.Errorf("Expected EstimatedMemoryBytes > 0, got %v", stats.EstimatedMemoryBytes)
	}
}

func TestDagStatisticsEmptyDag(t *testing.T) {
	dag := NewCompiledDag()
	stats := NewDagStatisticsFromDag(dag)

	if stats.TotalNodes != 0 {
		t.Errorf("Expected TotalNodes = 0, got %v", stats.TotalNodes)
	}
	if stats.PrimitiveNodes != 0 {
		t.Errorf("Expected PrimitiveNodes = 0, got %v", stats.PrimitiveNodes)
	}
	if stats.LogicalNodes != 0 {
		t.Errorf("Expected LogicalNodes = 0, got %v", stats.LogicalNodes)
	}
	if stats.ResultNodes != 0 {
		t.Errorf("Expected ResultNodes = 0, got %v", stats.ResultNodes)
	}
	if stats.MaxDepth != 0 {
		t.Errorf("Expected MaxDepth = 0, got %v", stats.MaxDepth)
	}
	if stats.AvgFanout != 0.0 {
		t.Errorf("Expected AvgFanout = 0, got %v", stats.AvgFanout)
	}
	if stats.SharedPrimitives != 0 {
		t.Errorf("Expected SharedPrimitives = 0, got %v", stats.SharedPrimitives)
	}
}

func TestDagStatisticsSingleNode(t *testing.T) {
	dag := NewCompiledDag()
	dag.AddNode(*NewDagNode(0, NewPrimitiveNodeType(1)))
	dag.ExecutionOrder = []NodeId{0}

	stats := NewDagStatisticsFromDag(dag)

	if stats.TotalNodes != 1 {
		t.Errorf("Expected TotalNodes = 1, got %v", stats.TotalNodes)
	}
	if stats.PrimitiveNodes != 1 {
		t.Errorf("Expected PrimitiveNodes = 1, got %v", stats.PrimitiveNodes)
	}
	if stats.LogicalNodes != 0 {
		t.Errorf("Expected LogicalNodes = 0, got %v", stats.LogicalNodes)
	}
	if stats.ResultNodes != 0 {
		t.Errorf("Expected ResultNodes = 0, got %v", stats.ResultNodes)
	}
	if stats.MaxDepth != 1 {
		t.Errorf("Expected MaxDepth = 1, got %v", stats.MaxDepth)
	}
	if stats.AvgFanout != 0.0 {
		t.Errorf("Expected AvgFanout = 0, got %v", stats.AvgFanout)
	}
	if stats.SharedPrimitives != 0 {
		t.Errorf("Expected SharedPrimitives = 0, got %v", stats.SharedPrimitives)
	}
}

func TestDagStatisticsComplexDag(t *testing.T) {
	dag := createTestDagForTypes()
	stats := NewDagStatisticsFromDag(dag)

	if stats.TotalNodes != 4 {
		t.Errorf("Expected TotalNodes = 4, got %v", stats.TotalNodes)
	}
	if stats.PrimitiveNodes != 2 {
		t.Errorf("Expected PrimitiveNodes = 2, got %v", stats.PrimitiveNodes)
	}
	if stats.LogicalNodes != 1 {
		t.Errorf("Expected LogicalNodes = 1, got %v", stats.LogicalNodes)
	}
	if stats.ResultNodes != 1 {
		t.Errorf("Expected ResultNodes = 1, got %v", stats.ResultNodes)
	}
	if stats.MaxDepth != 3 {
		t.Errorf("Expected MaxDepth = 3, got %v", stats.MaxDepth)
	}
	if stats.AvgFanout <= 0.0 {
		t.Errorf("Expected AvgFanout > 0, got %v", stats.AvgFanout)
	}
	if stats.SharedPrimitives != 0 {
		t.Errorf("Expected SharedPrimitives = 0 for test DAG, got %v", stats.SharedPrimitives)
	}
}

func TestDagStatisticsSharedPrimitives(t *testing.T) {
	dag := NewCompiledDag()

	// Add multiple nodes using the same primitive
	dag.AddNode(*NewDagNode(0, NewPrimitiveNodeType(1)))
	dag.AddNode(*NewDagNode(1, NewPrimitiveNodeType(1))) // Same primitive
	dag.AddNode(*NewDagNode(2, NewPrimitiveNodeType(2)))
	dag.AddNode(*NewDagNode(3, NewPrimitiveNodeType(2))) // Same primitive
	dag.AddNode(*NewDagNode(4, NewPrimitiveNodeType(3))) // Unique primitive
	dag.ExecutionOrder = []NodeId{0, 1, 2, 3, 4}

	stats := NewDagStatisticsFromDag(dag)

	if stats.TotalNodes != 5 {
		t.Errorf("Expected TotalNodes = 5, got %v", stats.TotalNodes)
	}
	if stats.PrimitiveNodes != 5 {
		t.Errorf("Expected PrimitiveNodes = 5, got %v", stats.PrimitiveNodes)
	}
	if stats.SharedPrimitives != 2 {
		t.Errorf("Expected SharedPrimitives = 2 (primitives 1 and 2), got %v", stats.SharedPrimitives)
	}
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			indexOf(s, substr) >= 0))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
