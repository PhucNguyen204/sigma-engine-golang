package compiler

import (
	"testing"
)

// TestDagCodegenContextCreation matches Rust test_dag_codegen_context_creation
func TestDagCodegenContextCreation(t *testing.T) {
	ctx := NewDagCodegenContext(42)
	if ctx.currentRuleID != 42 {
		t.Errorf("Expected rule ID 42, got %d", ctx.currentRuleID)
	}
	if ctx.nextNodeID != 0 {
		t.Errorf("Expected next node ID 0, got %d", ctx.nextNodeID)
	}
	if len(ctx.nodes) != 0 {
		t.Errorf("Expected empty nodes, got %d", len(ctx.nodes))
	}
	if len(ctx.primitiveNodes) != 0 {
		t.Errorf("Expected empty primitive nodes, got %d", len(ctx.primitiveNodes))
	}
}

// TestGenerateDagFromIdentifierSinglePrimitive matches Rust test_generate_dag_from_identifier_single_primitive
func TestGenerateDagFromIdentifierSinglePrimitive(t *testing.T) {
	ast := &Identifier{Name: "selection2"}
	selectionMap := createTestSelectionMap()

	result, err := GenerateDagFromAst(ast, selectionMap, 1)
	if err != nil {
		t.Fatalf("Failed to generate DAG: %v", err)
	}

	if result.RuleID != 1 {
		t.Errorf("Expected rule ID 1, got %d", result.RuleID)
	}
	if len(result.Nodes) != 2 { // primitive + result node
		t.Errorf("Expected 2 nodes, got %d", len(result.Nodes))
	}
	if len(result.PrimitiveNodes) != 1 {
		t.Errorf("Expected 1 primitive node, got %d", len(result.PrimitiveNodes))
	}
	// selection2 maps to primitive ID 1, not 2 according to createTestSelectionMap
	if _, exists := result.PrimitiveNodes[1]; !exists {
		t.Error("Expected primitive node for ID 1")
	}
}

// TestGenerateDagFromAndExpression matches Rust test_generate_dag_from_and_expression
func TestGenerateDagFromAndExpression(t *testing.T) {
	ast := &And{
		Left:  &Identifier{Name: "selection1"},
		Right: &Identifier{Name: "selection2"},
	}
	selectionMap := createTestSelectionMap()

	result, err := GenerateDagFromAst(ast, selectionMap, 1)
	if err != nil {
		t.Fatalf("Failed to generate DAG: %v", err)
	}

	if result.RuleID != 1 {
		t.Errorf("Expected rule ID 1, got %d", result.RuleID)
	}
	// Should have multiple nodes including AND logic - at least 4 nodes is fine
	if len(result.Nodes) < 4 {
		t.Errorf("Expected at least 4 nodes, got %d", len(result.Nodes))
	}

	// Check that we have the expected primitive nodes based on createTestSelectionMap
	if _, exists := result.PrimitiveNodes[0]; !exists {
		t.Error("Expected primitive node for ID 0")
	}
	if _, exists := result.PrimitiveNodes[1]; !exists {
		t.Error("Expected primitive node for ID 1")
	}
}

// TestGenerateDagUnknownSelectionError matches Rust test_generate_dag_unknown_selection_error
func TestGenerateDagUnknownSelectionError(t *testing.T) {
	ast := &Identifier{Name: "unknown_selection"}
	selectionMap := createTestSelectionMap()

	result, err := GenerateDagFromAst(ast, selectionMap, 1)
	if err == nil {
		t.Error("Expected error for unknown selection")
	}
	if result != nil {
		t.Error("Expected nil result for error case")
	}

	if err != nil && !contains(err.Error(), "unknown selection: unknown_selection") {
		t.Errorf("Expected 'unknown selection' error, got: %v", err)
	}
}
