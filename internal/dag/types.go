package dag

import (
	"fmt"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
	"github.com/PhucNguyen204/sigma-engine-golang/pkg/errors"
)

type NodeId uint32
type LogicalOp int

const (
	LogicalAnd LogicalOp = iota
	LogicalOr
	LogicalNot
)

func (op LogicalOp) String() string {
	switch op {
	case LogicalAnd:
		return "AND"
	case LogicalOr:
		return "OR"
	case LogicalNot:
		return "NOT"
	default:
		return fmt.Sprintf("Unknown LogicalOp: %d", op)
	}
}

type NodeType struct {
	Type         string
	PrimitiveId  *ir.PrimitiveID
	Operation    *LogicalOp
	RuleId       *ir.RuleID
	PrefilterID  *uint32
	PatternCount *int
}

func NewPrimitiveNodeType(primitiveId ir.PrimitiveID) NodeType {
	return NodeType{
		Type:        "Primitive",
		PrimitiveId: &primitiveId,
	}
}

func NewLogicalNodeType(operation LogicalOp) NodeType {
	return NodeType{
		Type:      "Logical",
		Operation: &operation,
	}
}

func NewResultNodeType(ruleId ir.RuleID) NodeType {
	return NodeType{
		Type:   "Result",
		RuleId: &ruleId,
	}
}

func NewPrefilterNodeType(prefilterID uint32, patternCount int) NodeType {
	return NodeType{
		Type:         "Prefilter",
		PrefilterID:  &prefilterID,
		PatternCount: &patternCount,
	}
}

type DagNode struct {
	ID           NodeId
	NodeType     NodeType
	Dependencies []NodeId
	Dependents   []NodeId
	CachedResult *bool
}

func NewDagNode(id NodeId, nodeType NodeType) *DagNode {
	return &DagNode{
		ID:           id,
		NodeType:     nodeType,
		Dependencies: make([]NodeId, 0),
		Dependents:   make([]NodeId, 0),
		CachedResult: nil,
	}
}

func (node *DagNode) AddDependency(dependencyId NodeId) {

	for _, exist := range node.Dependencies {
		if exist == dependencyId {
			return
		}
	}
	node.Dependencies = append(node.Dependencies, dependencyId)
}

func (node *DagNode) AddDependent(dependentId NodeId) {

	for _, exist := range node.Dependents {
		if exist == dependentId {
			return
		}
	}
	node.Dependents = append(node.Dependents, dependentId)
}

// clear Cached
func (node *DagNode) ClearCache() {
	node.CachedResult = nil
}

// la
func (node *DagNode) IsLeaf() bool {
	return len(node.Dependencies) == 0
}

func (node *DagNode) IsRoot() bool {
	return len(node.Dependents) == 0
}

type CompiledDag struct {
	Nodes            []DagNode
	ExecutionOrder   []NodeId
	PrimitiveMap     map[ir.PrimitiveID]NodeId
	RuleResults      map[ir.RuleID]NodeId
	ResultBufferSize int
}

func NewCompiledDag() *CompiledDag {
	return &CompiledDag{
		Nodes:            make([]DagNode, 0),
		ExecutionOrder:   make([]NodeId, 0),
		PrimitiveMap:     make(map[ir.PrimitiveID]NodeId),
		RuleResults:      make(map[ir.RuleID]NodeId),
		ResultBufferSize: 0,
	}
}

func (dag *CompiledDag) GetNode(nodeId NodeId) *DagNode {
	if int(nodeId) < len(dag.Nodes) {
		return &dag.Nodes[nodeId]
	}
	return nil
}

func (dag *CompiledDag) GetNodeMut(nodeId NodeId) *DagNode {
	return dag.GetNode(nodeId)
}

func (dag *CompiledDag) AddNode(node DagNode) NodeId {
	nodeId := node.ID
	dag.Nodes = append(dag.Nodes, node)
	dag.ResultBufferSize = len(dag.Nodes)
	return nodeId
}
func (dag *CompiledDag) NodeCount() int {
	return len(dag.Nodes)
}

func (dag *CompiledDag) Validate() error {
	// Check that execution order contains all nodes
	if len(dag.ExecutionOrder) != len(dag.Nodes) {
		return errors.NewCompilationError("Execution order length mismatch")
	}

	// Check that all dependencies are valid
	for _, node := range dag.Nodes {
		for _, depId := range node.Dependencies {
			if int(depId) >= len(dag.Nodes) {
				return errors.NewCompilationError(
					fmt.Sprintf("Invalid dependency: %d -> %d", node.ID, depId))
			}
		}
	}

	// Check that all rule result nodes exist
	for _, resultNodeId := range dag.RuleResults {
		if int(resultNodeId) >= len(dag.Nodes) {
			return errors.NewCompilationError(
				fmt.Sprintf("Invalid result node: %d", resultNodeId))
		}
	}

	return nil
}

func (dag *CompiledDag) ClearCache() {
	for i := range dag.Nodes {
		dag.Nodes[i].ClearCache()
	}
}

func (dag *CompiledDag) Statistics() *DagStatistics {
	return NewDagStatisticsFromDag(dag)
}

type DagStatistics struct {
	TotalNodes           int
	PrimitiveNodes       int
	LogicalNodes         int
	ResultNodes          int
	MaxDepth             int
	AvgFanout            float64
	SharedPrimitives     int
	EstimatedMemoryBytes int
}

func NewDagStatisticsFromDag(dag *CompiledDag) *DagStatistics {
	var primitiveNodes, logicalNodes, resultNodes int
	var totalDependencies int

	for _, node := range dag.Nodes {
		switch node.NodeType.Type {
		case "Primitive":
			primitiveNodes++
		case "Logical":
			logicalNodes++
		case "Result":
			resultNodes++
		case "Prefilter":
			primitiveNodes++
		}
		totalDependencies += len(node.Dependencies)
	}

	var avgFanout float64
	if len(dag.Nodes) > 0 {
		avgFanout = float64(totalDependencies) / float64(len(dag.Nodes))
	}

	maxDepth := calculateMaxDepth(dag)
	sharedPrimitives := calculateSharedPrimitives(dag)
	estimatedMemoryBytes := len(dag.Nodes)*120 +
		len(dag.ExecutionOrder)*4 +
		len(dag.PrimitiveMap)*12 +
		len(dag.RuleResults)*12

	return &DagStatistics{
		TotalNodes:           len(dag.Nodes),
		PrimitiveNodes:       primitiveNodes,
		LogicalNodes:         logicalNodes,
		ResultNodes:          resultNodes,
		MaxDepth:             maxDepth,
		AvgFanout:            avgFanout,
		SharedPrimitives:     sharedPrimitives,
		EstimatedMemoryBytes: estimatedMemoryBytes,
	}
}

func calculateMaxDepth(dag *CompiledDag) int {
	if len(dag.Nodes) == 0 {
		return 0
	}

	depths := make(map[NodeId]int)
	maxDepth := 0

	for _, nodeId := range dag.ExecutionOrder {
		node := dag.GetNode(nodeId)
		if node == nil {
			continue
		}

		var nodeDepth int
		if len(node.Dependencies) == 0 {
			nodeDepth = 1
		} else {
			maxDepDepth := 0
			for _, depId := range node.Dependencies {
				if depth, exists := depths[depId]; exists && depth > maxDepDepth {
					maxDepDepth = depth
				}
			}
			nodeDepth = maxDepDepth + 1
		}

		depths[nodeId] = nodeDepth
		if nodeDepth > maxDepth {
			maxDepth = nodeDepth
		}
	}

	return maxDepth
}

func calculateSharedPrimitives(dag *CompiledDag) int {
	primitiveUsage := make(map[ir.PrimitiveID]int)
	for _, node := range dag.Nodes {
		if node.NodeType.Type == "Primitive" && node.NodeType.PrimitiveId != nil {
			primitiveUsage[*node.NodeType.PrimitiveId]++
		}
	}
	sharedCount := 0
	for _, count := range primitiveUsage {
		if count > 1 {
			sharedCount++
		}
	}

	return sharedCount
}
