package dag

import (
    "github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
    "github.com/PhucNguyen204/sigma-engine-golang/pkg/errors"
)
type DagBuilder struct {
	nodes              []DagNode
	nextNodeId         NodeId
	primitiveNodes     map[ir.PrimitiveID]NodeId
	ruleResultNodes    map[ir.RuleID]NodeId
	enableOptimization bool
	enablePrefilter    bool
}

func NewDagBuilder() *DagBuilder {
	return &DagBuilder{
		nodes:              make([]DagNode, 0),
		nextNodeId:         0,
		primitiveNodes:     make(map[ir.PrimitiveID]NodeId),
		ruleResultNodes:    make(map[ir.RuleID]NodeId),
		enableOptimization: false,
		enablePrefilter:    false,
	}
}

func (builder *DagBuilder) WithOptimization(enable bool) *DagBuilder {
	builder.enableOptimization = enable
	return builder
}

func (builder *DagBuilder) WithPrefilter(enable bool) *DagBuilder {
	builder.enablePrefilter = enable
	return builder
}

func (builder *DagBuilder) FromRuleset(ruleset *ir.CompiledRuleset) *DagBuilder {
	// First pass: Create primitive nodes (shared across rules)
	for _, primitiveId := range ruleset.PrimitiveMap {
		nodeId := builder.createPrimitiveNode(primitiveId)
		builder.primitiveNodes[primitiveId] = nodeId
	}
	return builder
}
func (builder *DagBuilder) FromPrimitives(primitives []ir.Primitive) error {
	// TODO: Build prefilter if enabled (implement later)
	// if builder.enablePrefilter {
	//     // Create prefilter
	// }

	// Create primitive nodes
	for i, _ := range primitives {
		primitiveId := ir.PrimitiveID(i)
		nodeId := builder.createPrimitiveNode(primitiveId)
		builder.primitiveNodes[primitiveId] = nodeId
	}

	return nil
}

func (builder *DagBuilder) createPrimitiveNode(primitiveId ir.PrimitiveID) NodeId {
	nodeId := builder.nextNodeId
	builder.nextNodeId++

	nodeType := NewPrimitiveNodeType(primitiveId)
	node := NewDagNode(nodeId, nodeType)
	builder.nodes = append(builder.nodes, *node)

	return nodeId
}

func (builder *DagBuilder) createLogicalNode(operation LogicalOp) NodeId {
	nodeId := builder.nextNodeId
	builder.nextNodeId++

	nodeType := NewLogicalNodeType(operation)
	node := NewDagNode(nodeId, nodeType)
	builder.nodes = append(builder.nodes, *node)

	return nodeId
}

func (builder *DagBuilder) createResultNode(ruleId ir.RuleID) NodeId {
	nodeId := builder.nextNodeId
	builder.nextNodeId++

	nodeType := NewResultNodeType(ruleId)
	node := NewDagNode(nodeId, nodeType)
	builder.nodes = append(builder.nodes, *node)

	builder.ruleResultNodes[ruleId] = nodeId
	return nodeId
}

func (builder *DagBuilder) createPrefilterNode(patternCount int) NodeId {
    nodeId := builder.nextNodeId
    builder.nextNodeId++
    
    nodeType := NewPrefilterNodeType(0, patternCount)
    node := NewDagNode(nodeId, nodeType)
    builder.nodes = append(builder.nodes, *node)
    return nodeId
}

// Optimize - Enable optimization passes
func (builder *DagBuilder) Optimize() *DagBuilder {
    if builder.enableOptimization {
        builder.performOptimizations()
    }
    return builder
}

// Build - Build the final compiled DAG
func (builder *DagBuilder) Build() (*CompiledDag, error) {
    // Perform topological sort for execution order
    executionOrder, err := builder.topologicalSort()
    if err != nil {
        return nil, err
    }
    
    // Validate the DAG structure
    if err := builder.validateDagStructure(); err != nil {
        return nil, err
    }
    
    dag := &CompiledDag{
        Nodes:            builder.nodes,
        ExecutionOrder:   executionOrder,
        PrimitiveMap:     builder.primitiveNodes,
        RuleResults:      builder.ruleResultNodes,
        ResultBufferSize: int(builder.nextNodeId),
    }
    
    // Final validation
    if err := dag.Validate(); err != nil {
        return nil, err
    }
    
    return dag, nil
}

// performOptimizations - Perform optimization passes on the DAG
func (builder *DagBuilder) performOptimizations() {
    // Apply optimizations using the DagOptimizer
    if dag, err := builder.buildTemporaryDag(); err == nil {
        if optimizedDag, err := builder.applyDagOptimizations(dag); err == nil {
            builder.updateFromOptimizedDag(optimizedDag)
        }
    }
}

// buildTemporaryDag - Build a temporary DAG for optimization
func (builder *DagBuilder) buildTemporaryDag() (*CompiledDag, error) {
    // Perform topological sort for execution order
    executionOrder, err := builder.topologicalSort()
    if err != nil {
        return nil, err
    }
    
    // Validate the DAG structure
    if err := builder.validateDagStructure(); err != nil {
        return nil, err
    }
    
    // Create a copy of nodes
    nodesCopy := make([]DagNode, len(builder.nodes))
    copy(nodesCopy, builder.nodes)
    
    // Create copies of maps
    primitiveMapCopy := make(map[ir.PrimitiveID]NodeId)
    for k, v := range builder.primitiveNodes {
        primitiveMapCopy[k] = v
    }
    
    ruleResultsCopy := make(map[ir.RuleID]NodeId)
    for k, v := range builder.ruleResultNodes {
        ruleResultsCopy[k] = v
    }
    
    return &CompiledDag{
        Nodes:            nodesCopy,
        ExecutionOrder:   executionOrder,
        PrimitiveMap:     primitiveMapCopy,
        RuleResults:      ruleResultsCopy,
        ResultBufferSize: int(builder.nextNodeId),
    }, nil
}

// applyDagOptimizations - Apply DAG optimizations using the DagOptimizer
func (builder *DagBuilder) applyDagOptimizations(dag *CompiledDag) (*CompiledDag, error) {
    // TODO: Implement when DagOptimizer is ready
    // optimizer := NewDagOptimizer().
    //     WithCSE(true).
    //     WithDCE(true).
    //     WithConstantFolding(true)
    // return optimizer.Optimize(dag)
    
    // For now, return the original DAG
    return dag, nil
}

// updateFromOptimizedDag - Update builder state from optimized DAG
func (builder *DagBuilder) updateFromOptimizedDag(optimizedDag *CompiledDag) {
    builder.nodes = optimizedDag.Nodes
    builder.primitiveNodes = optimizedDag.PrimitiveMap
    builder.ruleResultNodes = optimizedDag.RuleResults
    
    // Update nextNodeId to be safe
    maxId := NodeId(0)
    for _, node := range builder.nodes {
        if node.ID > maxId {
            maxId = node.ID
        }
    }
    builder.nextNodeId = maxId + 1
}

func (builder *DagBuilder) topologicalSort() ([]NodeId, error) {
    inDegree := make([]int, len(builder.nodes))
    var queue []NodeId
    var result []NodeId
    
    // Calculate in-degrees
    for _, node := range builder.nodes {
        for _, depId := range node.Dependencies {
            if int(depId) < len(inDegree) {
                inDegree[node.ID]++
            }
        }
    }
    
    // Find nodes with no dependencies
    for i, degree := range inDegree {
        if degree == 0 {
            queue = append(queue, NodeId(i))
        }
    }
    
    // Process nodes in topological order
    for len(queue) > 0 {
        nodeId := queue[0]
        queue = queue[1:]
        result = append(result, nodeId)
        
        if int(nodeId) < len(builder.nodes) {
            node := &builder.nodes[nodeId]
            for _, dependentId := range node.Dependents {
                if int(dependentId) < len(inDegree) {
                    inDegree[dependentId]--
                    if inDegree[dependentId] == 0 {
                        queue = append(queue, dependentId)
                    }
                }
            }
        }
    }
    
    if len(result) != len(builder.nodes) {
        return nil, errors.NewCompilationError("Cycle detected in DAG")
    }
    
    return result, nil
}

func (builder *DagBuilder) validateDagStructure() error {
    // Check that all rule result nodes exist
    for ruleId := range builder.ruleResultNodes {
        if _, exists := builder.ruleResultNodes[ruleId]; !exists {
            return errors.NewCompilationError("Missing result node for rule: " + string(ruleId))
        }
    }
    
    // Check that all dependencies are valid
    for _, node := range builder.nodes {
        for _, depId := range node.Dependencies {
            if int(depId) >= len(builder.nodes) {
                return errors.NewCompilationError("Invalid dependency")
            }
        }
    }
    
    return nil
}