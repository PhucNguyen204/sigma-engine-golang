package dag

import (
	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
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


func (eval *DagEvaluator) evaluateLogicalOperation