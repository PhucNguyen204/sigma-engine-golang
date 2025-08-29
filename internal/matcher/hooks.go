package matcher

import (
	"sync"

	"github.com/PhucNguyen204/sigma-engine-golang/internal/ir"
)

// CompilationPhase represents different phases of compilation where hooks can be registered
type CompilationPhase int

const (
	// PrimitiveDiscovery is called for each primitive during discovery phase
	PrimitiveDiscovery CompilationPhase = iota
	
	// CompilationStart is called before compilation begins
	CompilationStart
	
	// CompilationEnd is called after compilation completes
	CompilationEnd
	
	// OptimizationStart is called before optimization phase
	OptimizationStart
	
	// OptimizationEnd is called after optimization completes
	OptimizationEnd
)

// CompilationContext contains information passed to compilation hooks
type CompilationContext struct {
	// Current primitive being processed
	Primitive *ir.Primitive
	
	// Phase of compilation
	Phase CompilationPhase
	
	// Extracted literal values (for PrimitiveDiscovery phase)
	LiteralValues []string
	
	// Whether this primitive contains only literal patterns
	IsLiteralOnly bool
	
	// Field name being processed
	FieldName string
	
	// Match type of the primitive
	MatchType string
	
	// Applied modifiers
	Modifiers []string
	
	// Estimated selectivity (0.0 = highly selective, 1.0 = not selective)
	Selectivity float64
	
	// Total primitives discovered so far
	TotalPrimitives int
	
	// Additional metadata
	Metadata map[string]interface{}
}

// CompilationHookFn is the signature for compilation hook functions
type CompilationHookFn func(ctx *CompilationContext) error

// HookRegistry manages compilation hooks for different phases
type HookRegistry struct {
	hooks map[CompilationPhase][]CompilationHookFn
	mutex sync.RWMutex
}

// NewHookRegistry creates a new hook registry
func NewHookRegistry() *HookRegistry {
	return &HookRegistry{
		hooks: make(map[CompilationPhase][]CompilationHookFn),
	}
}

// RegisterHook registers a hook function for a specific compilation phase
func (r *HookRegistry) RegisterHook(phase CompilationPhase, hook CompilationHookFn) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	if r.hooks[phase] == nil {
		r.hooks[phase] = make([]CompilationHookFn, 0)
	}
	
	r.hooks[phase] = append(r.hooks[phase], hook)
}

// ExecuteHooks executes all registered hooks for a given phase
func (r *HookRegistry) ExecuteHooks(ctx *CompilationContext) error {
	r.mutex.RLock()
	hooks := r.hooks[ctx.Phase]
	r.mutex.RUnlock()
	
	for _, hook := range hooks {
		if err := hook(ctx); err != nil {
			return err
		}
	}
	
	return nil
}

// ClearHooks removes all hooks for a specific phase
func (r *HookRegistry) ClearHooks(phase CompilationPhase) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	delete(r.hooks, phase)
}

// ClearAllHooks removes all registered hooks
func (r *HookRegistry) ClearAllHooks() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	r.hooks = make(map[CompilationPhase][]CompilationHookFn)
}

// GetHookCount returns the number of hooks registered for a phase
func (r *HookRegistry) GetHookCount(phase CompilationPhase) int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	return len(r.hooks[phase])
}

// Global hook registry instance
var globalHookRegistry *HookRegistry
var hookRegistryOnce sync.Once

// GetGlobalHookRegistry returns the singleton global hook registry
func GetGlobalHookRegistry() *HookRegistry {
	hookRegistryOnce.Do(func() {
		globalHookRegistry = NewHookRegistry()
	})
	return globalHookRegistry
}

// CompilationHookManager manages the execution of compilation hooks during building
type CompilationHookManager struct {
	registry         *HookRegistry
	filterIntegration *FilterIntegration
	context          *CompilationContext
}

// NewCompilationHookManager creates a new compilation hook manager
func NewCompilationHookManager() *CompilationHookManager {
	return &CompilationHookManager{
		registry:          GetGlobalHookRegistry(),
		filterIntegration: NewFilterIntegration(),
		context: &CompilationContext{
			Metadata: make(map[string]interface{}),
		},
	}
}

// NotifyPrimitiveDiscovery notifies hooks about primitive discovery
func (m *CompilationHookManager) NotifyPrimitiveDiscovery(primitive *ir.Primitive) error {
	// Update context
	m.context.Primitive = primitive
	m.context.Phase = PrimitiveDiscovery
	m.context.FieldName = primitive.Field
	m.context.MatchType = primitive.MatchType
	m.context.Modifiers = primitive.Modifiers
	m.context.TotalPrimitives++
	
	// Extract literal values and determine if literal-only
	m.context.LiteralValues = m.extractLiteralValues(primitive)
	m.context.IsLiteralOnly = IsLiteralMatchType(primitive.MatchType)
	
	// Calculate selectivity
	m.context.Selectivity = m.calculatePrimitiveSelectivity(primitive)
	
	// Add to filter integration
	m.filterIntegration.AddPrimitive(primitive)
	
	// Execute registered hooks
	return m.registry.ExecuteHooks(m.context)
}

// NotifyCompilationStart notifies hooks about compilation start
func (m *CompilationHookManager) NotifyCompilationStart() error {
	m.context.Phase = CompilationStart
	return m.registry.ExecuteHooks(m.context)
}

// NotifyCompilationEnd notifies hooks about compilation end
func (m *CompilationHookManager) NotifyCompilationEnd() error {
	m.context.Phase = CompilationEnd
	return m.registry.ExecuteHooks(m.context)
}

// NotifyOptimizationStart notifies hooks about optimization start
func (m *CompilationHookManager) NotifyOptimizationStart() error {
	m.context.Phase = OptimizationStart
	return m.registry.ExecuteHooks(m.context)
}

// NotifyOptimizationEnd notifies hooks about optimization end
func (m *CompilationHookManager) NotifyOptimizationEnd() error {
	m.context.Phase = OptimizationEnd
	return m.registry.ExecuteHooks(m.context)
}

// GetFilterIntegration returns the filter integration helper
func (m *CompilationHookManager) GetFilterIntegration() *FilterIntegration {
	return m.filterIntegration
}

// extractLiteralValues extracts literal values from a primitive
func (m *CompilationHookManager) extractLiteralValues(primitive *ir.Primitive) []string {
	if IsLiteralMatchType(primitive.MatchType) {
		return primitive.Values
	}
	return nil
}

// calculatePrimitiveSelectivity estimates the selectivity of a primitive
func (m *CompilationHookManager) calculatePrimitiveSelectivity(primitive *ir.Primitive) float64 {
	if len(primitive.Values) == 0 {
		return 1.0 // Not selective
	}
	
	// Average selectivity across all values
	totalSelectivity := 0.0
	for _, value := range primitive.Values {
		totalSelectivity += CalculateSelectivity(value)
	}
	
	return totalSelectivity / float64(len(primitive.Values))
}

// CreateAhoCorasickHook creates a hook for AhoCorasick pattern collection
func CreateAhoCorasickHook(patterns *[]string) CompilationHookFn {
	var mutex sync.Mutex
	
	return func(ctx *CompilationContext) error {
		if ctx.Phase == PrimitiveDiscovery && ctx.IsLiteralOnly {
			mutex.Lock()
			defer mutex.Unlock()
			
			*patterns = append(*patterns, ctx.LiteralValues...)
		}
		return nil
	}
}

// CreateStatisticsHook creates a hook for collecting compilation statistics
func CreateStatisticsHook(stats *FilterCompilationStats) CompilationHookFn {
	var mutex sync.Mutex
	
	return func(ctx *CompilationContext) error {
		if ctx.Phase == PrimitiveDiscovery {
			mutex.Lock()
			defer mutex.Unlock()
			
			stats.TotalPrimitives++
			if ctx.IsLiteralOnly {
				stats.LiteralPrimitives++
			} else {
				stats.RegexPrimitives++
			}
		}
		return nil
	}
}

// CreateFieldTrackingHook creates a hook for tracking unique fields
func CreateFieldTrackingHook(fields *map[string]bool) CompilationHookFn {
	var mutex sync.Mutex
	
	return func(ctx *CompilationContext) error {
		if ctx.Phase == PrimitiveDiscovery {
			mutex.Lock()
			defer mutex.Unlock()
			
			if *fields == nil {
				*fields = make(map[string]bool)
			}
			(*fields)[ctx.FieldName] = true
		}
		return nil
	}
}
