package compiler

import (
	"fmt"
	"strings"
	"sync"
)

// FieldMapping handles field name normalization and taxonomy support
// This allows mapping between different field naming conventions
type FieldMapping struct {
	// Mapping from source field name to target field name
	mappings map[string]string

	// Reverse mapping for lookups
	reverseMappings map[string]string

	// Taxonomy name (e.g., "sysmon", "windows", "custom_edr")
	taxonomy string

	// Mutex for thread safety
	mutex sync.RWMutex

	// Case sensitivity settings
	caseSensitive bool

	// Default field transformations
	transformations map[string]FieldTransformFn
}

// FieldTransformFn represents a function that transforms field names
type FieldTransformFn func(fieldName string) string

// NewFieldMapping creates a new field mapping instance
func NewFieldMapping() *FieldMapping {
	return &FieldMapping{
		mappings:        make(map[string]string),
		reverseMappings: make(map[string]string),
		taxonomy:        "",
		caseSensitive:   false,
		transformations: make(map[string]FieldTransformFn),
	}
}

// NewFieldMappingWithTaxonomy creates a new field mapping with a specific taxonomy
func NewFieldMappingWithTaxonomy(taxonomy string) *FieldMapping {
	fm := NewFieldMapping()
	fm.taxonomy = taxonomy
	return fm
}

// AddMapping adds a field mapping from source to target
func (fm *FieldMapping) AddMapping(source, target string) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// Normalize field names if not case sensitive
	if !fm.caseSensitive {
		source = strings.ToLower(source)
		target = strings.ToLower(target)
	}

	fm.mappings[source] = target
	fm.reverseMappings[target] = source
}

// RemoveMapping removes a field mapping
func (fm *FieldMapping) RemoveMapping(source string) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	if !fm.caseSensitive {
		source = strings.ToLower(source)
	}

	if target, exists := fm.mappings[source]; exists {
		delete(fm.mappings, source)
		delete(fm.reverseMappings, target)
	}
}

// MapField maps a source field name to its target equivalent
func (fm *FieldMapping) MapField(source string) string {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	originalSource := source
	if !fm.caseSensitive {
		source = strings.ToLower(source)
	}

	// Check direct mapping first
	if target, exists := fm.mappings[source]; exists {
		return target
	}

	// Apply transformations if available
	for pattern, transform := range fm.transformations {
		if strings.Contains(source, pattern) {
			return transform(originalSource)
		}
	}

	// Return original if no mapping found
	return originalSource
}

// HasMapping checks if a mapping exists for the given source field
func (fm *FieldMapping) HasMapping(source string) bool {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	if !fm.caseSensitive {
		source = strings.ToLower(source)
	}

	_, exists := fm.mappings[source]
	return exists
}

// GetReverseMapping gets the source field name for a target field
func (fm *FieldMapping) GetReverseMapping(target string) (string, bool) {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	if !fm.caseSensitive {
		target = strings.ToLower(target)
	}

	source, exists := fm.reverseMappings[target]
	return source, exists
}

// SetTaxonomy sets the taxonomy name
func (fm *FieldMapping) SetTaxonomy(taxonomy string) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	fm.taxonomy = taxonomy
}

// GetTaxonomy returns the current taxonomy name
func (fm *FieldMapping) GetTaxonomy() string {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return fm.taxonomy
}

// SetCaseSensitive sets whether field mapping is case sensitive
func (fm *FieldMapping) SetCaseSensitive(caseSensitive bool) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	if fm.caseSensitive != caseSensitive {
		// Need to rebuild mappings with new case sensitivity
		oldMappings := make(map[string]string)
		for k, v := range fm.mappings {
			oldMappings[k] = v
		}

		fm.mappings = make(map[string]string)
		fm.reverseMappings = make(map[string]string)
		fm.caseSensitive = caseSensitive

		// Rebuild with new case sensitivity
		for source, target := range oldMappings {
			if !caseSensitive {
				source = strings.ToLower(source)
				target = strings.ToLower(target)
			}
			fm.mappings[source] = target
			fm.reverseMappings[target] = source
		}
	}
}

// IsCaseSensitive returns whether field mapping is case sensitive
func (fm *FieldMapping) IsCaseSensitive() bool {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return fm.caseSensitive
}

// AddTransformation adds a field name transformation function
func (fm *FieldMapping) AddTransformation(pattern string, transform FieldTransformFn) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	fm.transformations[pattern] = transform
}

// RemoveTransformation removes a field name transformation
func (fm *FieldMapping) RemoveTransformation(pattern string) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	delete(fm.transformations, pattern)
}

// GetAllMappings returns all current mappings
func (fm *FieldMapping) GetAllMappings() map[string]string {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	// Return a copy to avoid external modifications
	result := make(map[string]string)
	for k, v := range fm.mappings {
		result[k] = v
	}
	return result
}

// MappingCount returns the number of current mappings
func (fm *FieldMapping) MappingCount() int {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return len(fm.mappings)
}

// Clear removes all mappings and transformations
func (fm *FieldMapping) Clear() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	fm.mappings = make(map[string]string)
	fm.reverseMappings = make(map[string]string)
	fm.transformations = make(map[string]FieldTransformFn)
}

// Clone creates a deep copy of the field mapping
func (fm *FieldMapping) Clone() *FieldMapping {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	clone := &FieldMapping{
		mappings:        make(map[string]string),
		reverseMappings: make(map[string]string),
		taxonomy:        fm.taxonomy,
		caseSensitive:   fm.caseSensitive,
		transformations: make(map[string]FieldTransformFn),
	}

	// Copy mappings
	for k, v := range fm.mappings {
		clone.mappings[k] = v
	}
	for k, v := range fm.reverseMappings {
		clone.reverseMappings[k] = v
	}

	// Copy transformations
	for k, v := range fm.transformations {
		clone.transformations[k] = v
	}

	return clone
}

// LoadPresetMappings loads predefined mappings for common taxonomies
func (fm *FieldMapping) LoadPresetMappings(presetName string) error {
	switch strings.ToLower(presetName) {
	case "sysmon":
		fm.loadSysmonMappings()
	case "windows":
		fm.loadWindowsMappings()
	case "linux":
		fm.loadLinuxMappings()
	case "elastic":
		fm.loadElasticMappings()
	default:
		return fmt.Errorf("unknown preset mapping: %s", presetName)
	}
	return nil
}

// loadSysmonMappings loads Sysmon-specific field mappings
func (fm *FieldMapping) loadSysmonMappings() {
	mappings := map[string]string{
		"ProcessImage":       "Image",
		"ProcessCommandLine": "CommandLine",
		"ProcessId":          "ProcessId",
		"ParentProcessImage": "ParentImage",
		"ParentCommandLine":  "ParentCommandLine",
		"User":               "User",
		"TargetFilename":     "TargetFilename",
		"SourceImage":        "SourceImage",
		"TargetImage":        "TargetImage",
	}

	for source, target := range mappings {
		fm.AddMapping(source, target)
	}
	fm.SetTaxonomy("sysmon")
}

// loadWindowsMappings loads Windows Event Log field mappings
func (fm *FieldMapping) loadWindowsMappings() {
	mappings := map[string]string{
		"SubjectUserName":   "Account_Name",
		"TargetUserName":    "Target_Account_Name",
		"ProcessName":       "Process_Name",
		"NewProcessName":    "New_Process_Name",
		"CommandLine":       "Process_Command_Line",
		"ParentProcessName": "Parent_Process_Name",
		"LogonType":         "Logon_Type",
		"IpAddress":         "Source_Network_Address",
		"WorkstationName":   "Workstation_Name",
	}

	for source, target := range mappings {
		fm.AddMapping(source, target)
	}
	fm.SetTaxonomy("windows")
}

// loadLinuxMappings loads Linux/Unix field mappings
func (fm *FieldMapping) loadLinuxMappings() {
	mappings := map[string]string{
		"cmd":  "command",
		"exe":  "executable",
		"pid":  "process_id",
		"ppid": "parent_process_id",
		"uid":  "user_id",
		"gid":  "group_id",
		"comm": "command_name",
	}

	for source, target := range mappings {
		fm.AddMapping(source, target)
	}
	fm.SetTaxonomy("linux")
}

// loadElasticMappings loads Elastic Common Schema (ECS) field mappings
func (fm *FieldMapping) loadElasticMappings() {
	mappings := map[string]string{
		"ProcessImage":       "process.executable",
		"ProcessCommandLine": "process.command_line",
		"ProcessId":          "process.pid",
		"ParentProcessImage": "process.parent.executable",
		"User":               "user.name",
		"SourceIp":           "source.ip",
		"DestinationIp":      "destination.ip",
		"FileName":           "file.name",
		"FilePath":           "file.path",
	}

	for source, target := range mappings {
		fm.AddMapping(source, target)
	}
	fm.SetTaxonomy("elastic")
}

// String returns a string representation of the field mapping
func (fm *FieldMapping) String() string {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	return fmt.Sprintf("FieldMapping{taxonomy=%s, mappings=%d, caseSensitive=%t}",
		fm.taxonomy, len(fm.mappings), fm.caseSensitive)
}

// Common field transformation functions

// CreateCamelCaseTransform creates a transformation to camelCase
func CreateCamelCaseTransform() FieldTransformFn {
	return func(fieldName string) string {
		parts := strings.Split(fieldName, "_")
		if len(parts) <= 1 {
			return fieldName
		}

		result := parts[0]
		for i := 1; i < len(parts); i++ {
			if len(parts[i]) > 0 {
				result += strings.ToUpper(parts[i][:1]) + parts[i][1:]
			}
		}
		return result
	}
}

// CreateSnakeCaseTransform creates a transformation to snake_case
func CreateSnakeCaseTransform() FieldTransformFn {
	return func(fieldName string) string {
		var result strings.Builder
		for i, r := range fieldName {
			if i > 0 && 'A' <= r && r <= 'Z' {
				result.WriteRune('_')
			}
			result.WriteRune(r)
		}
		return strings.ToLower(result.String())
	}
}

// CreateDotNotationTransform creates a transformation to dot.notation
func CreateDotNotationTransform() FieldTransformFn {
	return func(fieldName string) string {
		// Convert underscores and camelCase to dot notation
		fieldName = strings.ReplaceAll(fieldName, "_", ".")

		var result strings.Builder
		for i, r := range fieldName {
			if i > 0 && 'A' <= r && r <= 'Z' {
				result.WriteRune('.')
			}
			result.WriteRune(r)
		}
		return strings.ToLower(result.String())
	}
}

// Default field mapping instance
var defaultFieldMapping = NewFieldMapping()

// GetDefaultFieldMapping returns the default global field mapping
func GetDefaultFieldMapping() *FieldMapping {
	return defaultFieldMapping
}

// SetDefaultFieldMapping sets the default global field mapping
func SetDefaultFieldMapping(fm *FieldMapping) {
	defaultFieldMapping = fm
}
