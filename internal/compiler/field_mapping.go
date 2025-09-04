// Package compiler provides SIGMA rule compilation functionality.
//
// This module handles the offline compilation of SIGMA YAML rules into
// efficient DAG structures for execution by the DAG engine.
//
// The compiler is organized into several sub-modules:
// - field_mapping - Field name normalization and taxonomy support
// - parser - Tokenization and parsing of SIGMA condition expressions
// - dag_codegen - DAG generation from parsed ASTs
package compiler

// FieldMapping provides field name normalization and taxonomy support.
// This supports the SIGMA taxonomy and custom field mappings.
//
// According to the SIGMA specification, field mappings should be:
// - Rule-driven: The SIGMA rule itself defines what fields it uses
// - Taxonomy-based: Field mappings come from the taxonomy system
// - Configurable: Field mappings should be configurable per deployment
type FieldMapping struct {
	fieldMap map[string]string
	taxonomy string
}

// NewFieldMapping creates a new empty field mapping using the default SIGMA taxonomy.
//
// Field mappings should be configured based on the deployment environment
// and the specific taxonomy being used.
func NewFieldMapping() *FieldMapping {
	return &FieldMapping{
		fieldMap: make(map[string]string),
		taxonomy: "sigma",
	}
}

// WithTaxonomy creates a new field mapping with a specific taxonomy.
func WithTaxonomy(taxonomy string) *FieldMapping {
	return &FieldMapping{
		fieldMap: make(map[string]string),
		taxonomy: taxonomy,
	}
}

// LoadTaxonomyMappings loads field mappings from a taxonomy configuration.
//
// This would typically be loaded from a configuration file or database
// based on the deployment environment.
func (fm *FieldMapping) LoadTaxonomyMappings(mappings map[string]string) {
	for k, v := range mappings {
		fm.fieldMap[k] = v
	}
}

// AddMapping adds a custom field mapping.
func (fm *FieldMapping) AddMapping(sourceField, targetField string) {
	fm.fieldMap[sourceField] = targetField
}

// Taxonomy returns the current taxonomy name.
func (fm *FieldMapping) Taxonomy() string {
	return fm.taxonomy
}

// SetTaxonomy sets the taxonomy name.
func (fm *FieldMapping) SetTaxonomy(taxonomy string) {
	fm.taxonomy = taxonomy
}

// NormalizeField normalizes a field name according to the mapping.
//
// Returns the normalized field name, or the original if no mapping exists.
//
// According to SIGMA spec, if no mapping exists, the field name should be used as-is
// from the rule, following the principle that rules define their own field usage.
func (fm *FieldMapping) NormalizeField(fieldName string) string {
	if mapped, exists := fm.fieldMap[fieldName]; exists {
		return mapped
	}
	return fieldName
}

// HasMapping checks if a field mapping exists for the given field name.
func (fm *FieldMapping) HasMapping(fieldName string) bool {
	_, exists := fm.fieldMap[fieldName]
	return exists
}

// Mappings returns all configured field mappings.
func (fm *FieldMapping) Mappings() map[string]string {
	return fm.fieldMap
}
