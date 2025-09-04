package compiler

import (
	"testing"
)

// TestFieldMappingCreation matches Rust test_field_mapping_creation
func TestFieldMappingCreation(t *testing.T) {
	mapping := NewFieldMapping()
	if mapping.Taxonomy() != "sigma" {
		t.Errorf("Expected taxonomy 'sigma', got '%s'", mapping.Taxonomy())
	}
	if len(mapping.Mappings()) != 0 {
		t.Errorf("Expected 0 mappings, got %d", len(mapping.Mappings()))
	}
}

// TestFieldMappingWithTaxonomy matches Rust test_field_mapping_with_taxonomy
func TestFieldMappingWithTaxonomy(t *testing.T) {
	mapping := WithTaxonomy("custom")
	if mapping.Taxonomy() != "custom" {
		t.Errorf("Expected taxonomy 'custom', got '%s'", mapping.Taxonomy())
	}
}

// TestAddMapping matches Rust test_add_mapping
func TestAddMapping(t *testing.T) {
	mapping := NewFieldMapping()
	mapping.AddMapping("Event_ID", "EventID")

	if !mapping.HasMapping("Event_ID") {
		t.Error("Expected mapping to exist for 'Event_ID'")
	}
	if mapping.NormalizeField("Event_ID") != "EventID" {
		t.Errorf("Expected normalized field 'EventID', got '%s'", mapping.NormalizeField("Event_ID"))
	}
}

// TestLoadTaxonomyMappings matches Rust test_load_taxonomy_mappings
func TestLoadTaxonomyMappings(t *testing.T) {
	mapping := NewFieldMapping()
	taxonomyMappings := map[string]string{
		"Event_ID":     "EventID",
		"Process_Name": "Image",
	}

	mapping.LoadTaxonomyMappings(taxonomyMappings)
	if len(mapping.Mappings()) != 2 {
		t.Errorf("Expected 2 mappings, got %d", len(mapping.Mappings()))
	}
	if mapping.NormalizeField("Event_ID") != "EventID" {
		t.Errorf("Expected normalized field 'EventID', got '%s'", mapping.NormalizeField("Event_ID"))
	}
	if mapping.NormalizeField("Process_Name") != "Image" {
		t.Errorf("Expected normalized field 'Image', got '%s'", mapping.NormalizeField("Process_Name"))
	}
}

// TestNormalizeFieldUnmapped matches Rust test_normalize_field_unmapped
func TestNormalizeFieldUnmapped(t *testing.T) {
	mapping := NewFieldMapping()
	if mapping.NormalizeField("UnmappedField") != "UnmappedField" {
		t.Errorf("Expected unmapped field to return original value 'UnmappedField', got '%s'", mapping.NormalizeField("UnmappedField"))
	}
}

// TestSetTaxonomy matches Rust test_set_taxonomy
func TestSetTaxonomy(t *testing.T) {
	mapping := NewFieldMapping()
	mapping.SetTaxonomy("custom")
	if mapping.Taxonomy() != "custom" {
		t.Errorf("Expected taxonomy 'custom', got '%s'", mapping.Taxonomy())
	}
}

// TestDefaultImplementation matches Rust test_default_implementation
func TestDefaultImplementation(t *testing.T) {
	// Go doesn't have a Default trait, but we can test NewFieldMapping()
	mapping := NewFieldMapping()
	if mapping.Taxonomy() != "sigma" {
		t.Errorf("Expected taxonomy 'sigma', got '%s'", mapping.Taxonomy())
	}
	if len(mapping.Mappings()) != 0 {
		t.Errorf("Expected 0 mappings, got %d", len(mapping.Mappings()))
	}
}
