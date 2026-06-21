package plugins

import (
	"testing"
)

// TestFindCargoRoot tests finding the Cargo.toml root
func TestFindCargoRoot(t *testing.T) {
	// Test finding root from current directory
	root, err := FindCargoRoot(".")
	if err != nil {
		t.Logf("Warning: Could not find Cargo root: %v (expected if not in a Rust project)", err)
		return
	}

	if root == "" {
		t.Error("Expected non-empty root")
	}
}

// TestNewCrateDependencyResolverFromCargo tests creating a resolver from cargo metadata
func TestNewCrateDependencyResolverFromCargo(t *testing.T) {
	root, err := FindCargoRoot("../..")
	if err != nil {
		t.Skipf("Skipping: not in a Rust workspace: %v", err)
	}

	resolver := NewCrateDependencyResolverFromCargo(root)
	if resolver == nil {
		t.Error("Expected non-nil resolver")
	}

	// The resolver should have at least the default crates
	if len(resolver.crateMap) == 0 {
		t.Error("Expected crate map to have entries")
	}

	// Test that common crates are resolvable (either from cargo or hardcoded)
	testCrates := []string{"tokio", "serde", "anyhow"}
	for _, crate := range testCrates {
		label, exists := resolver.ResolveCrate(crate)
		if !exists {
			t.Errorf("Expected crate %s to be resolvable", crate)
		}
		if label == "" {
			t.Errorf("Expected non-empty label for crate %s", crate)
		}
	}
}

// TestCargoResolverFallback tests that the resolver falls back gracefully
func TestCargoResolverFallback(t *testing.T) {
	// Use a non-existent path to force fallback to hardcoded mappings
	resolver := NewCrateDependencyResolverFromCargo("/nonexistent/path/that/does/not/exist")
	if resolver == nil {
		t.Error("Expected non-nil resolver even with fallback")
	}

	// Should still have hardcoded mappings as fallback
	label, exists := resolver.ResolveCrate("tokio")
	if !exists {
		t.Error("Expected hardcoded fallback for tokio")
	}
	if label != "@crates//:tokio" {
		t.Errorf("Expected @crates//:tokio, got %s", label)
	}
}
