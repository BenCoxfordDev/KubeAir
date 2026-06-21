package plugins

import (
	"testing"
)

// TestRustLangName tests that the language plugin name is correct.
func TestRustLangName(t *testing.T) {
	lang := NewRustLang()
	if lang.Name() != "rust" {
		t.Errorf("Expected name 'rust', got %q", lang.Name())
	}
}

// TestRustLangKinds tests that the plugin handles Rust rule kinds.
func TestRustLangKinds(t *testing.T) {
	lang := NewRustLang()
	kinds := lang.Kinds()

	expectedKinds := []string{"rust_library", "rust_binary"}
	for _, kind := range expectedKinds {
		if _, exists := kinds[kind]; !exists {
			t.Errorf("Expected kind %q not found", kind)
		}
	}
}

// TestRustLangLoads tests that necessary loads are returned.
func TestRustLangLoads(t *testing.T) {
	lang := NewRustLang()
	loads := lang.Loads()

	if len(loads) == 0 {
		t.Errorf("Expected loads, got none")
	}

	found := false
	for _, load := range loads {
		if name, ok := load["name"]; ok && name == "@rules_rust//rust:defs.bzl" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected @rules_rust//rust:defs.bzl load not found")
	}
}

// TestRustAnalyzer tests the analyzer
func TestRustAnalyzer(t *testing.T) {
	analyzer := NewRustAnalyzer()
	if analyzer == nil {
		t.Errorf("Expected analyzer to be created")
	}
}

// TestGenerateBazelBuildFile tests BUILD file generation
func TestGenerateBazelBuildFile(t *testing.T) {
	analyzer := NewRustAnalyzer()

	crates := map[string]string{
		"tokio": "@crates//:tokio",
		"serde": "@crates//:serde",
	}

	buildFile := analyzer.GenerateBazelBuildFile("my_crate", crates)

	if !contains(buildFile, `crate_name = "my_crate"`) {
		t.Errorf("Expected crate_name in BUILD file")
	}

	if !contains(buildFile, "@crates//:serde") {
		t.Errorf("Expected serde dependency in BUILD file")
	}

	if !contains(buildFile, "@crates//:tokio") {
		t.Errorf("Expected tokio dependency in BUILD file")
	}

	if !contains(buildFile, "rust_library") {
		t.Errorf("Expected rust_library in BUILD file")
	}
}

// TestGetBazelDeps tests conversion to Bazel format
func TestGetBazelDeps(t *testing.T) {
	analyzer := NewRustAnalyzer()

	crates := map[string]string{
		"tokio": "@crates//:tokio",
		"serde": "@crates//:serde",
	}

	deps := analyzer.GetBazelDeps(crates)

	if len(deps) != 2 {
		t.Errorf("Expected 2 deps, got %d", len(deps))
	}

	// Check sorted
	if deps[0] != "@crates//:serde" || deps[1] != "@crates//:tokio" {
		t.Errorf("Expected sorted deps, got %v", deps)
	}
}

// Helper function
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
