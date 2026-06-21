package plugins

import (
	"os"
	"path/filepath"
	"strings"
)

// RustAnalyzer provides utilities for analyzing Rust code and generating BUILD file dependencies
type RustAnalyzer struct {
	parser   *ImportParser
	resolver *CrateDependencyResolver
}

// NewRustAnalyzer creates a new analyzer with hardcoded crate mappings
func NewRustAnalyzer() *RustAnalyzer {
	return &RustAnalyzer{
		parser:   NewImportParser(),
		resolver: NewCrateDependencyResolver(),
	}
}

// NewRustAnalyzerWithCargo creates a new analyzer that queries cargo for dependencies
func NewRustAnalyzerWithCargo(workspaceRoot string) *RustAnalyzer {
	return &RustAnalyzer{
		parser:   NewImportParser(),
		resolver: NewCrateDependencyResolverFromCargo(workspaceRoot),
	}
}

// AnalyzeDirectory analyzes all Rust files in a directory and returns resolved dependencies
func (a *RustAnalyzer) AnalyzeDirectory(dir string) (map[string]string, error) {
	allCrates := make(map[string]bool)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		// Skip subdirectories — they are separate Bazel packages and
		// Gazelle will invoke GenerateRules on them independently.
		if info.IsDir() && path != dir {
			return filepath.SkipDir
		}
		if !info.IsDir() && strings.HasSuffix(path, ".rs") {
			content, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			crates := a.parser.ExtractCrateNames(string(content))
			for crate := range crates {
				allCrates[crate] = true
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for crate := range allCrates {
		label, _ := a.resolver.ResolveCrate(crate)
		result[crate] = label
	}

	return result, nil
}

// AnalyzeFile analyzes a single Rust file
func (a *RustAnalyzer) AnalyzeFile(filePath string) (map[string]string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	crates := a.parser.ExtractCrateNames(string(content))
	result := make(map[string]string)
	for crate := range crates {
		label, _ := a.resolver.ResolveCrate(crate)
		result[crate] = label
	}

	return result, nil
}

// GetBazelDeps converts resolved dependencies to Bazel format
func (a *RustAnalyzer) GetBazelDeps(crates map[string]string) []string {
	depMap := make(map[string]bool)
	for _, label := range crates {
		depMap[label] = true
	}

	deps := make([]string, 0, len(depMap))
	for dep := range depMap {
		deps = append(deps, dep)
	}

	// Sort
	for i := 0; i < len(deps); i++ {
		for j := i + 1; j < len(deps); j++ {
			if deps[j] < deps[i] {
				deps[i], deps[j] = deps[j], deps[i]
			}
		}
	}
	return deps
}

// GenerateBazelBuildFile generates a Bazel BUILD file snippet for Rust
func (a *RustAnalyzer) GenerateBazelBuildFile(crateName string, crates map[string]string) string {
	deps := a.GetBazelDeps(crates)

	output := `load("@rules_rust//rust:defs.bzl", "rust_library")

rust_library(
    name = "lib",
    srcs = glob(["**/*.rs"]),
    crate_name = "` + crateName + `",
    visibility = ["//visibility:public"],
`

	if len(deps) > 0 {
		output += `    deps = [
`
		for _, dep := range deps {
			output += `        "` + dep + `",
`
		}
		output += `    ],
`
	}

	output += `)
`

	return output
}

// RustLang is a placeholder for gazelle plugin support
type RustLang struct {
	analyzer *RustAnalyzer
}

// NewRustLang creates a new Rust analyzer
func NewRustLang() *RustLang {
	return &RustLang{
		analyzer: NewRustAnalyzer(),
	}
}

// Name returns the name
func (rl *RustLang) Name() string {
	return "rust"
}

// Kinds returns supported kinds
func (rl *RustLang) Kinds() map[string]interface{} {
	return map[string]interface{}{
		"rust_library": true,
		"rust_binary":  true,
	}
}

// Loads returns required loads
func (rl *RustLang) Loads() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name":    "@rules_rust//rust:defs.bzl",
			"symbols": []string{"rust_library", "rust_binary"},
		},
	}
}
