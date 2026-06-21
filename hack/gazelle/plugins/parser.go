package plugins

import (
	"regexp"
	"strings"
)

// ParseRustImports parses Rust use statements from source code.
type ImportParser struct {
	// Patterns to match various use statement forms
	simpleUse   *regexp.Regexp
	braceUse    *regexp.Regexp
	aliasedUse  *regexp.Regexp
	wildcardUse *regexp.Regexp
	// Pattern to match bare crate path usages (e.g. tokio::runtime::Builder)
	barePath *regexp.Regexp
}

// NewImportParser creates a new Rust import parser.
func NewImportParser() *ImportParser {
	return &ImportParser{
		// Match: use foo::bar;
		simpleUse: regexp.MustCompile(`(?m)^\s*use\s+([a-z_][a-z0-9_]*(?:::[a-zA-Z_][a-zA-Z0-9_:]*)?)\s*;`),
		// Match: use foo::{ bar, baz };
		braceUse: regexp.MustCompile(`(?m)^\s*use\s+([a-z_][a-z0-9_]*)\s*::\s*\{`),
		// Match: use foo as bar;
		aliasedUse: regexp.MustCompile(`(?m)^\s*use\s+([a-z_][a-z0-9_]*)\s+as\s+[a-zA-Z_]`),
		// Match: use foo::*;
		wildcardUse: regexp.MustCompile(`(?m)^\s*use\s+([a-z_][a-z0-9_]*(?:::[a-zA-Z0-9_:]+)?)\s*::\s*\*`),
		// Require the identifier to NOT be preceded by :: (which would make it
		// a submodule of an already-matched path, not a root crate name).
		barePath: regexp.MustCompile(`(?:[^a-zA-Z0-9_:]|^)([a-z][a-z0-9_]*)::(?:[A-Z]|[a-z][a-z0-9_]*::)`),
	}
}

// ExtractCrateNames extracts the root crate names from Rust source.
func (p *ImportParser) ExtractCrateNames(source string) map[string]bool {
	crates := make(map[string]bool)

	// Skip comments
	source = p.stripComments(source)

	lines := strings.Split(source, "\n")
	insideBraceImport := false
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// Track if we're inside a multiline brace import
		if strings.Contains(trimmedLine, "use ") && strings.Contains(trimmedLine, "{") {
			insideBraceImport = true
		}
		if insideBraceImport && strings.Contains(trimmedLine, "}") {
			insideBraceImport = false
		}

		// Simple use statement: use foo::bar;
		if matches := p.simpleUse.FindStringSubmatch(trimmedLine); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
			}
		}

		// Brace use: use foo::{ bar, baz };
		if matches := p.braceUse.FindStringSubmatch(trimmedLine); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
			}
		}

		// Aliased use: use foo as bar;
		if matches := p.aliasedUse.FindStringSubmatch(trimmedLine); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
			}
		}

		// Wildcard use: use foo::*;
		if matches := p.wildcardUse.FindStringSubmatch(trimmedLine); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
			}
		}

		// Bare path usages: tokio::runtime, anyhow::Result, etc.
		// Use a non-word-or-colon prefix to avoid matching submodule names
		// inside a longer path (e.g. `manager` in `kubelet_app::manager::X`).
		// Skip bare path matching if we're inside a multiline brace import.
		if !insideBraceImport {
			for _, m := range p.barePath.FindAllStringSubmatch(trimmedLine, -1) {
				if isValidCrateName(m[1]) {
					crates[m[1]] = true
				}
			}
		}
	}

	return crates
}

// stripComments removes single-line and block comments.
func (p *ImportParser) stripComments(source string) string {
	// Remove line comments (//)
	lineCommentRe := regexp.MustCompile(`//.*$`)
	source = lineCommentRe.ReplaceAllString(source, "")

	// Remove block comments (/* ... */) - use (?s) flag to match across newlines
	blockCommentRe := regexp.MustCompile(`(?s)/\*.*?\*/`)
	source = blockCommentRe.ReplaceAllString(source, "")

	return source
}

// extractRootCrate extracts the root crate name from a potentially nested path.
// E.g., "tokio::runtime" -> "tokio"
func (p *ImportParser) extractRootCrate(path string) string {
	parts := strings.Split(path, "::")
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}

// isValidCrateName checks if a name is a valid crate import (not std/self/super/crate).
func isValidCrateName(name string) bool {
	switch name {
	case "", "crate", "self", "super", "std", "alloc", "core", "proc_macro",
		// Common submodule names that appear as bare paths but are not root crates
		"fmt", "io", "fs", "net", "sync", "ops", "str", "mem", "ptr",
		"time", "hash", "iter", "num", "env", "path", "convert", "collections",
		"result", "option", "error", "any", "clone", "cmp", "marker", "default",
		"borrow", "cell", "rc", "arc", "pin", "future", "task", "async":
		return false
	}
	return true
}

// CrateDependencyResolver resolves crate names to Bazel labels.
type CrateDependencyResolver struct {
	crateMap map[string]string
}

// NewCrateDependencyResolver creates a new resolver with default hardcoded mappings.
func NewCrateDependencyResolver() *CrateDependencyResolver {
	return &CrateDependencyResolver{
		crateMap: defaultCrateMap(),
	}
}

// NewCrateDependencyResolverFromCargo creates a resolver by querying cargo metadata.
// Falls back to hardcoded mappings if cargo metadata fails or if a crate isn't found.
func NewCrateDependencyResolverFromCargo(workspaceRoot string) *CrateDependencyResolver {
	resolver := &CrateDependencyResolver{
		crateMap: defaultCrateMap(), // Start with defaults as fallback
	}

	// Try to load from cargo metadata
	cargoResolver, err := NewCargoResolver(workspaceRoot)
	if err == nil {
		// Merge cargo dependencies into the map (cargo takes precedence)
		for name, label := range cargoResolver.GetCrateMap() {
			resolver.crateMap[name] = label
		}
	}
	// If cargo fails, we silently fall back to hardcoded mappings

	return resolver
}

// ResolveCrate returns the Bazel label for a crate name.
func (r *CrateDependencyResolver) ResolveCrate(crateName string) (string, bool) {
	// Try exact match
	if label, exists := r.crateMap[crateName]; exists {
		return label, true
	}

	// Try with underscores replaced by hyphens
	normalized := strings.ReplaceAll(crateName, "_", "-")
	if label, exists := r.crateMap[normalized]; exists {
		return label, true
	}

	// Default: assume it's in @crates
	return "@crates//:" + normalized, true
}

// AddCrate adds or updates a crate mapping.
func (r *CrateDependencyResolver) AddCrate(crateName, label string) {
	r.crateMap[crateName] = label
}

// defaultCrateMap provides the default mapping for KubeAir dependencies.
func defaultCrateMap() map[string]string {
	return map[string]string{
		// Local workspace crates
		"kubelet_core":     "//crates/kubelet-core/src:lib",
		"kubelet_ports":    "//crates/kubelet-ports/src:lib",
		"kubelet_adapters": "//crates/kubelet-adapters/src:lib",
		"kubelet_app":      "//crates/kubelet-app/src:lib",
		"kubelet_cri":      "//crates/kubelet-cri/src:lib",
		// External crates whose names use underscores (not hyphens) in Bazel labels
		"serde_json":        "@crates//:serde_json",
		"serde_yaml":        "@crates//:serde_yaml",
		"serde_value":       "@crates//:serde_value",
		"num_cpus":          "@crates//:num_cpus",
		"parking_lot":       "@crates//:parking_lot",
		"once_cell":         "@crates//:once_cell",
		"prost_types":       "@crates//:prost-types",
		"tikv_jemallocator": "@crates//:tikv-jemallocator",
	}
}
