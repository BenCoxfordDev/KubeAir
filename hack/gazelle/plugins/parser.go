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
}

// NewImportParser creates a new Rust import parser.
func NewImportParser() *ImportParser {
	return &ImportParser{
		// Match: use foo::bar;
		simpleUse: regexp.MustCompile(`use\s+([a-z_][a-z0-9_]*(?:::[a-z_][a-z0-9_:]*)?)\s*;`),
		// Match: use foo::{ bar, baz };
		braceUse: regexp.MustCompile(`use\s+([a-z_][a-z0-9_]*)\s*::\s*\{([^}]+)\}`),
		// Match: use foo as bar;
		aliasedUse: regexp.MustCompile(`use\s+([a-z_][a-z0-9_]*)\s+as\s+[a-z_][a-z0-9_]*`),
		// Match: use foo::*;
		wildcardUse: regexp.MustCompile(`use\s+([a-z_][a-z0-9_]*(?:::[a-z0-9_:]+)?)\s*::\s*\*`),
	}
}

// ExtractCrateNames extracts the root crate names from Rust source.
func (p *ImportParser) ExtractCrateNames(source string) map[string]bool {
	crates := make(map[string]bool)

	// Skip comments
	source = p.stripComments(source)

	lines := strings.Split(source, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Simple use statement: use foo::bar;
		if matches := p.simpleUse.FindStringSubmatch(line); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
			}
		}

		// Brace use: use foo::{ bar, baz };
		if matches := p.braceUse.FindStringSubmatch(line); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
			}
		}

		// Aliased use: use foo as bar;
		if matches := p.aliasedUse.FindStringSubmatch(line); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
			}
		}

		// Wildcard use: use foo::*;
		if matches := p.wildcardUse.FindStringSubmatch(line); matches != nil {
			crate := p.extractRootCrate(matches[1])
			if isValidCrateName(crate) {
				crates[crate] = true
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

	// Remove block comments (/* ... */)
	blockCommentRe := regexp.MustCompile(`/\*.*?\*/`)
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
	return name != "" &&
		name != "crate" &&
		name != "self" &&
		name != "super" &&
		name != "std" &&
		name != "alloc" &&
		name != "core" &&
		name != "proc_macro"
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
	return map[string]string{}
}
