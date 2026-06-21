package plugins

import (
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bazelbuild/bazel-gazelle/config"
	"github.com/bazelbuild/bazel-gazelle/label"
	"github.com/bazelbuild/bazel-gazelle/language"
	"github.com/bazelbuild/bazel-gazelle/repo"
	"github.com/bazelbuild/bazel-gazelle/resolve"
	"github.com/bazelbuild/bazel-gazelle/rule"
)

const (
	rustLangName = "rust"
	rustEdition  = "2024"
)

// Language implements language.Language for Rust.
type Language struct {
	analyzer *RustAnalyzer
}

// NewLanguage creates a new Rust Gazelle language plugin.
func NewLanguage() language.Language {
	return &Language{analyzer: NewRustAnalyzer()}
}

// Name returns the language name.
func (l *Language) Name() string { return rustLangName }

// Kinds returns rule kinds and their merge/resolve metadata.
func (l *Language) Kinds() map[string]rule.KindInfo {
	return map[string]rule.KindInfo{
		"rust_library": {
			MatchAttrs:     []string{"srcs"},
			NonEmptyAttrs:  map[string]bool{"srcs": true},
			ResolveAttrs:   map[string]bool{"deps": true},
			MergeableAttrs: map[string]bool{"srcs": true, "deps": true},
		},
		"rust_binary": {
			MatchAttrs:     []string{"srcs"},
			NonEmptyAttrs:  map[string]bool{"srcs": true},
			ResolveAttrs:   map[string]bool{"deps": true},
			MergeableAttrs: map[string]bool{"srcs": true, "deps": true},
		},
		"rust_test": {
			MatchAttrs:     []string{"srcs"},
			NonEmptyAttrs:  map[string]bool{"srcs": true},
			ResolveAttrs:   map[string]bool{"deps": true},
			MergeableAttrs: map[string]bool{"srcs": true, "deps": true},
		},
	}
}

// Loads returns load statements for generated rules.
func (l *Language) Loads() []rule.LoadInfo {
	return []rule.LoadInfo{
		{
			Name:    "@rules_rust//rust:defs.bzl",
			Symbols: []string{"rust_binary", "rust_library", "rust_test"},
		},
	}
}

// Fix is a no-op.
func (l *Language) Fix(c *config.Config, f *rule.File) {}

// GenerateRules produces Rust rules for a directory:
//   - lib.rs / src/lib.rs   → rust_library
//   - main.rs / src/main.rs → rust_binary
//   - *_test.rs             → rust_test
func (l *Language) GenerateRules(args language.GenerateArgs) language.GenerateResult {
	if !hasRustFiles(args.RegularFiles) {
		return language.GenerateResult{}
	}

	pkgName := toCrateName(filepath.Base(args.Rel))
	deps := l.depsForDir(args.Dir)

	var gen []*rule.Rule

	if src := detectSrc(args.Dir, args.RegularFiles, "lib.rs", "src/lib.rs"); src != "" {
		r := rule.NewRule("rust_library", pkgName)
		if hasSubdirs(args.Dir) {
			r.SetAttr("srcs", rule.GlobValue{Patterns: []string{"**/*.rs"}})
		} else {
			r.SetAttr("srcs", []string{src})
		}
		r.SetAttr("edition", rustEdition)
		if len(deps) > 0 {
			r.SetAttr("deps", deps)
		}
		gen = append(gen, r)
	}

	if src := detectSrc(args.Dir, args.RegularFiles, "main.rs", "src/main.rs"); src != "" {
		r := rule.NewRule("rust_binary", pkgName)
		r.SetAttr("srcs", []string{src})
		r.SetAttr("edition", rustEdition)
		if len(deps) > 0 {
			r.SetAttr("deps", deps)
		}
		gen = append(gen, r)
	}

	var testSrcs []string
	for _, f := range args.RegularFiles {
		if strings.HasSuffix(f, "_test.rs") {
			testSrcs = append(testSrcs, f)
		}
	}
	if len(testSrcs) > 0 {
		r := rule.NewRule("rust_test", pkgName+"_test")
		r.SetAttr("srcs", testSrcs)
		r.SetAttr("edition", rustEdition)
		gen = append(gen, r)
	}

	return language.GenerateResult{Gen: gen, Imports: make([]interface{}, len(gen))}
}

// --- resolve.Resolver -------------------------------------------------------

// Imports returns ImportSpecs for cross-crate resolution.
func (l *Language) Imports(c *config.Config, r *rule.Rule, f *rule.File) []resolve.ImportSpec {
	if r.Kind() == "rust_library" {
		return []resolve.ImportSpec{{Lang: rustLangName, Imp: r.Name()}}
	}
	return nil
}

// Embeds is unused for Rust.
func (l *Language) Embeds(r *rule.Rule, from label.Label) []label.Label { return nil }

// Resolve handles cross-repo dep resolution (deps pre-resolved at generation time).
func (l *Language) Resolve(
	c *config.Config,
	ix *resolve.RuleIndex,
	rc *repo.RemoteCache,
	r *rule.Rule,
	imports interface{},
	from label.Label,
) {
}

// --- config.Configurer ------------------------------------------------------

func (l *Language) RegisterFlags(fs *flag.FlagSet, cmd string, c *config.Config) {}
func (l *Language) CheckFlags(fs *flag.FlagSet, c *config.Config) error          { return nil }
func (l *Language) KnownDirectives() []string                                    { return nil }
func (l *Language) Configure(c *config.Config, rel string, f *rule.File)         {}

// --- helpers ----------------------------------------------------------------

func hasRustFiles(files []string) bool {
	for _, f := range files {
		if strings.HasSuffix(f, ".rs") {
			return true
		}
	}
	return false
}

func hasSubdirs(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() {
			return true
		}
	}
	return false
}

func detectSrc(dir string, files []string, topLevel, subPath string) string {
	for _, f := range files {
		if f == topLevel {
			return topLevel
		}
	}
	return ""
}

func toCrateName(name string) string {
	name = strings.ReplaceAll(name, "-", "_")
	if name == "" || name == "." {
		return "lib"
	}
	return name
}

func (l *Language) depsForDir(dir string) []string {
	resolved, err := l.analyzer.AnalyzeDirectory(dir)
	if err != nil || len(resolved) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(resolved))
	labels := make([]string, 0, len(resolved))
	for _, lbl := range resolved {
		if !seen[lbl] {
			seen[lbl] = true
			labels = append(labels, lbl)
		}
	}
	sort.Strings(labels)
	return labels
}
