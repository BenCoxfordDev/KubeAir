package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BenCoxfordDev/kubeair/hack/gazelle/plugins"
)

func main() {
	analyzeDir := flag.String("dir", ".", "Directory to analyze for Rust files")
	outputDeps := flag.Bool("deps", false, "Output resolved dependencies")
	workspaceRoot := flag.String("workspace", "", "Workspace root for cargo metadata (optional)")
	flag.Parse()

	parser := plugins.NewImportParser()

	// Use cargo resolver if workspace is provided, otherwise use hardcoded defaults
	var resolver *plugins.CrateDependencyResolver
	if *workspaceRoot != "" {
		resolver = plugins.NewCrateDependencyResolverFromCargo(*workspaceRoot)
	} else {
		resolver = plugins.NewCrateDependencyResolver()
	}

	// Find all Rust files in the directory
	var rustFiles []string
	filepath.Walk(*analyzeDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && filepath.Ext(path) == ".rs" {
			rustFiles = append(rustFiles, path)
		}
		return nil
	})

	if len(rustFiles) == 0 {
		fmt.Fprintf(os.Stderr, "No Rust files found in %s\n", *analyzeDir)
		os.Exit(1)
	}

	// Extract and aggregate imports from all files
	allCrates := make(map[string]bool)
	for _, file := range rustFiles {
		content, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		crates := parser.ExtractCrateNames(string(content))
		for crate := range crates {
			allCrates[crate] = true
		}
	}

	if *outputDeps {
		fmt.Println("# Dependencies for BUILD file")
		fmt.Println("deps = [")
		for crate := range allCrates {
			label, _ := resolver.ResolveCrate(crate)
			fmt.Printf("    \"%s\",\n", label)
		}
		fmt.Println("]")
	} else {
		// Default: output each crate on its own line
		fmt.Println("# Detected crates:")
		for crate := range allCrates {
			label, _ := resolver.ResolveCrate(crate)
			fmt.Printf("%s -> %s\n", crate, label)
		}
	}
}
