package plugins

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// CargoMetadata represents the output of `cargo metadata`
type CargoMetadata struct {
	Packages []Package `json:"packages"`
	Resolve  Resolve   `json:"resolve"`
}

// Package represents a Cargo package
type Package struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Version  string `json:"version"`
	Manifest string `json:"manifest_path"`
}

// Resolve represents the resolved dependency graph
type Resolve struct {
	Nodes []Node `json:"nodes"`
}

// Node represents a resolved node in the dependency graph
type Node struct {
	ID   string `json:"id"`
	Deps []Dep  `json:"deps"`
}

// Dep represents a resolved dependency
type Dep struct {
	Name string `json:"name"`
}

// CargoResolver queries cargo metadata to dynamically build the crate map
type CargoResolver struct {
	workspaceRoot string
	crateMap      map[string]string
}

// NewCargoResolver creates a resolver for a given workspace
func NewCargoResolver(workspaceRoot string) (*CargoResolver, error) {
	cr := &CargoResolver{
		workspaceRoot: workspaceRoot,
		crateMap:      make(map[string]string),
	}

	if err := cr.loadFromCargo(); err != nil {
		return nil, err
	}

	return cr, nil
}

// loadFromCargo queries cargo metadata and builds the crate map
func (cr *CargoResolver) loadFromCargo() error {
	// Run cargo metadata from the workspace root
	cmd := exec.Command("cargo", "metadata", "--format-version", "1")
	cmd.Dir = cr.workspaceRoot

	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to run cargo metadata: %w", err)
	}

	var metadata CargoMetadata
	if err := json.Unmarshal(output, &metadata); err != nil {
		return fmt.Errorf("failed to parse cargo metadata: %w", err)
	}

	// Build a map of package ID -> package name
	pkgMap := make(map[string]string)
	for _, pkg := range metadata.Packages {
		// Normalize package name: hyphens become underscores in Rust code
		normalizedName := strings.ReplaceAll(pkg.Name, "-", "_")
		pkgMap[pkg.ID] = normalizedName
	}

	// Extract all dependencies from the resolve graph
	for _, node := range metadata.Resolve.Nodes {
		for _, dep := range node.Deps {
			// Normalize underscore to hyphen for Bazel label
			bazelName := strings.ReplaceAll(dep.Name, "_", "-")
			cr.crateMap[dep.Name] = fmt.Sprintf("@crates//:%s", bazelName)
		}
	}

	return nil
}

// GetCrateMap returns the resolved crate map
func (cr *CargoResolver) GetCrateMap() map[string]string {
	return cr.crateMap
}

// FindCargoRoot finds the Cargo.toml root by walking up the directory tree
func FindCargoRoot(startPath string) (string, error) {
	path := startPath
	for {
		cargoPath := filepath.Join(path, "Cargo.toml")
		if _, err := os.Stat(cargoPath); err == nil {
			return path, nil
		}

		parent := filepath.Dir(path)
		if parent == path {
			// Reached root
			return "", fmt.Errorf("no Cargo.toml found above %s", startPath)
		}
		path = parent
	}
}
