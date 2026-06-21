package plugins

import (
	"testing"
)

func TestImportParser_ExtractCrateNames(t *testing.T) {
	parser := NewImportParser()

	tests := []struct {
		name     string
		source   string
		expected map[string]bool
	}{
		{
			name: "simple use statement",
			source: `use tokio::runtime;
fn main() {}`,
			expected: map[string]bool{"tokio": true},
		},
		{
			name: "multiple uses",
			source: `use tokio::runtime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;`,
			expected: map[string]bool{
				"tokio": true,
				"serde": true,
				// std should be filtered out
			},
		},
		{
			name: "aliased use",
			source: `use tokio as rt;
use serde::Serialize as S;`,
			expected: map[string]bool{
				"tokio": true,
				"serde": true,
			},
		},
		{
			name: "wildcard use",
			source: `use tokio::*;
use tonic::transport::*;`,
			expected: map[string]bool{
				"tokio": true,
				"tonic": true,
			},
		},
		{
			name: "with comments",
			source: `// Comment with use ignored::crate
use tokio::runtime; // inline comment
/* block comment with use fake::crate */
use serde::Deserialize;`,
			expected: map[string]bool{
				"tokio": true,
				"serde": true,
			},
		},
		{
			name: "nested modules",
			source: `use std::collections::HashMap;
use kube::api::ObjectMeta;
use k8s_openapi::api::core::v1::Pod;`,
			expected: map[string]bool{
				"kube":        true,
				"k8s_openapi": true,
			},
		},
		{
			name: "self/super/crate keywords",
			source: `use self::config;
use super::parent;
use crate::types;`,
			expected: map[string]bool{}, // All should be filtered
		},
		{
			name: "brace imports",
			source: `use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    runtime::Runtime,
};`,
			expected: map[string]bool{"tokio": true},
		},
		{
			name: "underscores to hyphens",
			source: `use tokio_stream::StreamExt;
use serde_json::json;`,
			expected: map[string]bool{
				"tokio_stream": true,
				"serde_json":   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parser.ExtractCrateNames(tt.source)

			// Check for expected crates
			for expected := range tt.expected {
				if !result[expected] {
					t.Errorf("Expected crate %q not found", expected)
				}
			}

			// Check no unexpected crates (except allowed ones)
			allowedStdLibs := map[string]bool{
				"std":        true,
				"core":       true,
				"alloc":      true,
				"proc_macro": true,
			}
			for found := range result {
				if !tt.expected[found] && !allowedStdLibs[found] {
					t.Errorf("Unexpected crate %q found", found)
				}
			}
		})
	}
}

func TestImportParser_stripComments(t *testing.T) {
	parser := NewImportParser()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "line comment",
			input:    "use foo; // this is a comment",
			expected: "use foo; ",
		},
		{
			name:     "block comment",
			input:    "use foo; /* block comment */ use bar;",
			expected: "use foo;  use bar;",
		},
		{
			name:     "no comments",
			input:    "use foo; use bar;",
			expected: "use foo; use bar;",
		},
		{
			name:     "multiline block comment",
			input:    "use foo;\n/* comment\nspanning\nlines */\nuse bar;",
			expected: "use foo;\n\nuse bar;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parser.stripComments(tt.input)
			if result != tt.expected {
				t.Errorf("Got %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestCrateDependencyResolver_ResolveCrate(t *testing.T) {
	resolver := NewCrateDependencyResolver()

	tests := []struct {
		name     string
		crate    string
		expected string
		found    bool
	}{
		{
			name:     "known crate",
			crate:    "tokio",
			expected: "@crates//:tokio",
			found:    true,
		},
		{
			name:     "crate with underscores",
			crate:    "tokio_stream",
			expected: "@crates//:tokio-stream",
			found:    true,
		},
		{
			name:     "unknown crate defaults to crates",
			crate:    "custom_lib",
			expected: "@crates//:custom-lib",
			found:    true,
		},
		{
			name:     "k8s_openapi",
			crate:    "k8s_openapi",
			expected: "@crates//:k8s-openapi",
			found:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := resolver.ResolveCrate(tt.crate)
			if found != tt.found {
				t.Errorf("Expected found=%v, got %v", tt.found, found)
			}
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestCrateDependencyResolver_AddCrate(t *testing.T) {
	resolver := NewCrateDependencyResolver()

	resolver.AddCrate("my_crate", "@//my/package")
	result, found := resolver.ResolveCrate("my_crate")

	if !found {
		t.Errorf("Expected to find added crate")
	}
	if result != "@//my/package" {
		t.Errorf("Expected @//my/package, got %q", result)
	}
}

func TestIsValidCrateName(t *testing.T) {
	tests := []struct {
		name      string
		crateName string
		valid     bool
	}{
		{"valid name", "tokio", true},
		{"valid with underscores", "tokio_stream", true},
		{"crate keyword", "crate", false},
		{"self keyword", "self", false},
		{"super keyword", "super", false},
		{"std", "std", false},
		{"core", "core", false},
		{"empty string", "", false},
		{"alloc", "alloc", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidCrateName(tt.crateName)
			if result != tt.valid {
				t.Errorf("isValidCrateName(%q) = %v, expected %v", tt.crateName, result, tt.valid)
			}
		})
	}
}
