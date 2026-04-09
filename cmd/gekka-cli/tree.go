/*
 * tree.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"fmt"
	"sort"
	"strings"
)

// TreeNode is a node in a dotted-key config tree.  Internal nodes have
// non-empty Children; leaves have a non-nil Value.
type TreeNode struct {
	Name     string
	Value    any
	Children map[string]*TreeNode
}

// Find returns the direct child named name, or nil if absent.
func (n *TreeNode) Find(name string) *TreeNode {
	if n == nil || n.Children == nil {
		return nil
	}
	return n.Children[name]
}

// BuildTree constructs a tree from a flat dotted-key map.
// Keys are split on '.'; values become leaves.
func BuildTree(entries map[string]any) *TreeNode {
	root := &TreeNode{Children: map[string]*TreeNode{}}
	for key, val := range entries {
		parts := strings.Split(key, ".")
		cur := root
		for i, p := range parts {
			child, ok := cur.Children[p]
			if !ok {
				child = &TreeNode{Name: p, Children: map[string]*TreeNode{}}
				cur.Children[p] = child
			}
			if i == len(parts)-1 {
				child.Value = val
			}
			cur = child
		}
	}
	return root
}

// FilterTree returns the subtree rooted at prefix (a dotted path), or nil if
// the prefix does not exist.  An empty prefix returns root unchanged.
func FilterTree(root *TreeNode, prefix string) *TreeNode {
	if prefix == "" {
		return root
	}
	cur := root
	for _, p := range strings.Split(prefix, ".") {
		cur = cur.Find(p)
		if cur == nil {
			return nil
		}
	}
	// Wrap in a synthetic root so RenderTree treats cur as a top-level child.
	wrap := &TreeNode{Children: map[string]*TreeNode{cur.Name: cur}}
	return wrap
}

// RenderTree returns one string per line of the tree, suitable for a viewport.
func RenderTree(root *TreeNode) []string {
	if root == nil {
		return nil
	}
	var lines []string
	keys := sortedKeys(root.Children)
	for i, k := range keys {
		child := root.Children[k]
		last := i == len(keys)-1
		renderNode(&lines, child, "", last)
	}
	return lines
}

func renderNode(lines *[]string, n *TreeNode, indent string, last bool) {
	var connector, nextIndent string
	if last {
		connector = "└── "
		nextIndent = indent + "    "
	} else {
		connector = "├── "
		nextIndent = indent + "│   "
	}
	line := indent + connector + n.Name
	if n.Value != nil && len(n.Children) == 0 {
		line += fmt.Sprintf(" = %v", n.Value)
	}
	*lines = append(*lines, line)
	keys := sortedKeys(n.Children)
	for i, k := range keys {
		childLast := i == len(keys)-1
		renderNode(lines, n.Children[k], nextIndent, childLast)
	}
}

func sortedKeys(m map[string]*TreeNode) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
