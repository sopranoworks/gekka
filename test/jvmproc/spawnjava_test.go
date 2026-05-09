/*
 * spawnjava_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"reflect"
	"testing"
)

func TestBuildJavaArgs(t *testing.T) {
	tests := []struct {
		name      string
		jar       string
		mainClass string
		mainArgs  []string
		jvmFlags  []string
		want      []string
	}{
		{
			name:      "no flags, no args",
			jar:       "/tmp/foo.jar",
			mainClass: "com.example.Main",
			want:      []string{"-cp", "/tmp/foo.jar", "com.example.Main"},
		},
		{
			name:      "with flags",
			jar:       "/tmp/foo.jar",
			mainClass: "com.example.Main",
			jvmFlags:  []string{"-Xmx2g", "-Dfoo=bar"},
			want:      []string{"-Xmx2g", "-Dfoo=bar", "-cp", "/tmp/foo.jar", "com.example.Main"},
		},
		{
			name:      "with main args",
			jar:       "/tmp/foo.jar",
			mainClass: "com.example.SBRTestNode",
			mainArgs:  []string{"keep-oldest"},
			want:      []string{"-cp", "/tmp/foo.jar", "com.example.SBRTestNode", "keep-oldest"},
		},
		{
			name:      "flags and main args",
			jar:       "/tmp/foo.jar",
			mainClass: "com.example.Main",
			jvmFlags:  []string{"-Xmx2g"},
			mainArgs:  []string{"a", "b"},
			want:      []string{"-Xmx2g", "-cp", "/tmp/foo.jar", "com.example.Main", "a", "b"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildJavaArgs(tc.jar, tc.mainClass, tc.mainArgs, tc.jvmFlags)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
