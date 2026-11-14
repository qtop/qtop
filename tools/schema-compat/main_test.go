// Copyright (c) 2026 qtop contributors
// SPDX-License-Identifier: MIT

package main

import (
	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"encoding/json"
	"os"
	"testing"
)

func TestCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, old, next string
		ok              bool
	}{
		{"identical", "{x: int}", "{x: int}", true},
		{"wider", "{x: int & >0}", "{x: int}", true},
		{"narrower", "{x: int}", "{x: int & >0}", false},
		{"optional addition", "{x: int}", "{x: int, y?: string}", true},
		{"required constraint", "{x: int}", "{x: int, y!: string}", false},
		{"new regular requirement", "{x: int}", "{x: int, y: string}", false},
		{"field removal", "{x: int, y?: string}", "{x: int}", false},
		{"closing records", "{x: int, ...}", "{x: int}", false},
		{"nested closing", "{jobs: [...{id: string, ...}]}", "{jobs: [...{id: string}]}", false},
		{"optional closing", "{node?: {id: string, ...}}", "{node?: {id: string}}", false},
		{"required unchanged", "{x!: int}", "{x!: int}", true},
		{"required relaxed", "{x!: int}", "{x?: int}", true},
		{"defaults do not hide narrowing", "{x: *1 | int}", "{x: 1}", false},
		{"preserved versions", "{v: 1} | {v: 2}", "{v: 1} | {v: 2} | {v: 3}", true},
		{"dropped version", "{v: 1} | {v: 2}", "{v: 3}", false},
		{"invalid schema", "{x: int}", "{x: int & string}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compare([]byte("@experiment(explicitopen)\n#ClusterState: "+tc.old), []byte("@experiment(explicitopen)\n#ClusterState: "+tc.next), "#ClusterState")
			if (err == nil) != tc.ok {
				t.Fatalf("compatible=%v, error=%v", tc.ok, err)
			}
		})
	}
	if compare([]byte("#Other: int"), []byte("#ClusterState: int"), "#ClusterState") == nil {
		t.Fatal("missing baseline definition accepted")
	}
}

func TestCommittedSchema(t *testing.T) {
	data, err := os.ReadFile("../../schema/cluster-state.cue")
	if err != nil {
		t.Fatal(err)
	}
	if err := compare(data, data, "#ClusterState"); err != nil {
		t.Fatal(err)
	}
}

func TestStructuralContract(t *testing.T) {
	data, err := os.ReadFile("../../schema/cluster-state.cue")
	if err != nil {
		t.Fatal(err)
	}
	ctx := cuecontext.New()
	schema, err := compile(ctx, data, "schema.cue", "#ClusterState")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name       string
		State      json.RawMessage
		Structural bool
	}
	corpus, err := os.ReadFile("../../tests/schema_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(corpus, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			err := schema.Subsume(ctx.CompileBytes(tc.State), cue.Final())
			if (err == nil) != tc.Structural {
				t.Fatalf("expected structural=%v, error=%v", tc.Structural, err)
			}
		})
	}
}
