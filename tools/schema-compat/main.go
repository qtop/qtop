// Copyright (c) 2026 qtop contributors
// SPDX-License-Identifier: MIT

// Command schema-compat checks that a proposed CUE schema subsumes its baseline.
package main

import (
	"flag"
	"fmt"
	"os"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/parser"
	"cuelang.org/go/cue/token"
)

// compile treats required constraints as regular fields for concrete JSON data.
func compile(ctx *cue.Context, source []byte, filename, definition string) (cue.Value, error) {
	file, err := parser.ParseFile(filename, source)
	if err != nil {
		return cue.Value{}, err
	}
	ast.Walk(file, func(node ast.Node) bool {
		if field, ok := node.(*ast.Field); ok && field.Constraint == token.NOT {
			field.Constraint = token.ILLEGAL
		}
		return true
	}, nil)
	value := ctx.BuildFile(file).LookupPath(cue.ParsePath(definition))
	return value, value.Validate()
}

// subsumes supplements CUE's API with explicit openness checks, including nested
// records and version unions. Indeterminate or excessively recursive checks fail.
func subsumes(next, old cue.Value, depth int) error {
	if depth > 64 {
		return fmt.Errorf("schema nesting exceeds comparison limit")
	}
	if op, branches := old.Expr(); op == cue.OrOp {
		for _, branch := range branches {
			if err := subsumes(next, branch, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if op, branches := next.Expr(); op == cue.OrOp {
		for _, branch := range branches {
			if subsumes(branch, old, depth+1) == nil {
				return nil
			}
		}
		return fmt.Errorf("no proposed branch subsumes a baseline branch")
	}
	if err := next.Subsume(old, cue.Raw()); err != nil {
		return err
	}
	if old.IncompleteKind() == cue.StructKind {
		if old.Allows(cue.AnyString) && !next.Allows(cue.AnyString) {
			return fmt.Errorf("open baseline record becomes closed")
		}
		fields, err := old.Fields(cue.Optional(true))
		if err != nil {
			return err
		}
		for fields.Next() {
			child := next.LookupPath(cue.MakePath(fields.Selector()))
			if child.Exists() && fields.Value().Err() == nil {
				if err := subsumes(child, fields.Value(), depth+1); err != nil {
					return fmt.Errorf("%s: %w", fields.Selector(), err)
				}
			}
		}
	}
	if old.IncompleteKind() == cue.ListKind {
		if old.Allows(cue.AnyIndex) {
			if err := subsumes(next.LookupPath(cue.MakePath(cue.AnyIndex)), old.LookupPath(cue.MakePath(cue.AnyIndex)), depth+1); err != nil {
				return err
			}
		}
		items, err := old.List()
		if err != nil {
			return err
		}
		for i := 0; items.Next(); i++ {
			if err := subsumes(next.LookupPath(cue.MakePath(cue.Index(i))), items.Value(), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func compare(baseline, proposed []byte, definition string) error {
	ctx := cuecontext.New()
	old, err := compile(ctx, baseline, "baseline.cue", definition)
	if err != nil {
		return fmt.Errorf("invalid baseline: %w", err)
	}
	next, err := compile(ctx, proposed, "proposed.cue", definition)
	if err != nil {
		return fmt.Errorf("invalid proposed schema: %w", err)
	}
	if err := subsumes(next, old, 0); err != nil {
		return fmt.Errorf("backward compatibility not established: %w", err)
	}
	return nil
}

func run() error {
	baseline := flag.String("base", "", "baseline CUE file")
	proposed := flag.String("proposed", "", "proposed CUE file")
	definition := flag.String("definition", "#ClusterState", "definition to compare in both files")
	flag.Parse()
	if *baseline == "" || *proposed == "" || flag.NArg() != 0 {
		return fmt.Errorf("usage: schema-compat -base OLD.cue -proposed NEW.cue [-definition '#ClusterState']")
	}
	old, err := os.ReadFile(*baseline)
	if err != nil {
		return err
	}
	next, err := os.ReadFile(*proposed)
	if err != nil {
		return err
	}
	if err := compare(old, next, *definition); err != nil {
		return err
	}
	fmt.Println("PASS: proposed schema subsumes baseline")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
