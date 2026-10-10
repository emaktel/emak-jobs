package main

import (
	"bytes"
	"strings"
	"testing"
)

// Every argument list other than --version and check PATH... exits 2 with the usage and never
// reaches the worker.
func TestUnknownArgumentsNeverStartTheWorker(t *testing.T) {
	for _, args := range [][]string{
		{"check"}, {"--version", "x"}, {"-version"}, {"version"}, {"help"}, {"--help"}, {"run"},
		{"-h"}, {""}, {"Check", "testdata/jobs"}, {"--check", "testdata/jobs"},
	} {
		var out, errOut bytes.Buffer
		if code := command(args, &out, &errOut); code != 2 || !strings.HasPrefix(errOut.String(), "usage:") {
			t.Errorf("%q: exit %d, stderr %q", args, code, errOut.String())
		}
	}
}

func TestVersionAndCheck(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := command([]string{"--version"}, &out, &errOut); code != 0 || out.String() != "emak-jobs "+version+"\n" {
		t.Fatalf("--version: %d %q", code, out.String())
	}
	out.Reset()
	if code := command([]string{"check", "testdata/jobs"}, &out, &errOut); code != 0 {
		t.Fatalf("check testdata/jobs: %d\n%s", code, out.String())
	}
	if code := command([]string{"check", "testdata/invalid"}, &out, &errOut); code != 1 {
		t.Fatalf("check testdata/invalid: %d", code)
	}
	if code := command([]string{"check", "/does/not/exist"}, &out, &errOut); code != 2 {
		t.Fatalf("check of a missing path: %d", code)
	}
}
