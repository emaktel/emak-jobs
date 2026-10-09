package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// testdata/jobs holds one file for every shape emak_jobs.lua writes; all must be accepted.
func TestCheckAcceptsEveryWriterShape(t *testing.T) {
	var out bytes.Buffer
	bad, err := checkPaths([]string{"testdata/jobs"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatalf("%d fixture(s) refused:\n%s", bad, out.String())
	}
	if n := strings.Count(out.String(), "OK "); n < 10 {
		t.Fatalf("expected every fixture checked, got %d:\n%s", n, out.String())
	}
}

// testdata/invalid: each file is refused by check, and (except unknown fields, which the
// running worker only warns about) refused as invalid, so it goes straight to failed/.
func TestCheckRefusesInvalidJobs(t *testing.T) {
	files, _ := filepath.Glob("testdata/invalid/*.json")
	if len(files) < 15 {
		t.Fatalf("only %d invalid fixtures", len(files))
	}
	for _, f := range files {
		var out bytes.Buffer
		bad, err := checkPaths([]string{f}, &out)
		if err != nil || bad != 1 || !strings.HasPrefix(out.String(), "INVALID ") {
			t.Errorf("%s: bad=%d err=%v out=%q", f, bad, err, out.String())
		}
		if strings.HasSuffix(f, "unknown_field.json") {
			continue
		}
		var inv *invalidJobError
		if _, err := readJob(f); !errors.As(err, &inv) {
			t.Errorf("%s: want an invalid-job error, got %v", f, err)
		}
	}
}

func TestVersionErrorNamesTheVersion(t *testing.T) {
	_, err := readJob("testdata/invalid/v2.json")
	if err == nil || !strings.Contains(err.Error(), "unsupported job format v=2 (this worker reads v1)") {
		t.Fatalf("got %v", err)
	}
}

func TestLuaPatternExpectIsNamed(t *testing.T) {
	_, err := readJob("testdata/invalid/lua_pattern_expect.json")
	if err == nil || !strings.Contains(err.Error(), "Lua pattern") {
		t.Fatalf("got %v", err)
	}
}

// A file written before "v" existed reads as v1, and the first rewrite (a retry) records v1.
func TestMissingVersionIsWrittenBackAsV1(t *testing.T) {
	j, err := readJob("testdata/jobs/legacy_without_v.json")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "j.json")
	if err := writeJob(p, j); err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	b, _ := os.ReadFile(p)
	json.Unmarshal(b, &raw)
	if raw["v"] != float64(1) {
		t.Fatalf("rewritten job has v=%v", raw["v"])
	}
}

// Rewriting a job (as every retry does) keeps every field the writer set.
func TestRewriteKeepsEveryField(t *testing.T) {
	files, _ := filepath.Glob("testdata/jobs/*.json")
	for _, f := range files {
		in, _ := os.ReadFile(f)
		j, err := parseJob(in)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		p := filepath.Join(t.TempDir(), "j.json")
		if err := writeJob(p, j); err != nil {
			t.Fatal(err)
		}
		var before, after map[string]any
		json.Unmarshal(in, &before)
		b, _ := os.ReadFile(p)
		json.Unmarshal(b, &after)
		for k, v := range before {
			if !reflect.DeepEqual(after[k], v) {
				t.Errorf("%s: field %q was %v, rewritten as %v", f, k, v, after[k])
			}
		}
		again, err := parseJob(b)
		if err != nil {
			t.Fatalf("%s: rewritten file refused: %v", f, err)
		}
		if !reflect.DeepEqual(again, j) {
			t.Errorf("%s: round trip changed the job:\n%+v\n%+v", f, j, again)
		}
	}
}

func TestDefaults(t *testing.T) {
	j, err := parseJob([]byte(`{"id":"x","url":"https://api.example.com/api/v0/x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if j.V != 1 || j.Type != "http" || j.Method != "POST" || j.TimeoutS != defaultTimeoutS {
		t.Fatalf("defaults not applied: %+v", j)
	}
}
