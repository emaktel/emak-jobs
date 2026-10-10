package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

// formatVersion is the only job-file format this worker reads. A file without "v" is v1 (the
// files written before the field existed); any other value is refused, never guessed at.
const formatVersion = 1

// Job is one job file. The writer is emak_jobs.lua in the fusionpbx fork; testdata/jobs/ holds
// a file for every shape it writes, and `emak-jobs check` is how the writer's tests (and the
// installer's drift check) prove their files are ones this worker accepts.
//
// Every field the worker or the writer uses must be declared here: a retried job is rewritten
// from this struct, so an undeclared field would be dropped on the first retry.
type Job struct {
	V              int               `json:"v"`
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	Label          string            `json:"label"`
	Created        string            `json:"created"`
	Attempts       int               `json:"attempts"`
	NextAt         int64             `json:"next_at"`
	Method         string            `json:"method"`
	URL            string            `json:"url"`
	Auth           string            `json:"auth"`
	Headers        map[string]string `json:"headers,omitempty"`
	BodyFile       string            `json:"body_file,omitempty"`
	ContentType    string            `json:"content_type,omitempty"`
	Expect         string            `json:"expect,omitempty"`
	TimeoutS       int               `json:"timeout_s"`
	DeleteBodyFile bool              `json:"delete_body_file"`
	LogOK          string            `json:"log_ok,omitempty"`
	LogFail        string            `json:"log_fail,omitempty"`
	OriginalBody   string            `json:"original_body_file,omitempty"`
	LastError      string            `json:"last_error,omitempty"`

	// Unknown lists fields this worker doesn't know (or knows only under another case). `check`
	// refuses them, so the writer's tests catch a typo or an unannounced field; the running worker
	// only warns, so a harmless extra field never costs a voicemail.
	Unknown []string `json:"-"`
}

const (
	defaultTimeoutS = 120
	maxTimeoutS     = 3600
	// far above any EMAK_JOBS_MAX_ATTEMPTS; refuses values that would overflow when incremented
	maxAttemptsField = 1000
	// seconds; the longest backoff is an hour, so a later next_at is a unit mistake (ms?) or garbage
	maxNextAtAhead = 24 * 60 * 60
)

var (
	knownMethods = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true}
	knownAuth    = map[string]bool{"": true, "none": true, "vm_fetch": true, "emak_api_bearer": true}
	knownFields  = jsonFieldNames(reflect.TypeOf(Job{}))
	// A Lua pattern class (%s, %d, %w…) in "expect" compiles as a Go regexp but matches only the
	// literal text, so every attempt would "fail" after the server had already done the work.
	luaPatternClass = regexp.MustCompile(`%[acdglpsuwxACDGLPSUWXbf]`)
)

// invalidJobError marks a job that can never succeed as written: it goes straight to failed/
// instead of being retried.
type invalidJobError struct{ reason string }

func (e *invalidJobError) Error() string { return e.reason }

func invalid(format string, a ...any) error { return &invalidJobError{fmt.Sprintf(format, a...)} }

// readJob reads and validates a job file. Errors other than *invalidJobError are I/O errors.
func readJob(path string) (*Job, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseJob(b)
}

func parseJob(b []byte) (*Job, error) {
	var head struct {
		V json.RawMessage `json:"v"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return nil, invalid("not a JSON object: %v", err)
	}
	if head.V != nil && string(bytes.TrimSpace(head.V)) != fmt.Sprint(formatVersion) {
		return nil, invalid("unsupported job format v=%s (this worker reads v%d)", head.V, formatVersion)
	}
	var j Job
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, invalid("bad field: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, invalid("not a JSON object: %v", err)
	}
	for k := range fields {
		if knownFields[k] {
			continue
		}
		if knownFields[strings.ToLower(k)] {
			// encoding/json matched it case-insensitively and used it; the writer still has a typo
			k += " (mis-cased, used anyway)"
		}
		j.Unknown = append(j.Unknown, k)
	}
	sort.Strings(j.Unknown)
	j.V = formatVersion
	if j.Type == "" {
		j.Type = "http"
	}
	if j.Method == "" {
		j.Method = "POST"
	}
	if j.TimeoutS == 0 {
		j.TimeoutS = defaultTimeoutS
	}
	if err := j.validate(); err != nil {
		return nil, err
	}
	return &j, nil
}

func (j *Job) validate() error {
	if j.ID == "" {
		return invalid("missing id")
	}
	if j.Type != "http" {
		return invalid("unsupported type %q", j.Type)
	}
	if !knownMethods[j.Method] {
		return invalid("unsupported method %q", j.Method)
	}
	u, err := url.Parse(j.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return invalid("url must be an absolute http(s) URL: %q", j.URL)
	}
	if u.User != nil {
		return invalid("url must not embed credentials; use auth")
	}
	if !knownAuth[j.Auth] {
		return invalid("unknown auth %q", j.Auth)
	}
	if j.Expect != "" {
		if luaPatternClass.MatchString(j.Expect) {
			return invalid("expect %q is a Lua pattern; it must be a Go regexp (\\s, not %%s)", j.Expect)
		}
		if _, err := regexp.Compile(j.Expect); err != nil {
			return invalid("expect is not a valid Go regexp: %v", err)
		}
	}
	if j.Attempts < 0 || j.Attempts > maxAttemptsField {
		return invalid("attempts %d outside 0..%d", j.Attempts, maxAttemptsField)
	}
	if j.NextAt < 0 || j.NextAt > time.Now().Unix()+maxNextAtAhead {
		return invalid("next_at %d is negative or more than a day ahead", j.NextAt)
	}
	if j.TimeoutS < 1 || j.TimeoutS > maxTimeoutS {
		return invalid("timeout_s %d outside 1..%d", j.TimeoutS, maxTimeoutS)
	}
	if j.BodyFile != "" && !filepath.IsAbs(j.BodyFile) {
		return invalid("body_file must be an absolute path: %q", j.BodyFile)
	}
	for k := range j.Headers {
		if strings.EqualFold(k, "Authorization") {
			return invalid("headers must not carry Authorization; use auth")
		}
	}
	return nil
}

func jsonFieldNames(t reflect.Type) map[string]bool {
	names := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		if name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ","); name != "" && name != "-" {
			names[name] = true
		}
	}
	return names
}

// writeJob rewrites a job file atomically (tmp + rename in the same directory).
func writeJob(path string, j *Job) error {
	b, err := json.MarshalIndent(j, "", " ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0640); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// checkPaths implements `emak-jobs check`: every named file, or every *.json in a named
// directory, must be a job this worker accepts. Nothing is executed or modified.
func checkPaths(paths []string, out io.Writer) (bad int, err error) {
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			return bad, err
		}
		files := []string{p}
		if st.IsDir() {
			files = files[:0]
			ents, err := os.ReadDir(p)
			if err != nil {
				return bad, err
			}
			for _, e := range ents {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
					files = append(files, filepath.Join(p, e.Name()))
				}
			}
		}
		for _, f := range files {
			j, err := readJob(f)
			if err != nil {
				var inv *invalidJobError
				if !errors.As(err, &inv) {
					return bad, err
				}
				bad++
				fmt.Fprintf(out, "INVALID %s: %v\n", f, err)
				continue
			}
			if len(j.Unknown) > 0 {
				bad++
				fmt.Fprintf(out, "INVALID %s: unknown field(s) %s\n", f, strings.Join(j.Unknown, ", "))
				continue
			}
			fmt.Fprintf(out, "OK %s\n", f)
		}
	}
	return bad, nil
}
