package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type testWorker struct {
	*worker
	cancel context.CancelFunc
	hits   *int64
}

// newTestWorker builds a worker on a temp spool whose jobs all go to an httptest server.
func newTestWorker(t *testing.T, handler http.HandlerFunc) (*testWorker, *httptest.Server) {
	t.Helper()
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		handler(rw, r)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()
	w := &worker{
		spool: filepath.Join(dir, "spool"), workers: 2, maxAttempts: 3, doneKeep: time.Hour,
		statusFile: filepath.Join(dir, "status.json"), apiKey: "test-api-key", client: srv.Client(),
		ctx: ctx, queue: make(chan string, 100), started: time.Now(),
	}
	for _, d := range spoolDirs {
		if err := os.MkdirAll(w.dir(d), 0770); err != nil {
			t.Fatal(err)
		}
	}
	return &testWorker{w, cancel, &hits}, srv
}

func ok(rw http.ResponseWriter, _ *http.Request) { fmt.Fprint(rw, `{"success": true}`) }

func (tw *testWorker) put(t *testing.T, dir, id string, mutate func(*Job)) string {
	t.Helper()
	j := &Job{V: 1, ID: id, Type: "http", Method: "POST", URL: "", Auth: "none", Expect: `"success"\s*:\s*true`, TimeoutS: 5}
	if mutate != nil {
		mutate(j)
	}
	p := filepath.Join(tw.dir(dir), id+".json")
	if err := writeJob(p, j); err != nil {
		t.Fatal(err)
	}
	return p
}

func (tw *testWorker) job(t *testing.T, dir, id string) *Job {
	t.Helper()
	j, err := readJob(filepath.Join(tw.dir(dir), id+".json"))
	if err != nil {
		t.Fatalf("%s/%s: %v", dir, id, err)
	}
	return j
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A job a previous run left in work/ runs exactly once after start.
func TestStartRecoversWorkOnce(t *testing.T) {
	tw, srv := newTestWorker(t, ok)
	tw.put(t, "work", "a", func(j *Job) { j.URL = srv.URL + "/a" })
	if err := tw.start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a in done/", func() bool { return exists(filepath.Join(tw.dir("done"), "a.json")) })
	time.Sleep(200 * time.Millisecond)
	if h := atomic.LoadInt64(tw.hits); h != 1 {
		t.Fatalf("server hit %d times, want 1", h)
	}
}

// Jobs written to new/ while running are picked up through inotify, and the body of a
// delete_body_file job is deleted only once the job is in done/.
func TestNewJobRunsAndBodyGoesAfterDone(t *testing.T) {
	tw, srv := newTestWorker(t, ok)
	if err := tw.start(); err != nil {
		t.Fatal(err)
	}
	body := filepath.Join(tw.dir("blobs"), "b.wav")
	os.WriteFile(body, []byte("RIFF"), 0640)
	tw.put(t, "tmp", "b", func(j *Job) { j.URL = srv.URL + "/b"; j.BodyFile = body; j.DeleteBodyFile = true })
	os.Rename(filepath.Join(tw.dir("tmp"), "b.json"), filepath.Join(tw.dir("new"), "b.json"))
	waitFor(t, "b in done/", func() bool { return exists(filepath.Join(tw.dir("done"), "b.json")) })
	waitFor(t, "body deleted", func() bool { return !exists(body) })
	if j := tw.job(t, "done", "b"); j.Attempts != 1 || j.LastError != "" {
		t.Fatalf("done job: %+v", j)
	}
}

// If the job can't be filed in done/, its body stays, so the job can still be run again.
func TestBodyKeptWhenDoneFails(t *testing.T) {
	tw, srv := newTestWorker(t, ok)
	body := filepath.Join(tw.dir("blobs"), "c.wav")
	os.WriteFile(body, []byte("RIFF"), 0640)
	p := tw.put(t, "new", "c", func(j *Job) { j.URL = srv.URL + "/c"; j.BodyFile = body; j.DeleteBodyFile = true })
	os.RemoveAll(tw.dir("done"))
	os.WriteFile(tw.dir("done"), nil, 0640) // a file where the directory should be: the rename fails
	tw.process(p)
	if !exists(body) {
		t.Fatal("body deleted although the job never reached done/")
	}
	if !exists(filepath.Join(tw.dir("work"), "c.json")) {
		t.Fatal("job should stay in work/ for recovery on start")
	}
}

// A failure goes to retry/ with a backoff; a retry claimed early goes back untouched.
func TestRetryHonoursNextAt(t *testing.T) {
	var fail int64 = 1
	tw, srv := newTestWorker(t, func(rw http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt64(&fail) == 1 {
			rw.WriteHeader(503)
			return
		}
		ok(rw, r)
	})
	p := tw.put(t, "new", "d", func(j *Job) { j.URL = srv.URL + "/d" })
	tw.process(p)
	j := tw.job(t, "retry", "d")
	if j.Attempts != 1 || j.NextAt <= time.Now().Unix() || j.LastError == "" {
		t.Fatalf("after one failure: %+v", j)
	}
	atomic.StoreInt64(&fail, 0)
	tw.process(filepath.Join(tw.dir("retry"), "d.json")) // stale queue entry, before next_at
	if h := atomic.LoadInt64(tw.hits); h != 1 {
		t.Fatalf("retry ran before next_at (hits=%d)", h)
	}
	if got := tw.job(t, "retry", "d"); got.Attempts != 1 {
		t.Fatalf("early claim changed the job: %+v", got)
	}
	writeJob(filepath.Join(tw.dir("retry"), "d.json"), func() *Job { j.NextAt = time.Now().Unix() - 1; return j }())
	tw.process(filepath.Join(tw.dir("retry"), "d.json"))
	if got := tw.job(t, "done", "d"); got.Attempts != 2 {
		t.Fatalf("after success: %+v", got)
	}
}

// A retry whose time has passed while the worker was down runs on start; one still waiting doesn't.
func TestStartSchedulesRetries(t *testing.T) {
	tw, srv := newTestWorker(t, ok)
	tw.put(t, "retry", "due", func(j *Job) { j.URL = srv.URL + "/due"; j.Attempts = 1; j.NextAt = time.Now().Unix() - 10 })
	tw.put(t, "retry", "later", func(j *Job) { j.URL = srv.URL + "/later"; j.Attempts = 1; j.NextAt = time.Now().Unix() + 3600 })
	if err := tw.start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "due in done/", func() bool { return exists(filepath.Join(tw.dir("done"), "due.json")) })
	if !exists(filepath.Join(tw.dir("retry"), "later.json")) || atomic.LoadInt64(tw.hits) != 1 {
		t.Fatal("a retry ran before its next_at")
	}
}

func TestLastAttemptGoesToFailed(t *testing.T) {
	tw, srv := newTestWorker(t, func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(500) })
	p := tw.put(t, "new", "e", func(j *Job) { j.URL = srv.URL + "/e"; j.Attempts = 2 })
	tw.process(p)
	if j := tw.job(t, "failed", "e"); j.Attempts != 3 || j.LastError == "" {
		t.Fatalf("dead job: %+v", j)
	}
}

// An invalid job goes to failed/ unchanged, at once, without a request.
func TestInvalidJobFailsAtOnce(t *testing.T) {
	tw, _ := newTestWorker(t, ok)
	in, _ := os.ReadFile("testdata/invalid/v2.json")
	p := filepath.Join(tw.dir("new"), "f.json")
	os.WriteFile(p, in, 0640)
	tw.process(p)
	out, err := os.ReadFile(filepath.Join(tw.dir("failed"), "f.json"))
	if err != nil || string(out) != string(in) {
		t.Fatalf("invalid job not filed unchanged in failed/: %v", err)
	}
	if atomic.LoadInt64(tw.hits) != 0 {
		t.Fatal("an invalid job made a request")
	}
}

// Stopping cancels the in-flight request and puts the job back in new/ without counting the attempt.
func TestStopRequeuesInFlightJob(t *testing.T) {
	entered := make(chan struct{})
	tw, srv := newTestWorker(t, func(rw http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	})
	if err := tw.start(); err != nil {
		t.Fatal(err)
	}
	tw.put(t, "new", "g", func(j *Job) { j.URL = srv.URL + "/g"; j.TimeoutS = 60 })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request never started")
	}
	tw.cancel()
	done := make(chan struct{})
	go func() { tw.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not stop")
	}
	if j := tw.job(t, "new", "g"); j.Attempts != 0 {
		t.Fatalf("requeued job counted the cancelled attempt: %+v", j)
	}
	if exists(filepath.Join(tw.dir("work"), "g.json")) || exists(filepath.Join(tw.dir("retry"), "g.json")) {
		t.Fatal("job left outside new/")
	}
}

func TestBearerAuthFromEnvironmentKey(t *testing.T) {
	var got atomic.Value
	tw, srv := newTestWorker(t, func(rw http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		ok(rw, r)
	})
	tw.process(tw.put(t, "new", "h", func(j *Job) { j.URL = srv.URL + "/h"; j.Auth = "emak_api_bearer" }))
	if got.Load() != "Bearer test-api-key" {
		t.Fatalf("Authorization = %v", got.Load())
	}
	tw.apiKey = ""
	tw.process(tw.put(t, "new", "i", func(j *Job) { j.URL = srv.URL + "/i"; j.Auth = "emak_api_bearer" }))
	if j := tw.job(t, "retry", "i"); j.LastError != "auth: EMAK_API_KEY missing from the environment" {
		t.Fatalf("missing key: %+v", j)
	}
}

func TestStatusReportsVersionAndProcess(t *testing.T) {
	tw, _ := newTestWorker(t, ok)
	if err := tw.writeStatus(); err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	b, _ := os.ReadFile(tw.statusFile)
	json.Unmarshal(b, &st)
	if st["version"] != version || st["pid"] != float64(os.Getpid()) || st["started"] == "" || st["work"] != float64(0) {
		t.Fatalf("status: %s", b)
	}
}
