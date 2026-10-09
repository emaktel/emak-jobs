// emak-jobs: spool-directory job worker for FreeSWITCH Lua scripts.
//
// Lua writes one small JSON file per job into <spool>/new (atomically, via
// tmp + rename). This daemon wakes on inotify, claims the file by renaming it
// into <spool>/work, executes it (HTTP request), and moves it to done/ or
// retry/ (with backoff) or failed/ (after max attempts).
//
// Standard library only — no external modules — so it builds offline.
//
// Job file schema (all strings unless noted):
//
//	{
//	  "id":            "uuid",                    // required, unique
//	  "type":          "http",                    // only type in v1
//	  "label":         "vm_upload msg_<uuid>",    // free text for logs
//	  "created":       "2026-09-21T13:40:00Z",
//	  "attempts":      0,                         // int, managed by worker
//	  "next_at":       0,                         // unix secs, managed by worker
//	  "method":        "POST",
//	  "url":           "https://host/path",       // WITHOUT credentials
//	  "auth":          "vm_fetch",                // named credential, see authFor()
//	  "headers":       {"X-Foo": "bar"},          // optional
//	  "body_file":     "/path/to/file",           // optional: streamed as body
//	  "content_type":  "audio/wav",               // optional
//	  "expect":        "\"success\"\\s*:\\s*true", // optional regex on response body
//	  "timeout_s":     120,                       // int, optional (default 120)
//	  "delete_body_file": false,                  // bool: unlink body_file after success
//	  "log_ok":        "[voicemail] R2 message upload OK: ...", // optional
//	  "log_fail":      "[voicemail] R2 message upload FAILED: ..."
//	}
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

type Job struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	Label          string            `json:"label"`
	Created        string            `json:"created"`
	Attempts       int               `json:"attempts"`
	NextAt         int64             `json:"next_at"`
	Method         string            `json:"method"`
	URL            string            `json:"url"`
	Auth           string            `json:"auth"`
	Headers        map[string]string `json:"headers"`
	BodyFile       string            `json:"body_file"`
	ContentType    string            `json:"content_type"`
	Expect         string            `json:"expect"`
	TimeoutS       int               `json:"timeout_s"`
	DeleteBodyFile bool              `json:"delete_body_file"`
	LogOK          string            `json:"log_ok"`
	LogFail        string            `json:"log_fail"`
	OriginalBody   string            `json:"original_body_file,omitempty"`
	LastError      string            `json:"last_error,omitempty"`
}

var (
	spool       = envOr("EMAK_JOBS_SPOOL", "/var/spool/emak-jobs")
	workers     = envInt("EMAK_JOBS_WORKERS", 4)
	maxAttempts = envInt("EMAK_JOBS_MAX_ATTEMPTS", 8)
	envFile     = envOr("EMAK_JOBS_ENV_FILE", "/usr/share/freeswitch/scripts/.env")
	statusFile  = envOr("EMAK_JOBS_STATUS_FILE", "/run/emak-jobs/status.json")
	doneKeep    = time.Duration(envInt("EMAK_JOBS_DONE_KEEP_HOURS", 24)) * time.Hour
	secrets     = map[string]string{}
	backoff     = []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute, 60 * time.Minute}

	queue    = make(chan string, 100000) // paths in new/ (or retry/ when due)
	inflight int64
	stats    = struct {
		sync.Mutex
		OK, Fail, Retry, Dead int64
		Last                  []string
		DurMs                 []int64
	}{}
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return d
}

func dir(n string) string { return filepath.Join(spool, n) }

// loadSecrets reads KEY=VALUE lines from the FreeSWITCH scripts .env (same file Lua uses).
func loadSecrets() {
	f, err := os.Open(envFile)
	if err != nil {
		log.Printf("WARN cannot read env file %s: %v (auth'd jobs will fail)", envFile, err)
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		kv := strings.SplitN(line, "=", 2)
		v := strings.TrimSpace(kv[1])
		v = strings.Trim(v, `"'`)
		secrets[strings.TrimSpace(kv[0])] = v
	}
}

// authFor maps a job's named auth to concrete credentials; secrets never live in job files.
// It runs on every attempt, so a retried job is signed afresh.
func authFor(name string, req *http.Request) error {
	switch name {
	case "", "none":
		return nil
	case "vm_fetch": // voicemail storage routes: presigned with this box's key (voicemail_auth.go)
		id, secret, err := loadVoicemailCredential(vmCredentialFile)
		if err != nil {
			return err
		}
		return presignVoicemail(req, id, secret, time.Now())
	case "emak_api_bearer": // recordings.lua: Authorization: Bearer <EMAK_API_KEY>
		s := secrets["EMAK_API_KEY"]
		if s == "" {
			return errors.New("EMAK_API_KEY missing")
		}
		req.Header.Set("Authorization", "Bearer "+s)
	default:
		return fmt.Errorf("unknown auth %q", name)
	}
	return nil
}

func readJob(path string) (*Job, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var j Job
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, err
	}
	if j.ID == "" || j.URL == "" {
		return nil, errors.New("job missing id or url")
	}
	if j.Type == "" {
		j.Type = "http"
	}
	if j.Method == "" {
		j.Method = "POST"
	}
	if j.TimeoutS <= 0 {
		j.TimeoutS = 120
	}
	return &j, nil
}

func writeJob(path string, j *Job) error {
	b, _ := json.MarshalIndent(j, "", " ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// execute performs the HTTP request. Returns (ok, detail).
func execute(j *Job) (bool, string) {
	if j.Type != "http" {
		return false, "unsupported type " + j.Type
	}
	var body io.Reader
	var size int64 = -1
	if j.BodyFile != "" {
		f, err := os.Open(j.BodyFile)
		if err != nil {
			return false, "body_file: " + err.Error()
		}
		defer f.Close()
		if st, err := f.Stat(); err == nil {
			size = st.Size()
		}
		body = f
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(j.TimeoutS)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, j.Method, j.URL, body)
	if err != nil {
		return false, "request: " + err.Error()
	}
	if size >= 0 {
		req.ContentLength = size
	}
	if j.ContentType != "" {
		req.Header.Set("Content-Type", j.ContentType)
	}
	for k, v := range j.Headers {
		req.Header.Set(k, v)
	}
	if err := authFor(j.Auth, req); err != nil {
		return false, "auth: " + err.Error()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, "http: " + err.Error()
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return false, fmt.Sprintf("status %d body=%q", resp.StatusCode, trunc(string(rb), 200))
	}
	if j.Expect != "" {
		re, err := regexp.Compile(j.Expect)
		if err != nil {
			return false, "bad expect regex: " + err.Error()
		}
		if !re.Match(rb) {
			return false, fmt.Sprintf("status %d but body did not match expect: %q", resp.StatusCode, trunc(string(rb), 200))
		}
	}
	return true, fmt.Sprintf("status %d", resp.StatusCode)
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func record(ok bool, line string, ms int64) {
	stats.Lock()
	defer stats.Unlock()
	if ok {
		stats.OK++
	} else {
		stats.Fail++
	}
	stats.Last = append(stats.Last, line)
	if len(stats.Last) > 50 {
		stats.Last = stats.Last[1:]
	}
	stats.DurMs = append(stats.DurMs, ms)
	if len(stats.DurMs) > 500 {
		stats.DurMs = stats.DurMs[1:]
	}
}

// process claims a job file (new/ or retry/) by renaming into work/, runs it, and files the result.
func process(src string) {
	name := filepath.Base(src)
	work := filepath.Join(dir("work"), name)
	if err := os.Rename(src, work); err != nil {
		return // gone or claimed by someone else
	}
	atomic.AddInt64(&inflight, 1)
	defer atomic.AddInt64(&inflight, -1)

	j, err := readJob(work)
	if err != nil {
		log.Printf("ERR job=%s unreadable: %v -> failed/", name, err)
		os.Rename(work, filepath.Join(dir("failed"), name))
		return
	}
	j.Attempts++
	t0 := time.Now()
	ok, detail := execute(j)
	ms := time.Since(t0).Milliseconds()
	if ok {
		log.Printf("OK  job=%s attempt=%d dur_ms=%d %s | %s", j.ID, j.Attempts, ms, detail, j.Label)
		if j.LogOK != "" {
			log.Printf("    %s", j.LogOK)
		}
		if j.DeleteBodyFile && j.BodyFile != "" {
			os.Remove(j.BodyFile)
		}
		j.LastError = ""
		writeJob(work, j)
		os.Rename(work, filepath.Join(dir("done"), name))
		record(true, fmt.Sprintf("%s OK %dms %s", time.Now().Format("15:04:05"), ms, j.Label), ms)
		return
	}
	j.LastError = detail
	record(false, fmt.Sprintf("%s FAIL %dms %s: %s", time.Now().Format("15:04:05"), ms, j.Label, detail), ms)
	if j.Attempts >= maxAttempts {
		log.Printf("DEAD job=%s attempt=%d dur_ms=%d %s | %s%s", j.ID, j.Attempts, ms, detail, j.Label, failLine(j))
		writeJob(work, j)
		os.Rename(work, filepath.Join(dir("failed"), name))
		stats.Lock()
		stats.Dead++
		stats.Unlock()
		return
	}
	d := backoff[min(j.Attempts-1, len(backoff)-1)]
	j.NextAt = time.Now().Add(d).Unix()
	log.Printf("RETRY job=%s attempt=%d dur_ms=%d in=%s %s | %s", j.ID, j.Attempts, ms, d, detail, j.Label)
	writeJob(work, j)
	retry := filepath.Join(dir("retry"), name)
	os.Rename(work, retry)
	stats.Lock()
	stats.Retry++
	stats.Unlock()
	time.AfterFunc(d, func() { queue <- retry })
}

func failLine(j *Job) string {
	if j.LogFail != "" {
		return "\n    " + j.LogFail
	}
	return ""
}

// sweep re-queues anything left in new/ and work/ (crash recovery) and schedules retry/.
func sweep() {
	for _, n := range list(dir("work")) { // interrupted mid-flight: back to new/
		os.Rename(filepath.Join(dir("work"), n), filepath.Join(dir("new"), n))
	}
	for _, n := range list(dir("new")) {
		queue <- filepath.Join(dir("new"), n)
	}
	now := time.Now().Unix()
	for _, n := range list(dir("retry")) {
		p := filepath.Join(dir("retry"), n)
		j, err := readJob(p)
		if err != nil {
			os.Rename(p, filepath.Join(dir("failed"), n))
			continue
		}
		if j.NextAt <= now {
			queue <- p
		} else {
			time.AfterFunc(time.Duration(j.NextAt-now)*time.Second, func() { queue <- p })
		}
	}
}

func list(d string) []string {
	ents, _ := os.ReadDir(d)
	var out []string
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// watchNew blocks on inotify for files appearing in new/ (rename-in or close-after-write).
func watchNew() {
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC)
	if err != nil {
		log.Fatalf("inotify_init: %v", err)
	}
	if _, err := syscall.InotifyAddWatch(fd, dir("new"), syscall.IN_MOVED_TO|syscall.IN_CLOSE_WRITE); err != nil {
		log.Fatalf("inotify_add_watch: %v", err)
	}
	buf := make([]byte, 64*1024)
	for {
		n, err := syscall.Read(fd, buf)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			log.Fatalf("inotify read: %v", err)
		}
		off := 0
		for off < n {
			ev := (*syscall.InotifyEvent)(unsafe.Pointer(&buf[off]))
			nameLen := int(ev.Len)
			name := ""
			if nameLen > 0 {
				name = strings.TrimRight(string(buf[off+syscall.SizeofInotifyEvent:off+syscall.SizeofInotifyEvent+nameLen]), "\x00")
			}
			off += syscall.SizeofInotifyEvent + nameLen
			if strings.HasSuffix(name, ".json") {
				queue <- filepath.Join(dir("new"), name)
			}
		}
	}
}

func housekeeping() {
	t := time.NewTicker(60 * time.Second)
	for range t.C {
		// belt-and-braces: anything sitting in new/ that inotify somehow missed
		for _, n := range list(dir("new")) {
			queue <- filepath.Join(dir("new"), n)
		}
		// prune done/
		cut := time.Now().Add(-doneKeep)
		ents, _ := os.ReadDir(dir("done"))
		for _, e := range ents {
			if fi, err := e.Info(); err == nil && fi.ModTime().Before(cut) {
				os.Remove(filepath.Join(dir("done"), e.Name()))
			}
		}
	}
}

func writeStatus() {
	t := time.NewTicker(10 * time.Second)
	for range t.C {
		stats.Lock()
		var p50, p95 int64
		if len(stats.DurMs) > 0 {
			s := append([]int64(nil), stats.DurMs...)
			sort.Slice(s, func(a, b int) bool { return s[a] < s[b] })
			p50, p95 = s[len(s)/2], s[len(s)*95/100]
		}
		st := map[string]any{
			"time": time.Now().Format(time.RFC3339), "ok": stats.OK, "fail": stats.Fail, "retry": stats.Retry, "dead": stats.Dead,
			"inflight": atomic.LoadInt64(&inflight), "queued": len(queue),
			"new": len(list(dir("new"))), "retry_dir": len(list(dir("retry"))), "failed_dir": len(list(dir("failed"))),
			"dur_p50_ms": p50, "dur_p95_ms": p95, "last": stats.Last,
		}
		stats.Unlock()
		b, _ := json.MarshalIndent(st, "", " ")
		os.MkdirAll(filepath.Dir(statusFile), 0755)
		os.WriteFile(statusFile+".tmp", b, 0644)
		os.Rename(statusFile+".tmp", statusFile)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	for _, d := range []string{"new", "work", "retry", "done", "failed", "tmp", "blobs"} {
		if err := os.MkdirAll(dir(d), 0770); err != nil {
			log.Fatalf("mkdir %s: %v", dir(d), err)
		}
	}
	loadSecrets()
	http.DefaultClient = &http.Client{Transport: &http.Transport{MaxIdleConns: 32, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second}}
	log.Printf("emak-jobs starting spool=%s workers=%d max_attempts=%d", spool, workers, maxAttempts)
	for i := 0; i < workers; i++ {
		go func() {
			for p := range queue {
				process(p)
			}
		}()
	}
	go watchNew()
	go housekeeping()
	go writeStatus()
	sweep()
	select {}
}
