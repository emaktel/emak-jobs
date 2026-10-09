package main

import (
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var backoff = []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute, 60 * time.Minute}

var spoolDirs = []string{"new", "tmp", "work", "retry", "done", "failed", "blobs"}

type worker struct {
	spool       string
	workers     int
	maxAttempts int
	doneKeep    time.Duration
	statusFile  string
	apiKey      string
	client      *http.Client

	ctx      context.Context // cancelled on SIGTERM: nothing new is claimed and in-flight requests stop
	queue    chan string     // paths in new/ or retry/ to claim
	inflight int64
	started  time.Time
	wg       sync.WaitGroup

	stats struct {
		sync.Mutex
		OK, Fail, Retry, Dead, Invalid int64
		Last                           []string
		DurMs                          []int64
	}
}

func (w *worker) dir(n string) string { return filepath.Join(w.spool, n) }

func (w *worker) enqueue(path string) {
	select {
	case w.queue <- path:
	default:
		// The 60 s housekeeping rescan of new/ picks the file up once there is room.
		log.Printf("WARN queue full, %s left for the next rescan", filepath.Base(path))
	}
}

// start runs the worker. The order matters: work/ is recovered while nothing else runs, the
// watch exists before new/ is scanned (so no file falls between the two), and only then do the
// consumers start claiming.
func (w *worker) start() error {
	for _, d := range spoolDirs {
		if err := os.MkdirAll(w.dir(d), 0770); err != nil {
			return fmt.Errorf("mkdir %s: %w", w.dir(d), err)
		}
	}
	if n := w.recoverWork(); n > 0 {
		log.Printf("recovered %d interrupted job(s) from work/", n)
	}
	fd, err := w.watch()
	if err != nil {
		return err
	}
	go w.readEvents(fd)
	for _, n := range list(w.dir("new")) {
		w.enqueue(filepath.Join(w.dir("new"), n))
	}
	w.scheduleRetries()
	for i := 0; i < w.workers; i++ {
		w.wg.Add(1)
		go w.consume()
	}
	go w.housekeeping()
	go w.writeStatusLoop()
	return nil
}

func (w *worker) consume() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case p := <-w.queue:
			w.process(p)
		}
	}
}

// recoverWork moves jobs a previous run left in work/ back to new/. Only called before any
// consumer starts, so nothing in work/ can be in flight.
func (w *worker) recoverWork() int {
	n := 0
	for _, name := range list(w.dir("work")) {
		if err := os.Rename(filepath.Join(w.dir("work"), name), filepath.Join(w.dir("new"), name)); err != nil {
			log.Printf("ERR recover %s: %v", name, err)
			continue
		}
		n++
	}
	return n
}

// scheduleRetries queues every job in retry/ for its next_at.
func (w *worker) scheduleRetries() {
	now := time.Now().Unix()
	for _, name := range list(w.dir("retry")) {
		p := filepath.Join(w.dir("retry"), name)
		j, err := readJob(p)
		if err != nil {
			w.fail(p, name, err)
			continue
		}
		w.scheduleAt(p, j.NextAt-now)
	}
}

func (w *worker) scheduleAt(p string, inSecs int64) {
	if inSecs <= 0 {
		w.enqueue(p)
		return
	}
	time.AfterFunc(time.Duration(inSecs)*time.Second, func() { w.enqueue(p) })
}

// fail files a job that can't be read or can never run as written into failed/, unchanged.
func (w *worker) fail(p, name string, err error) {
	var inv *invalidJobError
	if errors.As(err, &inv) {
		log.Printf("ERR job=%s invalid: %v -> failed/", name, err)
		w.stats.Lock()
		w.stats.Invalid++
		w.stats.Unlock()
	} else {
		log.Printf("ERR job=%s unreadable: %v -> failed/", name, err)
	}
	if err := os.Rename(p, filepath.Join(w.dir("failed"), name)); err != nil {
		log.Printf("ERR job=%s move to failed/: %v", name, err)
	}
}

// process claims a job file (new/ or retry/) by renaming it into work/, runs it, and files it.
func (w *worker) process(src string) {
	if w.ctx.Err() != nil {
		return // stopping: leave it where it is for the next start
	}
	name := filepath.Base(src)
	work := filepath.Join(w.dir("work"), name)
	if err := os.Rename(src, work); err != nil {
		return // gone, or claimed by another consumer
	}
	atomic.AddInt64(&w.inflight, 1)
	defer atomic.AddInt64(&w.inflight, -1)

	j, err := readJob(work)
	if err != nil {
		w.fail(work, name, err)
		return
	}
	if len(j.Unknown) > 0 {
		log.Printf("WARN job=%s unknown field(s) %s (ignored; `emak-jobs check` refuses them) | %s", j.ID, strings.Join(j.Unknown, ", "), j.Label)
	}
	if wait := j.NextAt - time.Now().Unix(); wait > 0 {
		// Claimed before its backoff ended (a stale queue entry): put it back and wait.
		retry := filepath.Join(w.dir("retry"), name)
		if err := os.Rename(work, retry); err != nil {
			log.Printf("ERR job=%s back to retry/: %v", j.ID, err)
			return
		}
		w.scheduleAt(retry, wait)
		return
	}

	j.Attempts++
	t0 := time.Now()
	ok, detail := w.execute(j)
	ms := time.Since(t0).Milliseconds()

	if !ok && w.ctx.Err() != nil {
		// Stopped mid-request: the attempt doesn't count. The file in work/ is still the one we
		// claimed (nothing rewritten yet), so it goes back to new/ as it was.
		if err := os.Rename(work, filepath.Join(w.dir("new"), name)); err != nil {
			log.Printf("ERR job=%s requeue on stop: %v (left in work/, recovered on start)", j.ID, err)
			return
		}
		log.Printf("REQUEUE job=%s stopping, attempt not counted | %s", j.ID, j.Label)
		return
	}

	if ok {
		j.LastError = ""
		if err := writeJob(work, j); err != nil {
			log.Printf("ERR job=%s record success: %v", j.ID, err)
		}
		// Filed in done/ before the body is deleted: a job must never be recoverable without its body.
		if err := os.Rename(work, filepath.Join(w.dir("done"), name)); err != nil {
			log.Printf("ERR job=%s move to done/: %v (body kept)", j.ID, err)
			return
		}
		log.Printf("OK  job=%s attempt=%d dur_ms=%d %s | %s", j.ID, j.Attempts, ms, detail, j.Label)
		if j.LogOK != "" {
			log.Printf("    %s", j.LogOK)
		}
		if j.DeleteBodyFile && j.BodyFile != "" {
			if err := os.Remove(j.BodyFile); err != nil && !os.IsNotExist(err) {
				log.Printf("WARN job=%s delete body %s: %v", j.ID, j.BodyFile, err)
			}
		}
		w.record(true, fmt.Sprintf("%s OK %dms %s", time.Now().Format("15:04:05"), ms, j.Label), ms)
		return
	}

	j.LastError = detail
	w.record(false, fmt.Sprintf("%s FAIL %dms %s: %s", time.Now().Format("15:04:05"), ms, j.Label, detail), ms)
	if j.Attempts >= w.maxAttempts {
		if err := writeJob(work, j); err != nil {
			log.Printf("ERR job=%s record failure: %v", j.ID, err)
		}
		if err := os.Rename(work, filepath.Join(w.dir("failed"), name)); err != nil {
			log.Printf("ERR job=%s move to failed/: %v", j.ID, err)
			return
		}
		log.Printf("DEAD job=%s attempt=%d dur_ms=%d %s | %s%s", j.ID, j.Attempts, ms, detail, j.Label, failLine(j))
		w.stats.Lock()
		w.stats.Dead++
		w.stats.Unlock()
		return
	}
	d := backoff[min(j.Attempts-1, len(backoff)-1)]
	j.NextAt = time.Now().Add(d).Unix()
	if err := writeJob(work, j); err != nil {
		log.Printf("ERR job=%s record retry: %v", j.ID, err)
	}
	retry := filepath.Join(w.dir("retry"), name)
	if err := os.Rename(work, retry); err != nil {
		log.Printf("ERR job=%s move to retry/: %v", j.ID, err)
		return
	}
	log.Printf("RETRY job=%s attempt=%d dur_ms=%d in=%s %s | %s", j.ID, j.Attempts, ms, d, detail, j.Label)
	w.stats.Lock()
	w.stats.Retry++
	w.stats.Unlock()
	w.scheduleAt(retry, int64(d/time.Second))
}

func failLine(j *Job) string {
	if j.LogFail != "" {
		return "\n    " + j.LogFail
	}
	return ""
}

// execute performs the HTTP request. Returns (ok, detail).
func (w *worker) execute(j *Job) (bool, string) {
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
	ctx, cancel := context.WithTimeout(w.ctx, time.Duration(j.TimeoutS)*time.Second)
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
	if err := authFor(j.Auth, w.apiKey, req); err != nil {
		return false, "auth: " + err.Error()
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return false, "http: " + err.Error()
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return false, fmt.Sprintf("status %d body=%q", resp.StatusCode, trunc(string(rb), 200))
	}
	if j.Expect != "" && !regexp.MustCompile(j.Expect).Match(rb) { // validate() already proved it compiles
		return false, fmt.Sprintf("status %d but body did not match expect: %q", resp.StatusCode, trunc(string(rb), 200))
	}
	return true, fmt.Sprintf("status %d", resp.StatusCode)
}

// authFor maps a job's named auth to credentials; secrets never live in job files. It runs on
// every attempt, so a retried job is signed afresh.
func authFor(name, apiKey string, req *http.Request) error {
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
		if apiKey == "" {
			return errors.New("EMAK_API_KEY missing from the environment")
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		return nil
	}
	return fmt.Errorf("unknown auth %q", name)
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func (w *worker) record(ok bool, line string, ms int64) {
	w.stats.Lock()
	defer w.stats.Unlock()
	if ok {
		w.stats.OK++
	} else {
		w.stats.Fail++
	}
	w.stats.Last = append(w.stats.Last, line)
	if len(w.stats.Last) > 50 {
		w.stats.Last = w.stats.Last[1:]
	}
	w.stats.DurMs = append(w.stats.DurMs, ms)
	if len(w.stats.DurMs) > 500 {
		w.stats.DurMs = w.stats.DurMs[1:]
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

// watch adds the inotify watch on new/ (rename-in or close-after-write).
func (w *worker) watch() (int, error) {
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC)
	if err != nil {
		return -1, fmt.Errorf("inotify_init: %w", err)
	}
	if _, err := syscall.InotifyAddWatch(fd, w.dir("new"), syscall.IN_MOVED_TO|syscall.IN_CLOSE_WRITE); err != nil {
		syscall.Close(fd)
		return -1, fmt.Errorf("inotify_add_watch: %w", err)
	}
	return fd, nil
}

func (w *worker) readEvents(fd int) {
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
			if ev.Mask&syscall.IN_Q_OVERFLOW != 0 {
				log.Printf("WARN inotify queue overflowed; the 60 s rescan of new/ catches up")
			}
			if strings.HasSuffix(name, ".json") {
				w.enqueue(filepath.Join(w.dir("new"), name))
			}
		}
	}
}

func (w *worker) housekeeping() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-t.C:
		}
		// belt and braces: anything sitting in new/ that inotify missed
		for _, n := range list(w.dir("new")) {
			w.enqueue(filepath.Join(w.dir("new"), n))
		}
		cut := time.Now().Add(-w.doneKeep)
		ents, _ := os.ReadDir(w.dir("done"))
		for _, e := range ents {
			if fi, err := e.Info(); err == nil && fi.ModTime().Before(cut) {
				os.Remove(filepath.Join(w.dir("done"), e.Name()))
			}
		}
	}
}

func (w *worker) writeStatusLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		if err := w.writeStatus(); err != nil {
			log.Printf("WARN status file: %v", err)
		}
		select {
		case <-w.ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *worker) writeStatus() error {
	w.stats.Lock()
	var p50, p95 int64
	if len(w.stats.DurMs) > 0 {
		s := append([]int64(nil), w.stats.DurMs...)
		sort.Slice(s, func(a, b int) bool { return s[a] < s[b] })
		p50, p95 = s[len(s)/2], s[len(s)*95/100]
	}
	st := map[string]any{
		"version": version, "pid": os.Getpid(), "started": w.started.Format(time.RFC3339),
		"time": time.Now().Format(time.RFC3339), "ok": w.stats.OK, "fail": w.stats.Fail, "retry": w.stats.Retry,
		"dead": w.stats.Dead, "invalid": w.stats.Invalid,
		"inflight": atomic.LoadInt64(&w.inflight), "queued": len(w.queue),
		"new": len(list(w.dir("new"))), "work": len(list(w.dir("work"))), "retry_dir": len(list(w.dir("retry"))),
		"failed_dir": len(list(w.dir("failed"))),
		"dur_p50_ms": p50, "dur_p95_ms": p95, "last": w.stats.Last,
	}
	w.stats.Unlock()
	b, err := json.MarshalIndent(st, "", " ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(w.statusFile), 0755); err != nil {
		return err
	}
	tmp := w.statusFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, w.statusFile)
}
