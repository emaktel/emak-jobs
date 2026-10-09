// emak-jobs: spool-directory job worker for FreeSWITCH Lua scripts.
//
// Lua writes one small JSON file per job into <spool>/new (atomically, via tmp + rename). This
// daemon wakes on inotify, claims the file by renaming it into <spool>/work, executes it (an
// HTTP request), and moves it to done/, to retry/ (with backoff) or to failed/ (after the last
// attempt, or at once if the job can never run as written). The file format is in job.go.
//
// Standard library only, so it builds offline.
//
//	emak-jobs                  run the worker (systemd: deploy/emak-jobs.service)
//	emak-jobs --version        print the version
//	emak-jobs check PATH...    validate job files (or every *.json in a directory); exit 1 if any is invalid
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// version is set at build time: -ldflags "-X main.version=vX.Y.Z".
var version = "dev"

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

const usage = `usage: emak-jobs                 run the worker
       emak-jobs --version       print the version
       emak-jobs check PATH...   validate job files or directories of them
`

func main() {
	args := os.Args[1:]
	switch {
	case len(args) == 0:
		os.Exit(run())
	case len(args) == 1 && args[0] == "--version":
		fmt.Println("emak-jobs " + version)
	case args[0] == "check" && len(args) > 1:
		bad, err := checkPaths(args[1:], os.Stdout)
		if err != nil {
			fmt.Fprintln(os.Stderr, "check:", err)
			os.Exit(2)
		}
		if bad > 0 {
			os.Exit(1)
		}
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}

func run() int {
	log.SetFlags(log.Ldate | log.Ltime)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	w := &worker{
		spool:       envOr("EMAK_JOBS_SPOOL", "/var/spool/emak-jobs"),
		workers:     envInt("EMAK_JOBS_WORKERS", 4),
		maxAttempts: envInt("EMAK_JOBS_MAX_ATTEMPTS", 8),
		doneKeep:    time.Duration(envInt("EMAK_JOBS_DONE_KEEP_HOURS", 24)) * time.Hour,
		statusFile:  envOr("EMAK_JOBS_STATUS_FILE", "/run/emak-jobs/status.json"),
		// From the systemd drop-in that loads /etc/emak/shared.env. Only emak_api_bearer jobs
		// need it; the voicemail jobs sign with this box's own key.
		apiKey: os.Getenv("EMAK_API_KEY"),
		client: &http.Client{Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment, MaxIdleConns: 32, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second,
		}},
		ctx:     ctx,
		queue:   make(chan string, 100000),
		queued:  map[string]bool{},
		started: time.Now(),
	}
	if w.apiKey == "" {
		log.Printf("WARN EMAK_API_KEY is not set: emak_api_bearer jobs will fail until it is")
	}
	log.Printf("emak-jobs %s starting spool=%s workers=%d max_attempts=%d", version, w.spool, w.workers, w.maxAttempts)
	if err := w.start(); err != nil {
		log.Printf("FATAL %v", err)
		return 1
	}

	<-ctx.Done()
	log.Printf("stopping: no new claims, in-flight requests cancelled and re-queued")
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("WARN a worker did not finish filing within 5 s; its job is recovered from work/ on start")
	}
	if err := w.writeStatus(); err != nil {
		log.Printf("WARN status file: %v", err)
	}
	log.Printf("stopped")
	return 0
}
