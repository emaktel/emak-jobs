# emak-jobs

A small spool worker that takes HTTP work off FreeSWITCH's call path. FreeSWITCH Lua scripts write one JSON file per job into a spool directory and carry on; `emak-jobs` picks the file up within a millisecond (inotify), performs the request, and retries with backoff until it succeeds or gives up.

It carries every voicemail upload, delete, copy and transcription, plus the recordings API calls, on our call servers. Standard library only; one static binary.

## Why it exists

Lua 5.2's `io.popen()` calls `fflush(NULL)` before `popen()`. `fflush(NULL)` takes glibc's process-wide stdio list lock and then locks every open `FILE*`. If another Lua thread is reading a pipe from a slow `curl`, every `fopen()`/`fclose()` in FreeSWITCH waits for that `curl` to exit, including mod_lua loading the script for every REGISTER and INVITE. On 2026-09-21 a slow upload endpoint froze SIP signalling for 20–110 s at a time. Since then:

- Lua never runs HTTP inline. It writes a job file (microseconds, no subprocess, no network).
- Lua never calls `io.popen`. The fork's `emak_jobs.run_capture` runs commands through `os.execute` into a temp file.

## How it works

```
FreeSWITCH Lua (www-data)                     emak-jobs (www-data, systemd)
  emak_jobs.enqueue{...}                        inotify on new/
    write spool/tmp/<id>.json                   claim: rename new/<id> -> work/<id>   (atomic)
    rename to spool/new/<id>.json               HTTP request (named auth, timeout, expect regexp)
                                                success      -> done/   (pruned after 24 h)
                                                failure      -> retry/  backoff 5s 15s 45s 2m 5m 10m 30m 60m
                                                last attempt -> failed/ (dead letter)
                                                invalid job  -> failed/ at once, never retried
```

- **Durable.** Jobs are files. On start the worker moves anything left in `work/` back to `new/` before it claims anything, then queues `new/` and schedules `retry/` by each job's `next_at`. A 60 s rescan of `new/` catches anything inotify missed.
- **Stopping.** SIGTERM stops claims, cancels in-flight requests and puts those jobs back in `new/` without counting the attempt; it takes milliseconds. A request the server had already completed when it was cancelled runs again on start, so the endpoints must tolerate a repeat.
- **Bodies.** A job can stream a file as its body. With `delete_body_file`, the file is deleted only after the job is filed in `done/`, so a job is never left without its body. Lua's `copy_body = true` first snapshots the file into `spool/blobs/`, because callers delete the original right after enqueueing.
- **No secrets in the spool.** A job names its credential (`auth`) and the worker supplies it on every attempt:
  - `vm_fetch`: the voicemail storage routes, presigned (AWS SigV4 query form) with this server's own key from `EMAK_JOBS_VOICEMAIL_CREDENTIAL` (default `/etc/freeswitch/voicemail_storage_profile.xml`, the FreeSWITCH `http_cache` profile). See `voicemail_auth.go`.
  - `emak_api_bearer`: `Authorization: Bearer $EMAK_API_KEY`, from the process environment.
  - `none` (or empty): no credential.

## Job files

The writer is `emak_jobs.lua` in the FusionPBX fork (`app/switch/resources/scripts/resources/functions/`). Format version 1:

```json
{"v": 1, "id": "<uuid>", "type": "http", "label": "vm_transcribe <domain> <mailbox> msg_<uuid>",
 "created": "2026-10-09T20:00:00Z", "attempts": 0, "next_at": 0,
 "method": "POST", "url": "https://api.example.com/api/v0/voicemails/messages/transcribe/<uuid>",
 "auth": "vm_fetch", "body_file": "/var/spool/emak-jobs/blobs/<id>.wav", "delete_body_file": true,
 "content_type": "audio/wav", "expect": "\"success\"\\s*:\\s*true", "timeout_s": 90,
 "log_ok": "[voicemail] transcription OK: msg_<uuid>", "log_fail": "..."}
```

| field | meaning |
|---|---|
| `v` | format version. Missing means 1 (files written before the field existed); any other value is refused |
| `id` | required; the file is `<id>.json` |
| `type` | `http` (the only type; default) |
| `method` | `GET`, `HEAD`, `POST` (default), `PUT`, `PATCH`, `DELETE` |
| `url` | absolute http(s) URL, never with credentials in it |
| `auth` | `vm_fetch`, `emak_api_bearer`, `none` |
| `headers` | optional; never `Authorization` (use `auth`) |
| `body_file`, `content_type`, `delete_body_file` | optional streamed body; absolute path |
| `original_body_file` | the file `copy_body` snapshotted (for the logs) |
| `expect` | optional **Go regexp** the response body must match (a Lua pattern such as `%s` is refused) |
| `timeout_s` | 1–3600, default 120 |
| `label`, `log_ok`, `log_fail` | log text; `log_ok`/`log_fail` are re-emitted by the worker |
| `attempts`, `next_at`, `last_error`, `created` | managed by the writer and the worker |

A job that can never run as written (bad JSON, unknown `v`, type, method or auth, a bad `expect`…) goes straight to `failed/` with one `ERR job=… invalid: …` log line.

**The contract.** `testdata/jobs/` holds a file for every shape the Lua writer produces, and the tests prove the worker accepts each one and rewrites it on retry without losing a field. `emak-jobs check` runs the same validation from the command line; it also refuses fields the worker doesn't know (the running worker only warns about those). The fork's Lua test writes real job files through `emak_jobs.enqueue` and runs `emak-jobs check` on them, and the installer's drift check runs it on the last day of `done/`. Changing the format means a new `v`: the worker that reads it ships and runs everywhere before the Lua that writes it.

## Command line

```
emak-jobs                  run the worker
emak-jobs --version        print the version
emak-jobs check PATH...    validate job files, or every *.json in a directory; exit 1 if any is invalid
```

Settings (environment; the unit sets them):

| variable | default |
|---|---|
| `EMAK_JOBS_SPOOL` | `/var/spool/emak-jobs` |
| `EMAK_JOBS_WORKERS` | `4` |
| `EMAK_JOBS_MAX_ATTEMPTS` | `8` |
| `EMAK_JOBS_STATUS_FILE` | `/run/emak-jobs/status.json` |
| `EMAK_JOBS_DONE_KEEP_HOURS` | `24` |
| `EMAK_JOBS_VOICEMAIL_CREDENTIAL` | `/etc/freeswitch/voicemail_storage_profile.xml` |
| `EMAK_API_KEY` | none; only `emak_api_bearer` jobs need it |

## Installing

Our call servers get it from the installer (`fusion-install`, `resources/emak-jobs.sh`), which downloads the release pinned in its `config.sh`, checks the sha256 against the pin, installs the binary, `emak-jobs.service`, a drop-in that loads `EMAK_API_KEY` from `/etc/emak`, and `emak-jobs-replay`, then restarts and checks the worker's health, rolling back on failure.

By hand: copy the release's files to `/usr/local/emak-jobs/`, the unit to `/etc/systemd/system/`, create the spool (`www-data`, `0770`, subdirectories `new tmp work retry done failed blobs`), provide `EMAK_API_KEY` if you need `emak_api_bearer`, and `systemctl enable --now emak-jobs`.

## Operations

- **Health:** `cat /run/emak-jobs/status.json` (version, pid, counts, queue depth, `work`/`retry_dir`/`failed_dir`, p50/p95 latency, the last 50 results), written every 10 s. `journalctl -u emak-jobs` has one line per attempt: `OK`, `RETRY`, `DEAD`, `REQUEUE`, `ERR … invalid`.
- **Dead letters:** `emak-jobs-replay` lists `failed/`; `emak-jobs-replay --all` or `<id>…` re-queues with attempts reset; `--purge <id>` drops a job and its blob.
- **Alerting** is the installer's cron monitor: service down, stale `status.json`, `failed/` > 0, `retry/` > 50, `new/` > 100, the oldest job in `new/` older than 2 minutes.

## Development and releases

`make test` runs gofmt, `go vet` and `go test -race` in the Go image pinned by `.go-version`; the tests need Linux (inotify). CI runs the same on every push.

A release is a tag `vX.Y.Z` on `main` with a matching `CHANGELOG.md` section. The release workflow builds `emak-jobs-vX.Y.Z-linux-amd64.tar.gz` (binary, unit, replay, README) and its `.sha256` with `make release`, in the same pinned image, so `make release VERSION=vX.Y.Z` on any machine reproduces the published sha256.
