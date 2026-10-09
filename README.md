# emak-jobs — take HTTP out of the FreeSWITCH call path

Deployed 2026-09-21 13:35-14:21 (worker 13:35, helper 13:36, 13 scripts 13:36; copy_body fix 14:14; delete-log wording 14:21).
Backups: /usr/share/freeswitch/scripts/**/*.bak-20260921-*. Installed: /usr/local/emak-jobs/{emak-jobs,README.md}, /etc/systemd/system/emak-jobs.service, /var/spool/emak-jobs.

## Why

Incident 2026-09-21: desk phones "No Service" in waves. Root cause, verified by walking lock owners in
FreeSWITCH's memory during a stall (see the incident notes):

* Lua 5.2 `io.popen(cmd)` is implemented as `fflush(NULL); popen(cmd)`.
* `fflush(NULL)` takes glibc's process-wide stdio list lock and then locks every open `FILE*`.
* A `FILE*` that another Lua thread is reading (`handle:read("*a")` on a popen'd `curl`) stays locked until
  that curl exits. The R2 voicemail-upload endpoint slowed to 20–200 s per message around 11:25.
* While the flusher waits, every `fopen()`/`fclose()` in the process blocks — including mod_lua loading
  `app.lua` for every REGISTER / INVITE (`xml_handler`). 175 threads were queued behind the lock at one point.
  SIP signalling froze for as long as the slowest curl; RTP was unaffected.

There were 22 `io.popen`/`os.execute` curl calls with no timeout across `/usr/share/freeswitch/scripts`.
FusionPBX cache flushes made it far worse (every lookup then needs Lua + fopen + PG) but were not the cause.

## Design (Option A: spool directory + inotify worker)

```
FreeSWITCH (Lua, www-data)                     emak-jobs (Go, www-data, systemd)
  emak_jobs.enqueue{...}                          inotify(IN_MOVED_TO) on new/
    -> write spool/tmp/<id>.json                   -> rename new/<id> -> work/<id>   (atomic claim)
    -> os.rename to spool/new/<id>.json            -> HTTP request (auth from .env, timeout, expect-regex)
    (microseconds, no subprocess, no HTTP)         -> success: done/  (pruned after 24h)
                                                   -> failure: retry/ with backoff 5s,15s,45s,2m,5m,10m,30m,60m
                                                   -> after 8 attempts: failed/ (dead letter, logged ERR)
                                                  startup: work/ -> new/, sweep new/, reschedule retry/
                                                  60s housekeeping rescan of new/ (belt and braces)
                                                  /run/emak-jobs/status.json every 10s (counts, p50/p95)
```

* **No polling in the hot path.** Pickup latency is inotify latency (sub-millisecond); measured end-to-end
  from a FreeSWITCH Lua enqueue to HTTP 200 inside the same second.
* **Durable.** Jobs are files; a worker crash/restart loses nothing (work/ is re-queued on start).
* **Body snapshot (`copy_body = true`).** For uploads, `enqueue()` copies the file into `spool/blobs/<id>.<ext>`
  natively in Lua (no subprocess) and points the job at the copy with `delete_body_file=true`. Found necessary at
  14:12: `record_menu.lua` deletes `recorded_name.wav` on the line after the (now async) upload, and `index.lua`
  deletes the message in some multi-destination configurations — the job must not depend on the original.
* **No secrets in the spool.** Jobs carry `auth: "vm_fetch" | "emak_api_bearer"`. `vm_fetch` (the voicemail
  storage routes) is presigned on every attempt with this box's own key, read from
  `/etc/freeswitch/voicemail_storage_profile.xml` (`EMAK_JOBS_VOICEMAIL_CREDENTIAL`; Loi 5 U10, see
  `voicemail_auth.go`); `emak_api_bearer` reads `EMAK_API_KEY` from `/usr/share/freeswitch/scripts/.env`. `enqueue()` refuses
  URLs with embedded credentials.
* **Standard library only** (Go 1.26 on the box, inotify via `syscall`). Builds offline: `go build`.
* **Observable.** One log line per job (`OK|RETRY|DEAD job=<id> attempt=N dur_ms=... | <label>`), the
  original `[voicemail] R2 ... OK/FAILED` lines are re-emitted by the worker so existing log greps still work,
  and `status.json` gives counts, queue depth and latency percentiles for monitoring.

### Job file

```json
{"id":"<uuid>","type":"http","label":"vm_upload_message dom 1001 msg_<uuid>",
 "method":"POST","url":"https://api.example.com/api/v0/voicemails/messages/upload/dom/1001/<uuid>",
 "auth":"vm_fetch","body_file":"/var/lib/freeswitch/storage/voicemail/default/dom/1001/msg_<uuid>.wav",
 "content_type":"audio/wav","expect":"\"success\"\\s*:\\s*true","timeout_s":180,
 "delete_body_file":false,"log_ok":"[voicemail] R2 message upload OK: msg_<uuid>.wav","log_fail":"..."}
```

## What changes in the scripts (13 files, 434 diff lines, zero `io.popen` left)

| file | change | behaviour |
|---|---|---|
| `app/voicemail/index.lua` | message upload → job; `soxi -D` via `run_capture` | upload no longer blocks the caller's hang-up path; result logged by worker |
| `.../forward_add_intro.lua` | intro upload → job | same |
| `.../forward_to_extension.lua` | R2 copy → job | copy completes ms later; local copy already exists for the recipient |
| `.../record_menu.lua` | greeting upload stays **synchronous** (response carries the stored filename) but via `run_capture` with `curl -m 90`; recorded-name upload/delete → jobs | greeting flow unchanged; name upload no longer blocks |
| `.../delete_recording.lua`, `.../remove_deleted_messages.lua` | R2 delete → job | idempotent deletes, retried on failure |
| `.../listen_to_recording.lua` | `file -b --mime-type` popen → magic bytes (`emak_jobs.audio_ext`); **local-first playback**: if `msg_<uuid>.wav/.mp3` exists on disk, play it, else fetch from R2 | needed because uploads are now async: a message left seconds ago is playable immediately |
| `directory.lua` | `curl -I` popen → `http_get` via mod_http_cache (in-process libcurl; also warms the cache used by the following `streamFile`) | same decision, no subprocess |
| `recordings.lua` | JSON POST → job (`emak_api_bearer`), worker deletes the temp JSON on success; script reports "202 Queued" | the "saved" prompt no longer waits for the API |
| `page.lua`, `magic_routing.lua`, `app/sms/index.lua`, `app/fax/.../hangup_rx.lua` | `io.popen` → `run_capture` (synchronous, bounded by `timeout`) | identical semantics, no global lock; magic_routing keeps its 10 s API budget (+3 s) |

`run_capture(cmd, timeout_s)` = `os.execute("timeout N sh -c '<cmd>' > tmpfile")` + read tmpfile. glibc 2.31's
`system()` uses `posix_spawn` and does **not** call `fflush(NULL)`; only Lua's `io.popen` adds that.

Reviewed and deliberately left as-is: Gemini transcription in `index.lua` (in-process `ssl.https`, blocks only
its own session thread — candidate for a phase-2 `transcribe` job type), `mkdir.lua`/`copy.lua` (`os.execute`,
fine), `app/servers/resources/clear_cache.lua` (`rm /var/cache/fusionpbx/*` — this is one way the cache gets
wiped; consider removing that ability or at least logging who calls it).

### Things to verify in review (behavioural, not mechanical)

1. **Playback race**: with async upload, `listen_to_recording.lua` now plays the local file when present. Any
   *other* consumer that reads a just-left message from R2 (web UI, mobile app, email link) sees it a few
   seconds later than before (worker p95 is expected ≈ API upload time, currently 20–200 s because the
   endpoint is slow — that is the API team's item, see below).
2. **Local file lifecycle**: nothing in this patch deletes local voicemail files; whatever currently cleans
   them (the 2026-05-20 "delete local after R2 upload" work) must not delete a file before its job is done.
   `done/<id>.json` records `body_file`, so a cleanup script can check the spool if needed.
3. **`recordings.lua` return value**: callers get `true` once queued (previously: once uploaded). Grep for
   callers that branch on it.
4. **Spool filling up**: if the API is down, `retry/` grows (one small JSON per job) — harmless, but
   `status.json.retry_dir` should be alerted on (> 50 for > 10 min).

## Rollout (≈15 minutes, no FreeSWITCH restart; Lua scripts reload on next execution)

```
cd <this dir>
./deploy.sh            # prints the plan
./deploy.sh worker     # spool dirs (www-data 0770), binary, unit; starts idle
./deploy.sh lua        # installs resources/functions/emak_jobs.lua
./deploy.sh scripts    # applies the 13 patches with .bak-<stamp> backups; refuses files that changed since the diff was made
```

Then the smoke test: leave a voicemail on a test mailbox, then

```
journalctl -u emak-jobs -f         # expect: OK job=... dur_ms=... | vm_upload_message ... and the [voicemail] R2 message upload OK line
grep -a "upload queued" /var/log/freeswitch/freeswitch.log | tail
```

Suggested order if you want to stage it: `index.lua` + `listen_to_recording.lua` first (the actual offender
and its playback fallback), watch 30 min, then the rest.

### Rollback

```
for f in $(find /usr/share/freeswitch/scripts -name "*.bak-<stamp>"); do mv "$f" "${f%.bak-<stamp>}"; done
systemctl disable --now emak-jobs      # optional; an idle worker is harmless
```
Jobs still in `new/`/`retry/` at rollback time can be replayed later by starting the worker again.

## Monitoring after deploy

1. `./monitor_after.sh >> /var/tmp/emak-jobs-monitor.log` (10 s cadence, 24 h): new-channel gap (stall
   detector; anything ≥ 15 s is flagged), top futex pile-up, worker counters. Expected: no gaps ≥ 15 s,
   `top_futex` counts in the tens, not hundreds.
2. `watch -n5 cat /run/emak-jobs/status.json` — `inflight` ≤ 4, `new` ≈ 0, `retry_dir` small, `dead` = 0.
3. `journalctl -u emak-jobs | grep -c ' OK '` vs `grep -c ' DEAD '`.
4. Voicemail upload latency (`dur_ms` p50/p95 in status.json). This is now purely the API's latency and is the
   number to hand to whoever owns the API host: uploads of ~500 KB WAVs were taking 20–200 s server-side
   this morning while a 10 MB blob to the same host round-trips in 0.3 s.
5. The pre-existing signal: `UdpRcvbufErrors` in `nstat` must stop increasing during business hours.

## Operations

### Alerting (installed 2026-09-21 14:58)
`/opt/memwatch/emak_jobs_monitor.sh` runs from root's crontab every minute (same Telegram/Postmark helpers and
state-file conventions as `web_monitor.sh`). Alerts once, re-alerts hourly while the condition persists, sends a
recovery notice. Conditions:

| condition | meaning |
|---|---|
| `emak-jobs.service` not active | worker down (systemd restarts it; if it stays down, look at `journalctl -u emak-jobs`) |
| `status.json` older than 60 s | worker hung |
| `failed/` > 0 | dead-lettered jobs — voicemail uploads stranded after ~1 h 48 min of retries; run the replay below |
| `retry/` > 50 | R2/API degraded (jobs are safe, just late) |
| `new/` > 100 | worker not keeping up (raise `EMAK_JOBS_WORKERS`) |
| Lua `[ERR]` from any patched voicemail script in the last minute | a code path regressed — see `freeswitch.log` |

Dry run: `EMAK_JOBS_MONITOR_DRYRUN=1 /opt/memwatch/emak_jobs_monitor.sh`. Log: `/var/tmp/emak_jobs_monitor.log`.

### Replaying dead-lettered jobs
```
emak-jobs-replay                 # list failed jobs with their last error
emak-jobs-replay --all           # re-queue everything in failed/ (attempts reset; worker picks up instantly)
emak-jobs-replay <id> [<id>...]  # re-queue specific jobs
emak-jobs-replay --purge <id>    # drop a job for good (also removes its blob copy)
```
Jobs are idempotent on the API side (upload = overwrite, delete = no-op if absent, copy = overwrite), so replaying is
always safe. Run as root or `www-data`.

### Day-2 checks (until every path has seen real traffic)
Paths still only mechanically verified as of 2026-09-21 15:00: `remove_deleted_messages` (retention purge),
`recordings.lua`, `page.lua`, `magic_routing.lua`, `sms/index.lua`, `fax/hangup_rx.lua`. The cron monitor alerts on
any Lua error from them. To see when they first ran:
```
journalctl -u emak-jobs | grep -E "recordings_upload|remove_deleted_messages" | head
grep -a "\[ERR\].*lua" /var/log/freeswitch/freeswitch.log | grep -E "page.lua|magic_routing|sms/index|hangup_rx|remove_deleted"
```
Health at a glance: `cat /run/emak-jobs/status.json`, `journalctl -u emak-jobs -n 20`, `ls /var/spool/emak-jobs/{new,retry,failed}`.

## Phase 2 (not in this patch)

* `transcribe` job type: move the Gemini call + transcription PATCH out of `index.lua` into the worker
  (currently reads the whole WAV into memory and blocks the session thread for the API round-trip).
* Generic `exec` job type for `sox`/`lame` conversions if any remain in call paths.
* Alert on `status.json` (`dead > 0`, `retry_dir > 50`) via the existing memwatch/Telegram path.
