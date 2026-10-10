# Changelog

## v1.0.0

First release from this repository; previously built on each server from the FusionPBX fork.

- **Job format version.** Job files carry `"v": 1`. A file without `v` reads as v1 and is rewritten with it on retry; any other value is refused.
- **Invalid jobs fail at once.** Bad JSON, an unknown `v`, type, method or auth, negative or absurd `attempts`, a negative `next_at` or one more than a day ahead, a bad or Lua-pattern `expect`, a relative URL or body path, credentials in the URL or an `Authorization` header go straight to `failed/` with one log line, instead of eight attempts over about two hours.
- **`emak-jobs check PATH...`** validates job files without running them, and refuses unknown or mis-cased fields. The running worker accepts them with a warning.
- **`emak-jobs --version`;** the version, pid and start time are also logged and written to `status.json`.
- **Any other arguments exit 2 with the usage.** Before v1.0.0 the binary ignored its arguments and started a worker; on 2026-10-09 that ran a second worker as root on f1-dev's live spool.
- **Refuses to run as root:** it would leave root-owned files in the spool that the `www-data` service can't use.
- **`EMAK_API_KEY` comes from the process environment** (the installer's drop-in loads `/etc/emak`). `EMAK_JOBS_ENV_FILE` and the `.env` parser are gone.
- **One worker per spool** (an flock on `<spool>/.lock`): a second instance would recover the first one's in-flight jobs and run them twice.
- **A panic costs one job:** the job goes to `failed/`, the worker keeps running, and `status.json` counts it (`panic`).
- **Queue:** a path already queued isn't queued again by the 60 s rescan; a due retry waits for room in a full queue instead of being dropped.
- **Clean stop.** SIGTERM stops claims, cancels in-flight requests and puts their jobs back in `new/` without counting the attempt.
- **Fixes:**
  - On start, a job claimed in the first milliseconds could be moved back from `work/` and run twice at the same time. Recovery now finishes before anything is claimed.
  - On success, the body was deleted before the job reached `done/`. A kill in between left a job that could never run again; the body is now deleted after the job is filed.
  - A job in `retry/` could be claimed before its `next_at`. It now goes back to wait.
  - Write and rename errors while filing a job are logged instead of ignored.
- **Unit:** drops `EMAK_JOBS_ENV_FILE`; adds `KillMode=control-group` and `TimeoutStopSec=10`.
- **Release builds are reproducible** (`make release`).
