# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Python toolbox (Windows-oriented) for analysing Mirth Connect log files, monitoring system resources, and emailing the resulting reports. Four standalone scripts at the repo root (`mirth_logs_parser.py`, `system_state.py`, `quickmail.py`, `mirth_api.py` — the latter a Mirth Connect REST client) run directly via CLI or through the interactive `_cmd_helper/launch.bat` menu. On top of them, **`checker_service.py`** is a long-running web service (the `lib/` package + `web/` pages) that reuses these scripts as libraries, records a time-series in SQLite, and serves dashboards.

## Environment & common commands

The `.bat` helpers in `_cmd_helper/` all `cd` to the repo root first and (except `venv_create`) auto-create the venv if missing. They are written for `cmd.exe`; this machine's default shell is PowerShell, so call them as `cmd /c _cmd_helper\launch.bat` if needed.

- Create venv: `_cmd_helper\venv_create.bat` (creates `venv/` at root)
- Load venv interactively: `_cmd_helper\venv_load.bat` (pass `--no-shell` to set up the env without spawning a sub-shell)
- Install/update deps: `_cmd_helper\update.bat` (upgrades pip, installs `requirements.txt`)
- Interactive launcher: `_cmd_helper\launch.bat` — lists root `.py` files (skips dotfiles like `.smtp_config.py`), shows each script's `-h` syntax, then prompts for args
- Build standalone `.exe`s: `_cmd_helper\_compilation.bat` (PyInstaller `--onefile` → `dist/`)

Running scripts directly:
- `python mirth_logs_parser.py [logfile] [-d DATE] [-r] [-m EMAIL]` — `-d 1`=all, `0`=today, `-1`=J-1, `-X`=J-X; `-r` also parses logrotate files (`{filename}.x`); `-m` emails the report. Default logfile is the standard Mirth Connect server log `C:\Program Files\Mirth Connect\logs\mirthconnect.log` (the bundled `Ressources\mirth-exemple.log` remains available as a sample).
- `python system_state.py [-c "chrome,python"]` — `-c` filters to named processes (comma-separated)
- `python quickmail.py -s SUBJECT -m MESSAGE -d DEST` — direct send; exits 0 on success, 1 on failure
- `python mirth_api.py [-t TIMEOUT] [-s server,channels,stats,errors] [-c NAME]` — queries the Mirth REST server and prints a tabulate report; exits 1 if the server is unreachable. Config from env / `.mirth_config.py` (see Web service section).

There is no test suite, linter, or CI configured.

## Architecture

**`mirth_logs_parser.py`** — the data layer is three pure functions (`mirth_file_parser` groups multi-line entries; `mirth_log_decoder` extracts level/channel/thread/class/`Caused by:`; `mirth_log_filter`). Everything CLI- and presentation-related (`display_statistics`, table printing, HTML report generation, `main()`) lives *inside* the `if __name__ == '__main__':` block, so importing the module gives you only the parsing primitives. When `-m` is set, stdout from `display_statistics` is captured via `contextlib.redirect_stdout` into a buffer, then converted to HTML (`format_report_to_html`) and sent through `quickmail.sendmail`.

**`system_state.py`** — a flat library of `get_*` probe functions (hostname, CPU, memory, disk, network I/O, TCP/UDP sockets, VPN detection via Windows adapter descriptions, ping) using `psutil` and `ping3`. The CLI block at the bottom orchestrates these into `tabulate` tables.

**`quickmail.py`** — used both as a CLI and as the import dependency for the parser's email feature. `sendmail()` is the public entry point; it auto-handles SSL vs STARTTLS (STARTTLS is forced when port == 587).

Both `mirth_logs_parser.py` and `system_state.py` define local `safe_print`/`print_table` helpers that fall back from Unicode `fancy_grid` to ASCII tables — needed for Windows terminals that can't encode box-drawing characters. Preserve this fallback when touching output code.

## Web service (`checker_service.py` + `lib/` + `web/`)

Run with `python checker_service.py [--host 0.0.0.0] [--port 8800] [--interval 60] [--stagger 5] [--logfile ...] [--no-browser]`. It needs the venv (`tabulate`, `psutil`). It exposes a JSON API + static dashboards and runs background collectors writing to a SQLite history.

- **`lib/webserver.py`** — minimal `ThreadingHTTPServer` with a `Router` (regex routes, `{param}` segments), static file serving from `web/`, and a `json_transform` hook (the service uses it to ceil-round all floats to 2 decimals). Handlers take a `Request` and return a dict/list (→JSON 200), a `(status, payload)` tuple, or a `Response` (raw bytes/content-type).
- **`lib/scheduler.py`** — `RecurringTask` runs a function on a daemon thread every `interval` s (duration-compensated, interruptible). `start_delay` offsets the first run; **`start_staggered(tasks, step=5.0)`** launches a list of tasks each delayed `n*step` s so their probes don't all hit the machine on the same tick. `main()` runs two collectors (`metrics-collector`, `mirth-collector`) this way.
- **`lib/database.py`** — SQLite (WAL) `metrics` table. Every row carries a **`tag`** column identifying its source: `system` (CPU/mem/disk of the host) or `mirth` (CPU/mem/`sockets` of the Mirth process). The schema is migrated in `init_db` (adds `event`, `tag`, `sockets` to old DBs; legacy rows become `tag='system'`). `get_history`/`get_latest`/`get_last_valid`/`insert_event_marker` are tag-aware (default `system`). Event markers (`boot`/`restart`) are null-metric rows tagged `system` that break the chart line on outages. **When adding a new metric source, just pick a new tag — no schema change.**
- **`mirth_api.py`** (repo root, used as a library by the service) — stdlib-only client for the Mirth Connect REST API (cookie-jar login, TLS unverified by default for Mirth's self-signed cert). Same two-part layout as `system_state.py`: importable `get_*` functions that never raise (errors come back as `{reachable: False, error}`) + a `tabulate` CLI (`python mirth_api.py [-t TIMEOUT] [-s server,channels,stats,errors] [-c NAME]`). Library entry points: `get_overview()` (full: version + JVM stats + channels + `totals` aggregated across channels), `get_channels_overview()`, `get_global_statistics()`, `get_server_info()`, `get_errors()`. Each channel dict is `{name, channel_id, state, received, filtered, queued, sent, error}`. **Statistics parsing is deliberately defensive** (`_parse_statistics`): Mirth serialises the `Map<Status,Long>` with the *enum's fully-qualified class name* as the entry key (`com.mirth.connect.donkey.model.message.Status`), not `string` — so the key is taken as the first non-counter textual value; it also handles plain-dict, `entry`-array, child-connector aggregation, and an optional `/channels/statistics` fallback. Config precedence mirrors quickmail: env vars (`MIRTH_BASE_URL`, `MIRTH_USER`, `MIRTH_PASSWORD`, `MIRTH_VERIFY_SSL`, `MIRTH_PROCESS`) > `.mirth_config.py` (git-ignored; `.mirth_config.py.template` is the stub) > defaults. `MIRTH_PROCESS` (default `mcservice.exe`) is the process name historised under the `mirth` tag.

Key API routes (full list/playground in `web/api.html`): `/api/history?tag=system|mirth|all&hours=…|date_deb=…&date_fin=…`, `/api/history/latest?tag=…`, `/api/mirth/api` (REST overview, incl. `totals`), `/api/mirth/channels` (channel list, `?channel=` filter), `/api/mirth/stats` (aggregated totals), `/api/mirth/server` (version/JVM), `/api/mirth/errors` (channels in error), `/api/getmirthinfo?type=server,stats,channels,errors&format=json|text|html` (multi-format, one login feeds all sections — mirrors `/api/getsysteminfo`), `/api/mirth/process` (live process snapshot), `/api/db/*` (info/integrity/export/vacuum/purge/reset/import), `/api/status` (`schedulers` lists every task). Note `tag` absent/empty ⇒ `system`; use `tag=all` for every source (the server can't see an empty query param — `parse_qs` drops it).

The dashboards (`web/statistiques.html`) have two sections: **🖥 Système** (host history chart) and **🩺 Supervision Mirth** (Mirth-process cards + history chart with a secondary sockets axis, plus a channels table fed by `/api/mirth/api`). All accumulation/rendering is client-side; auto-refresh pulls only new points (`date_deb=last`), and the Mirth REST overview is polled less often (login per call).

## SMTP configuration & secrets (important)

`quickmail.py` resolves config in this precedence: **environment variables** (`SMTP_SERVER`, `SMTP_PORT`, `USE_SSL`, `SMTP_USER`, `SMTP_PASSWORD`, `SENDER_ADDRESS`) > `.smtp_config.py` > hardcoded defaults.

- `.smtp_config.py` holds real credentials and is **git-ignored**. `.smtp_config.py.template` is the tracked stub — copy it to bootstrap: `copy .smtp_config.py.template .smtp_config.py`.
- The config is loaded dynamically via `importlib` from `base_dir`, which is `sys._MEIPASS` when frozen by PyInstaller, else the script dir. This is what lets the compiled `.exe` find its bundled config.
- **`_compilation.bat` bakes `.smtp_config.py` into the `quickmail`, `mirth_logs_parser` and `checker_service` `.exe`s** via `--add-data ".smtp_config.py;."`. The resulting executables are self-contained and ship your real SMTP credentials inside them — do not distribute them publicly. (`system_state.exe` and `mirth_api.exe` are built without it.) Caveat: the `checker_service.exe` build bundles only the SMTP config — it does **not** bundle the `web/` static pages or `.mirth_config.py`, so the frozen service expects those next to it (the Mirth client then falls back to env vars / defaults).

`.gitignore` excludes `dist/`, `build/`, `venv/`, `__pycache__/`, `*.spec`, `.smtp_config.py`, `.mirth_config.py`, and the `checker_history.db*` files. The `.spec` files present in the tree are untracked build artifacts. The Mirth REST client follows the same secrets pattern as SMTP — see `mirth_api.py` (repo root) and `.mirth_config.py.template` in the Web service section above.

## Dependencies

`requirements.txt`: `tabulate==0.9.0`, `psutil`, `ping3`. `pyinstaller` is needed for compilation but is not pinned in requirements — `_compilation.bat` locates the installed `pyinstaller.exe` and installs requirements before building.
