# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Dev install (what CI does)
pip install --group test
pip install -e ".[all]"

# Full test suite, as run in CI
pytest --cov=pytroll_collectors pytroll_collectors/tests --cov-report=xml

# Single module / class / test / keyword
pytest pytroll_collectors/tests/test_segments.py
pytest pytroll_collectors/tests/test_segments.py::TestSegmentGatherer
pytest pytroll_collectors/tests/test_region_collector.py::test_adjust_timeout
pytest pytroll_collectors/tests -k tolerance

# Lint
ruff check .
pre-commit run --all-files

# Docs
cd doc && make html
```

Two tests in `test_fsspec_to_message.py` (`TestUnpackMessage::test_pack_local_file_extract_filesystem`) open a real
SFTP connection to the local host on port 22; they fail with `NoValidConnectionsError` unless an SSH server is
reachable there. `test_region_collector.py::test_faulty_end_time` is permanently skipped ("test never finishes") —
it reproduces the open infinite-loop bug in `_adjust_end_time` handling.

## Architecture

Each tool in this package is an independent long-running posttroll daemon: a library module under
`pytroll_collectors/`, plus a thin `pytroll_collectors/scripts/<name>.py` with `main()` registered as a console
script in `pyproject.toml` (entry-point names deliberately keep the `.py` suffix, e.g. `segment_gatherer.py`).
`bin/` holds two obsolete scripts that are not installed.

### Two independent collection engines

**`segments.py` — SegmentGatherer (time-slot based).** Incoming messages are bucketed into `Slot`s keyed by
`time_name`, with `time_tolerance` deciding whether a message joins an existing slot (`_find_time_slot`). Each
configured *pattern* declares `critical_files` / `wanted_files` / `all_files` sets; `Slot.get_status()` computes a
per-pattern `Status`, and `get_collection_status()` merges those into the slot-level decision (`SLOT_READY`,
`SLOT_NONCRITICAL_NOT_READY`, `SLOT_READY_BUT_WAIT_FOR_MORE`, `SLOT_OBSOLETE_TIMEOUT`). `triage_slots()` walks
slots on each loop iteration and `_reinitialize_gatherer()` publishes and clears them. How a posttroll message is
matched to a pattern is pluggable via the `Parser` ABC: `UIDParser` (trollsift filename pattern against `uid`) or
`MessageParser` (`message_keys` present in the message plus a topic prefix).

**`region_collector.py` + `geographic_gatherer.py` — RegionCollector (area-coverage based).** Uses pyresample area
definitions plus pytroll-schedule/pyorbital `Pass` objects to predict which granule times will cover the region
(`_predict_pass_granules`), then collects until `planned_granule_times` is a subset of the received
`granule_times` or the timeout fires. Missing TLEs surface as `KeyError` from pyorbital and are deliberately
caught one level up, in the trigger.

### Trigger layer (`triggers/`) — geographic gatherer only

`Trigger` in `_base.py` owns the flow: `_process_metadata()` feeds metadata to each collector (swallowing
`KeyError`, which is how missing-TLE crashes are contained) and `publish_collection()` merges granule metadata
into a single `collection` message via `_merge_metadata()`. Two input sources implement it: `PostTrollTrigger`
(subscribes to posttroll topics) and `WatchDogTrigger` (filesystem events). `triggers/__init__.py` imports
`WatchDogTrigger` defensively and leaves it as `None` if watchdog is unavailable. `TriggerFactory` in
`geographic_gatherer.py` chooses between them per config section by *trying* the watchdog path and falling back
to posttroll on `KeyError`/`ValueError`.

### Configuration: ini vs yaml, and mismatched CLI flags

The geographic gatherer reads **ini only** (`ConfigParser`, one section per collection item). The segment gatherer
accepts **both**: yaml through `helper_functions.read_yaml`, or ini through `segments.ini_to_dict(config, section)`
— but only when a config item is named; without it an ini file is handed to the yaml parser and dies with a
`ParserError`. The two tools also spell their flags differently:

- `segment_gatherer.py -c <file> -C <section>` (`--config` / `--config_item`)
- `geographic_gatherer.py -c <section> ... <file>` (`--config-item`, repeatable; the file is positional)

`examples/` holds templates for both formats and `doc/source/index.rst` pulls several of them in with
`literalinclude`, so config-key changes must be reflected in the example templates or the docs go stale.

### Cross-cutting conventions

- **Messaging.** Message types are filtered on the way in: `_MessageProcessor` (posttroll trigger) accepts
  `file`, `collection` and `dataset`, while `SegmentGatherer.run()` accepts only `file` and `dataset`. Publishers are
  always built through `utils.create_publisher_config_dict()` + `utils.create_started_publisher_from_config()`,
  and nameserver CLI values pass through `utils.check_nameserver_options()`, where the string `"false"` and
  `False` both mean "no nameserver".
- **Time.** Everything is normalised to timezone-aware UTC by `utils.ensure_utc_aware()` and
  `utils.fix_start_end_time()` (which also derives `end_time` from `duration`, merges `start_date`/`end_date`, and
  rolls `end_time` forward past midnight). Use `dt.datetime.now(dt.timezone.utc)` in new code; there are no
  `utcnow()` calls left. Times parsed from 2met messages are made UTC-aware by `scisys._strptime_utc()`.
- **Logging.** `logging.setup_logging(opts, name)` is the single entry point for every script. It prefers
  `opts.log_config`, falls back to the legacy `opts.stalker_log_config`, and only then to the old
  `-l`/`--verbose` behaviour. `.yaml`/`.yml` configs go through `dictConfig`, anything else through `fileConfig`.
- **Shutdown.** Both gatherers install their SIGTERM handler *before* messaging is set up (in
  `GeographicGatherer.__init__`, and at the top of `SegmentGatherer.run()`), because publisher/subscriber setup
  can block for a long time on an unresponsive nameserver. They then *drain*: `_keep_running()` returns `False`
  only once in-flight slots/collections are finished. Tests run the gatherers in child processes and signal them,
  which is why `[tool.coverage.run]` sets `sigterm = true` and multiprocessing concurrency.
- **Object storage.** `s3stalker.py` (one-shot) and `s3stalker_daemon_runner.py` (long-running) list buckets with
  s3fs and convert listings to messages via `fsspec_to_message.py`, which serialises the filesystem itself into
  the message so consumers can reconstruct it. The segment gatherer can likewise check S3 for already-present
  segments (`check_and_add_existing_files`).
- **Optional dependencies.** Each daemon has its own extra in `pyproject.toml` (`geographic_gatherer`,
  `trollstalker`, `s3stalker`, `s3_segment_gatherer`, `scisys_receiver`). Keep pyresample, trollsched, watchdog
  and s3fs imports out of the shared import path so the other tools stay installable without them.

## Repo conventions

- ruff with `line-length = 120` and rule sets `A, E, W, TID, T10`; docstring convention is google (pydocstyle
  rules are not enabled yet, but every module/class/function has a docstring).
- Source files carry no per-file license header — Apache-2.0 is declared once in `LICENSE` and `pyproject.toml`.
- The version comes from git tags via hatch-vcs; `pytroll_collectors/version.py` is generated and must not be
  committed.
- `CHANGELOG.md` is generated with `loghub` at release time — see `RELEASING.md`, and do not hand-edit it.
