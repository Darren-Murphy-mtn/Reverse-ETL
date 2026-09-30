# GitHub ELT + Reverse ETL

[![ci](https://github.com/Darren-Murphy-mtn/Reverse-ETL/actions/workflows/ci.yml/badge.svg)](https://github.com/Darren-Murphy-mtn/Reverse-ETL/actions/workflows/ci.yml)

A small, complete data pipeline over GitHub issue activity. It **extracts** issues, comments, and events from the GitHub REST API, **loads** them untouched into DuckDB, **transforms** them with dbt into tested marts, then runs **reverse ETL**: each row of `fct_stale_issues` becomes a task in a ClickUp list.

GitHub is read-only. The warehouse computes something no single API call can (last *human* activity, excluding bots), and pushes that answer to where someone would act on it.

This is a portfolio-scale project built with free, local tooling. It's meant to show ELT fundamentals, dbt layering and testing discipline, and the reverse-ETL pattern done carefully. It is not a production system; [what I'd change at scale](#what-id-do-differently-at-scale) is spelled out below.

## Architecture

```
                        GitHub REST API
          (vercel/next.js, dbt-labs/dbt-core, sandbox)
                              │
      extract/github_extract.py   Link-header pagination, rate-limit aware
                              │
                              ▼
      data/raw/json/runs/<run_id>/<repo>/…      raw pages, never overwritten (audit trail)
                              │
      extract/to_parquet.py   latest run per repo → one Parquet per entity
                              ▼
      load/load_to_duckdb.py  → raw.raw_issues / raw_comments / raw_events / raw_extraction_runs
                              │
                              ▼
      dbt   staging  ─►  intermediate  ─►  marts        (8 models, 35 tests: `dbt build`)
                                              │
                                   marts.fct_stale_issues
                                              │
      reverse_etl/sync_stale_issues.py        ▼
          dry-run by default · idempotent · freshness-guarded · rate-limit aware · logged
                              │
      reverse_etl/organize_clickup.py    folder + one list per repo · tags · priority
                              │
      reverse_etl/backfill_dates.py      start date (GitHub created) · due (stale by)
                              ▼
            ClickUp folder "Stale issues" — one task per stale issue
```

`pytorch/pytorch` is not in the extract. Its quiet-open set is about 13,000 issues, which is the wrong volume to write into one demo list. The window is 30 days, the shortest span that still supports the 30-day stale rule.

## dbt models

Lineage (generated from the dbt manifest): [`docs/lineage.md`](docs/lineage.md). `make docs` serves the interactive dbt docs.

<!-- Screenshot of the dbt docs lineage graph goes here: docs/lineage.png -->

| Layer | Model | What it is |
|---|---|---|
| staging | `stg_issues` | Issues and PRs, typed and renamed, deduplicated across extraction streams |
| staging | `stg_comments` | Comments; issue key parsed from `issue_url`; flags the pipeline's own marker comments |
| staging | `stg_events` | Labeled / closed / reopened / … events |
| staging | `stg_extraction_runs` | Window start and snapshot time per repo |
| intermediate | `int_issue_lifecycle` | One row per issue: first response, reopen count, label history (`+bug > +triage > -triage`), last human activity |
| marts | `fct_issue_resolution_time` | Resolution hours per closed issue, bucketed via a macro |
| marts | `fct_contributor_activity` | Opened / closed / commented per contributor per repo per week |
| marts | `fct_stale_issues` | Open issues past their repo's staleness threshold. **The reverse-ETL input.** |

Layering rules: staging reads only from `source()`, never joins, and holds no business logic. Everything downstream uses `ref()`. Nothing downstream of staging touches raw.

## Tests

`dbt build` runs 35 data tests alongside the models:

- `unique` + `not_null` on every primary key (`issue_id`, `comment_id`, `event_id`, and the mart grains)
- `accepted_values` on issue `state` and on the resolution bucket
- `relationships`: `stg_comments.issue_id → stg_issues.issue_id` (error) and `stg_events.issue_id → stg_issues.issue_id` (warn; see tradeoffs)
- Singular tests: closed-after-created, first-response-after-created, and a guard that `fct_stale_issues` only contains open, non-PR issues past threshold

Python unit tests (`make test`) cover the parts dbt can't: pagination, primary/secondary rate-limit handling, retries, the events early-stop, ClickUp idempotency, the 429 backoff, the failure path, and a check that the Python and dbt configs agree on the marker, label, and sandbox repo.

CI runs lint, unit tests, and the full `fixture → Parquet → DuckDB → dbt build → dry-run sync` path on every push. It uses a deterministic **synthetic** fixture (`scripts/generate_fixture.py`) shaped like the real API payloads, so CI needs no tokens and no network, and no real users' content lives in the repo.

## Running it

Requires Python 3.11+.

```bash
make install
cp .env.example .env           # GITHUB_READ_TOKEN, CLICKUP_API_KEY, CLICKUP_LIST_ID

make extract                   # GitHub → raw JSON → Parquet
make load                      # Parquet → DuckDB raw.*
make transform                 # dbt build (models + tests)
make sync                      # reverse ETL dry run: prints the plan, makes no API calls
make sync-live                 # create ClickUp tasks, and only in CLICKUP_LIST_ID
```

`make ci` reproduces CI locally against the fixture. `make help` lists all targets.

**Tokens.**

| Variable | What it is |
|---|---|
| `GITHUB_READ_TOKEN` | Fine-grained PAT, repository access "Public repositories (read-only)", no extra permissions |
| `CLICKUP_API_KEY` | Personal token from Settings > ClickUp API. Sent as the raw `Authorization` value, with no `Bearer` prefix |
| `CLICKUP_LIST_ID` | Numeric id of the destination list, the number after `/li/` in the list URL |

GitHub is never written to. `CLICKUP_LIST_ID` is the only destination.

**Config.** Source repos and the 30-day window live in `config/pipeline.yml`. Staleness thresholds live in `dbt_project.yml` vars: 30 days by default, and 0 for the sandbox repo, which the fixture uses so brand-new seeded issues still qualify.

## Reverse ETL guardrails

- **Dry run by default.** Without `--live`, the script prints the plan and a sample task description and never touches the ClickUp API.
- **One list.** Live mode writes only to `CLICKUP_LIST_ID`.
- **Idempotent.** Before creating, live mode pages through the list's existing tasks, including closed ones, and skips any issue whose URL is already in a task description.
- **Task body.** The name is the issue title. The description is the repo and the issue URL. The ClickUp start date is the GitHub created date. The due date is the stale-by date: last human activity plus the repo threshold. A lower bound is marked on that line. Days stale is not stored. A number written once would freeze, and updating it on every task would spend the free plan's 60 custom-field uses.
- **Snapshot freshness guard.** Reverse ETL pushes a snapshot, not a live value. Live mode refuses to run if the extraction is older than `max_snapshot_age_hours` (24h).
- **Audited.** Every write attempt (payload, status, task URL or error) is appended to `logs/reverse_etl/sync_<ts>.jsonl`.
- **Rate-limit aware.** The free-plan token allows 100 requests per minute. The client reads `X-RateLimit-Remaining` and, on 429, sleeps until `X-RateLimit-Reset`.
- **Filed in ClickUp, not dumped in one list.** `reverse_etl/organize_clickup.py` puts tasks in a folder named `Stale issues`, one list per repo. GitHub labels are normalized to a small tag set (`bug`, `feature`, `docs`, `tech-debt`, `triage`, `needs-repro`). Bugs and needs-repro are high priority, docs are low, everything else is normal. Days stale is not used as a rank: this 30-day window can only prove "at least 30" for almost every row. Custom Fields are not written. The free plan allows 60 Custom Field uses for the life of the workspace.

## Design decisions and tradeoffs

- **Repo-level endpoints instead of per-issue calls.** `/issues/comments` and `/issues/events` return a whole repo's activity in pages of 100, which is orders of magnitude fewer requests than calling per issue.
- **Two issue streams.** The windowed pull (`since=`) misses exactly the issues that matter most for staleness: open ones untouched for longer than the window. A second pull of all open issues fills that gap, and `stg_issues` deduplicates the overlap.
- **Staleness as a lower bound.** Comments and events are only extracted inside the window. If an issue shows no activity there, the model can only say "at least N days", so it floors at the window start and exposes `last_activity_is_lower_bound` instead of inventing a precise number. The ClickUp task wording follows that flag. The window is 30 days so that lower bound still meets the stale threshold.
- **Natural key `owner/repo#number`.** Comments reference their issue only by URL, never by GitHub's numeric id. Deriving the same key in every staging model keeps staging join-free and makes the relationships test possible.
- **Events relationship test is `warn`.** Some events (e.g. `referenced` from a commit) don't bump the parent issue's `updated_at`, so the parent can legitimately fall outside the windowed pull. The intermediate layer inner-joins these orphans away; the warning keeps them visible.
- **Staleness measured at snapshot time**, not query time. The marts are reproducible for a given extraction, and they say what the sync would actually be acting on.
- **Raw runs are append-only.** Each extraction writes to its own run directory, and Parquet is rebuilt from the latest run per repo. Transforms can be re-run or debugged without re-hitting the API.
- **Two metadata columns added at landing** (`_source_repo`, `_run_id`), following the loader convention of stamping lineage onto raw rows. Nothing else is reshaped.
- **Full refresh, not incremental.** At this project's volume full rebuilds are cheap, so incrementality would add complexity without benefit.

## What I'd do differently at scale

- **Incremental everything.** Extraction would keep a high-water mark per repo and stream (`updated_at`) instead of re-pulling the window. The large dbt models would become `incremental` with `unique_key` merges, and late-arriving updates would be handled with a lookback.
- **Orchestration.** Airflow or Dagster instead of `make`, with scheduled runs, retries, backfills, and the reverse-ETL sync gated on a successful `dbt build` plus source freshness checks.
- **Partitioning and storage.** Raw data in object storage (S3 + Iceberg or Delta), partitioned by repo and date, and a cloud warehouse instead of one DuckDB file.
- **A real reverse-ETL platform.** Census or Hightouch for field mapping, scheduling, diffing (only sync changed rows), retries, and sync monitoring across many destinations. The hand-rolled script is fine for one destination; it becomes a maintenance burden at a dozen.
- **Change detection on the write side.** Sync only rows whose state changed since the last successful sync, and record sync state in the warehouse rather than inferring it from comments.
- **Observability.** Row-count and freshness monitors, volume anomaly alerts, dbt artifacts shipped to a metadata store, and alerting on sync error rates.
- **Contracts and CI against real data.** dbt model contracts on the marts that downstream tools depend on, and CI against a masked production sample in addition to the synthetic fixture.
- **Secrets and auth.** A GitHub App with installation tokens instead of personal tokens, with secrets in a manager.

## Repo layout

```
config/pipeline.yml          source repos, window, reverse-ETL settings
pipeline_common/             config loading, GitHub client, ClickUp client
extract/                     API → raw JSON → Parquet
load/                        Parquet → DuckDB raw.*
dbt_project/                 staging / intermediate / marts, macros, singular tests
reverse_etl/                 sync_stale_issues.py
scripts/                     synthetic fixture generator, lineage renderer
tests/                       Python unit tests
.github/workflows/ci.yml     lint + unit tests + fixture dbt build
```
