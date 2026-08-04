"""
OpenAI Ads Manager -> BigQuery daily reports pipeline (multi-account).

Pulls daily-granularity insights from the OpenAI Advertiser API for one or more
ad accounts and loads them into BigQuery as three reports, idempotently, over a
configurable lookback window. Runs as a Cloud Run Service triggered hourly by
Cloud Scheduler (HTTP).

Refresh model
-------------
The pipeline records every successful run in a small bookkeeping table
(openai_ads_sync_state), one row per account per report, holding the last day
it loaded. Comparing that marker against today sizes the next window:

  marker is missing, or older than today — the day's FIRST run
      -> DAILY_LOOKBACK_DAYS (default 7), a deep sweep that re-pulls the
         past week so late restatements get corrected
  marker already says today — every later run that day
      -> LOOKBACK_DAYS (default 1), today only

So each day opens with one wide run and then costs a single day per hour for
the remaining 23.

The marker exists because nothing in the report data can answer "has a run
covered today yet?". Rows being present only means something loaded them once,
and an account with paused campaigns returns no rows at all however often we
run — so a MAX(Day) over the report tables would freeze in place. The marker
advances on every successful run regardless.

An outage is self-correcting: come back after days down and the next run is a
first-of-day run, so it sweeps a week unprompted. Since no window is ever
derived from how stale the marker is, downtime cannot widen a run beyond
DAILY_LOOKBACK_DAYS. Gaps older than that are a ?backfill=true job.

Whatever the window is, the load DELETEs that range for that account before
appending the fresh rows, so re-running never duplicates — it replaces. Today's
figures are partial by definition and firm up over the day; days outside the
window are never touched.

Multi-account model
-------------------
Accounts are defined entirely in ONE Secret Manager secret as JSON, so adding,
removing, renaming, or rotating an account needs NO code change and NO redeploy
— just a new secret version.

    OPENAI_ADS_ACCOUNTS = [
      {"name": "Variant Group LLC", "api_key": "sk-svc-...", "start_date": "2026-05-01"},
      {"name": "Job Flow LLC",      "api_key": "sk-svc-...", "start_date": "2026-05-01"}
    ]

  - "name"       (required)  Stamped onto every row this key produces. Must be
                             unique across accounts (it is the per-account dedup
                             key in BigQuery).
  - "api_key"    (required)  Bearer token created INSIDE that account in Ads
                             Manager > Settings > API Keys. A key only ever sees
                             the account it was minted under.
  - "start_date" (optional)  Floor date for this account's backfill, YYYY-MM-DD.
                             Falls back to the global START_DATE.

Reports (all accounts share these three tables; rows differ by "Account name")
-----------------------------------------------------------------------------
Report 1  openai_ads_landing_page_report   (ad level)
Report 2  openai_ads_campaign_report        (campaign level)
Report 3  openai_ads_geo_report             (ad group level)

Operations (HTTP query params on the trigger endpoint)
------------------------------------------------------
  (default)                     Daily rolling-lookback run for ALL accounts.
  ?account=<name>               Restrict any run to a single account by name.
  ?backfill=true                Full historical pull (start_date -> yesterday).
                                Combine with ?account=<name> for one account.
  ?migrate=true&legacy_account=<name>
                                ONE-TIME: add the "Account name" column to the
                                existing tables and label all pre-existing
                                (NULL) rows as <name>. No API pull. Run this
                                once, before the first normal run, so legacy
                                single-account data gets its name.

Environment variables
----------------------
OPENAI_ADS_ACCOUNTS  (required)  JSON array of accounts (see above).
GCP_PROJECT          (required)  BigQuery project id.
BQ_DATASET           (required)  BigQuery dataset (must already exist).
BQ_LOCATION          (optional)  Dataset location, default "US".
DAILY_LOOKBACK_DAYS  (optional)  Window for the day's first run, default 7.
LOOKBACK_DAYS        (optional)  Window for the rest of the day, default 1.
START_DATE           (optional)  Global floor date, default "2026-05-01".
API_BASE_URL         (optional)  Default "https://api.ads.openai.com/v1".
"""

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("openai_ads_bq")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

ACCOUNTS_JSON = os.environ.get("OPENAI_ADS_ACCOUNTS")
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
# Width of the day's FIRST run — the deep sweep that re-pulls a week so late
# restatements of recent days get corrected. Also used for the very first run
# the pipeline ever performs.
DAILY_LOOKBACK_DAYS = int(os.environ.get("DAILY_LOOKBACK_DAYS", "7"))
# Width of every OTHER run in the day. 1 = today only, which is all the hourly
# cadence needs: the deep sweep already settled everything older this morning.
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "1"))

# Bookkeeping table: one row per (account, report) recording the last window
# this pipeline successfully loaded. It is what distinguishes "first ever run"
# from "hourly run #400", which nothing in the data itself can tell us — a
# report table having rows only means SOME process loaded them, and an account
# with no spend produces no rows at all no matter how often we run.
SYNC_STATE_TABLE = "openai_ads_sync_state"
# Global floor date: the pipeline never pulls earlier than this unless an
# account overrides it with its own "start_date". Format YYYY-MM-DD.
START_DATE = os.environ.get("START_DATE", "2026-05-01")
API_BASE_URL = os.environ.get("API_BASE_URL", "https://api.ads.openai.com/v1").rstrip("/")

PAGE_LIMIT = 1000          # rows per page (API max is 10000)
MAX_RETRIES = 6            # retry attempts for 429 / 5xx
REQUEST_TIMEOUT = 60       # seconds

# Substring of the 400 the API returns when the range ends past its own clock.
# See resolve_until for why we discover the usable end date instead of assuming.
FUTURE_END_MARKER = "cannot be in the future"

# Synthetic source key used to stamp the account name onto every row.
ACCOUNT_NAME_KEY = "_account_name"
ACCOUNT_NAME_COL = "Account name"

# Each report is one API pull at a given aggregation level, projecting a
# specific set of fields, written to one BigQuery table.
#
# "Account name" is sourced from config (ACCOUNT_NAME_KEY), NOT from the API —
# we know which account each key belongs to, so the label is deterministic and
# never depends on the API populating an account-name field.
REPORTS = {
    "landing_page": {
        "table": "openai_ads_landing_page_report",
        "aggregation_level": "ad",
        "api_fields": [
            "metadata.readable_time",
            "campaign.id",
            "ad_group.id",
            "ad.link",
            "ad.spend",
        ],
        "columns": [
            ("Day", "DATE", "_report_date"),
            (ACCOUNT_NAME_COL, "STRING", ACCOUNT_NAME_KEY),
            ("Campaign tracking ID", "STRING", "campaign_id"),
            ("Ad group tracking ID", "STRING", "ad_group_id"),
            ("Landing Page URL", "STRING", "ad_link"),
            ("Cost", "NUMERIC", "spend"),
        ],
    },
    "campaign": {
        "table": "openai_ads_campaign_report",
        "aggregation_level": "campaign",
        "api_fields": [
            "metadata.readable_time",
            "campaign.id",
            "campaign.name",
            "campaign.spend",
        ],
        "columns": [
            ("Day", "DATE", "_report_date"),
            (ACCOUNT_NAME_COL, "STRING", ACCOUNT_NAME_KEY),
            ("Campaign tracking ID", "STRING", "campaign_id"),
            ("Campaign name", "STRING", "campaign_name"),
            ("Cost", "NUMERIC", "spend"),
        ],
    },
    "geo": {
        "table": "openai_ads_geo_report",
        "aggregation_level": "ad_group",
        "api_fields": [
            "metadata.readable_time",
            "campaign.id",
            "ad_group.id",
            "country",
            "ad_group.spend",
        ],
        "columns": [
            ("Day", "DATE", "_report_date"),
            (ACCOUNT_NAME_COL, "STRING", ACCOUNT_NAME_KEY),
            ("Campaign tracking ID", "STRING", "campaign_id"),
            ("Ad group tracking ID", "STRING", "ad_group_id"),
            ("Country", "STRING", "country"),
            ("Cost", "NUMERIC", "spend"),
        ],
    },
}


def _require(name, value):
    if not value:
        log.error("Missing required environment variable: %s", name)
        sys.exit(2)


def load_accounts(only=None):
    """Parse and validate the OPENAI_ADS_ACCOUNTS JSON secret.

    `only` (optional) restricts the result to the named account.
    Returns a list of {"name", "api_key", "start_date"(optional)} dicts.
    """
    _require("OPENAI_ADS_ACCOUNTS", ACCOUNTS_JSON)
    try:
        accounts = json.loads(ACCOUNTS_JSON)
    except json.JSONDecodeError as exc:
        log.error("OPENAI_ADS_ACCOUNTS is not valid JSON: %s", exc)
        sys.exit(2)

    if not isinstance(accounts, list) or not accounts:
        log.error("OPENAI_ADS_ACCOUNTS must be a non-empty JSON array.")
        sys.exit(2)

    seen = set()
    cleaned = []
    for i, acct in enumerate(accounts):
        name = (acct or {}).get("name")
        key = (acct or {}).get("api_key")
        if not name or not key:
            log.error("Account #%s is missing 'name' or 'api_key'.", i)
            sys.exit(2)
        if name in seen:
            log.error("Duplicate account name %r — names must be unique.", name)
            sys.exit(2)
        seen.add(name)
        cleaned.append(acct)

    if only:
        cleaned = [a for a in cleaned if a["name"] == only]
        if not cleaned:
            raise ValueError(f"No account named {only!r} in OPENAI_ADS_ACCOUNTS.")
    return cleaned


# --------------------------------------------------------------------------- #
# OpenAI Ads API client
# --------------------------------------------------------------------------- #

def _session(api_key):
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }
    )
    return s


def _get_with_retries(session, url, params):
    """GET with exponential backoff for 429 and transient 5xx errors."""
    backoff = 2.0
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429 or resp.status_code >= 500:
            reset = resp.headers.get("x-ratelimit-reset-requests")
            try:
                wait = float(reset) if reset else backoff
            except ValueError:
                wait = backoff
            wait = min(wait, 120.0)
            log.warning(
                "HTTP %s on attempt %s/%s. Sleeping %.1fs. Body: %s",
                resp.status_code, attempt, MAX_RETRIES, wait, resp.text[:300],
            )
            time.sleep(wait)
            backoff = min(backoff * 2, 120.0)
            continue

        raise RuntimeError(
            f"OpenAI Ads API error {resp.status_code}: {resp.text[:500]}"
        )

    raise RuntimeError(f"Exhausted retries calling {url}")


def resolve_until(session, start_guess, max_back=3):
    """Latest date the API will accept as a range end.

    The API rejects a range ending "in the future", measured against its own
    clock, which does not track UTC — so date.today() on Cloud Run is a day
    ahead of it for part of every day and the call 400s:

        400: time_ranges.end cannot be in the future.

    Rather than hardcode an offset we cannot see from here, ask: probe with a
    single-row request and step back a day until one is accepted. That lands on
    the newest day the API will actually serve, at any hour, and keeps working
    if OpenAI ever changes the timezone it reports in.

    Everything downstream uses the answer — the window decision, the delete,
    and the marker — so all three agree on what "today" means. Comparing the
    marker against a UTC date the API refuses to serve would make every run
    look like the day's first and sweep 7 days, 24 times a day.
    """
    url = f"{API_BASE_URL}/ad_account/insights"
    cfg = REPORTS["campaign"]          # cheapest report to probe with
    day = start_guess

    for _ in range(max_back + 1):
        params = [
            ("time_granularity", "daily"),
            ("aggregation_level", cfg["aggregation_level"]),
            ("limit", "1"),
            ("time_ranges[]", json.dumps(
                {"type": "date_range", "since": day.isoformat(), "until": day.isoformat()}
            )),
        ]
        for f in cfg["api_fields"]:
            params.append(("fields[]", f))

        try:
            _get_with_retries(session, url, params)
            return day
        except RuntimeError as exc:
            if FUTURE_END_MARKER not in str(exc):
                raise
            log.info("    API calls %s a future date; trying %s",
                     day, day - timedelta(days=1))
            day -= timedelta(days=1)

    raise RuntimeError(
        f"API rejected every end date from {start_guess} back {max_back} days"
    )


def fetch_report_rows(session, aggregation_level, api_fields, since, until):
    """Pull all daily rows for one report over [since, until] with pagination."""
    url = f"{API_BASE_URL}/ad_account/insights"
    time_range = json.dumps(
        {"type": "date_range", "since": since.isoformat(), "until": until.isoformat()}
    )

    rows = []
    after = None
    page = 0

    while True:
        page += 1
        params = [
            ("time_granularity", "daily"),
            ("aggregation_level", aggregation_level),
            ("limit", str(PAGE_LIMIT)),
            ("time_ranges[]", time_range),
        ]
        for f in api_fields:
            params.append(("fields[]", f))
        if after:
            params.append(("after", after))

        payload = _get_with_retries(session, url, params)
        data = payload.get("data", [])
        rows.extend(data)
        log.info(
            "    [%s] page %s -> %s rows (running total %s)",
            aggregation_level, page, len(data), len(rows),
        )

        if payload.get("has_more") and payload.get("last_id"):
            after = payload["last_id"]
        else:
            break

    return rows


# --------------------------------------------------------------------------- #
# Transform
# --------------------------------------------------------------------------- #

def _to_numeric(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _report_date(row):
    """Derive a DATE string from readable_time or start_time."""
    rt = row.get("readable_time")
    if rt:
        try:
            return datetime.fromisoformat(rt.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            try:
                return datetime.strptime(rt[:10], "%Y-%m-%d").date().isoformat()
            except ValueError:
                pass
    st = row.get("start_time")
    if st is not None:
        try:
            return datetime.fromtimestamp(int(st), tz=timezone.utc).date().isoformat()
        except (TypeError, ValueError, OSError):
            pass
    return None


def transform_rows(raw_rows, report_cfg, account_name):
    """Map raw API rows into BigQuery-ready dicts, stamping the account name."""
    loaded_at = datetime.now(tz=timezone.utc).isoformat()
    out = []
    for row in raw_rows:
        # Make derived/synthetic keys available before column projection.
        row = dict(row)
        row["_report_date"] = _report_date(row)
        row[ACCOUNT_NAME_KEY] = account_name

        record = {}
        for label, col_type, source_key in report_cfg["columns"]:
            value = row.get(source_key)
            if col_type == "NUMERIC":
                value = _to_numeric(value)
            record[label] = value
        record["_loaded_at"] = loaded_at
        out.append(record)
    return out


# --------------------------------------------------------------------------- #
# BigQuery
# --------------------------------------------------------------------------- #

def build_schema(report_cfg):
    schema = []
    for label, col_type, _ in report_cfg["columns"]:
        schema.append(bigquery.SchemaField(label, col_type))
    schema.append(bigquery.SchemaField("_loaded_at", "TIMESTAMP"))
    return schema


def ensure_table(client, table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    # Partition by the report date column ("Day").
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="Day",
    )
    client.create_table(table, exists_ok=True)
    log.info("    ensured table %s", table_id)


def ensure_account_column(client, table_id):
    """Add the 'Account name' column to a pre-existing table if missing.

    create_table(exists_ok=True) does NOT alter existing tables, so legacy
    single-account tables won't gain the column on their own. This is a no-op
    once the column exists. Required before replace_window, which references
    the column in its WHERE clause.
    """
    query = (
        f"ALTER TABLE `{table_id}` "
        f"ADD COLUMN IF NOT EXISTS `{ACCOUNT_NAME_COL}` STRING"
    )
    client.query(query).result()


def relabel_null_account(client, table_id, account_name):
    """One-time: label pre-existing (NULL-account) rows as account_name."""
    query = f"""
        UPDATE `{table_id}`
        SET `{ACCOUNT_NAME_COL}` = @name
        WHERE `{ACCOUNT_NAME_COL}` IS NULL
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", account_name),
            ]
        ),
    )
    job.result()
    return job.num_dml_affected_rows or 0


def ensure_state_table(client):
    """Create the bookkeeping table if it isn't there yet."""
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{SYNC_STATE_TABLE}"
    table = bigquery.Table(
        table_id,
        schema=[
            bigquery.SchemaField("account_name", "STRING"),
            bigquery.SchemaField("report", "STRING"),
            bigquery.SchemaField("last_until", "DATE"),
            bigquery.SchemaField("last_run_at", "TIMESTAMP"),
        ],
    )
    client.create_table(table, exists_ok=True)
    return table_id


def read_state(client, account_name, report):
    """Last day successfully loaded for this account/report, or None if never.

    None means the pipeline has genuinely never completed a run for this pair —
    which is the signal for the wide first refresh.
    """
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{SYNC_STATE_TABLE}"
    query = (
        f"SELECT last_until FROM `{table_id}` "
        f"WHERE account_name = @name AND report = @report"
    )
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", account_name),
                bigquery.ScalarQueryParameter("report", "STRING", report),
            ]
        ),
    )
    for row in job.result():
        return row["last_until"]
    return None


def write_state(client, account_name, report, until):
    """Record a successful load. Called only after the rows are safely in.

    Leaving it until after the load means a failed run doesn't advance the
    marker, so the next run retries the same window instead of skipping it.
    """
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{SYNC_STATE_TABLE}"
    query = f"""
        MERGE `{table_id}` T
        USING (
          SELECT @name AS account_name, @report AS report, @until AS last_until
        ) S
        ON T.account_name = S.account_name AND T.report = S.report
        WHEN MATCHED THEN UPDATE SET
          last_until = S.last_until, last_run_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
          (account_name, report, last_until, last_run_at)
          VALUES (S.account_name, S.report, S.last_until, CURRENT_TIMESTAMP())
    """
    client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", account_name),
                bigquery.ScalarQueryParameter("report", "STRING", report),
                bigquery.ScalarQueryParameter("until", "DATE", until.isoformat()),
            ]
        ),
    ).result()


def replace_window(client, table_id, since, until, account_name):
    """Delete this account's rows in the window so each load is idempotent.

    Scoped by account so one account's run never deletes another's data.
    """
    query = f"""
        DELETE FROM `{table_id}`
        WHERE `Day` BETWEEN @since AND @until
          AND `{ACCOUNT_NAME_COL}` = @name
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("since", "DATE", since.isoformat()),
                bigquery.ScalarQueryParameter("until", "DATE", until.isoformat()),
                bigquery.ScalarQueryParameter("name", "STRING", account_name),
            ]
        ),
    )
    job.result()
    log.info(
        "    cleared %s rows for %r in window from %s",
        job.num_dml_affected_rows, account_name, table_id,
    )


def load_rows(client, table_id, rows, schema):
    if not rows:
        log.info("    no rows to load into %s", table_id)
        return
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Belt-and-suspenders: tolerate the new column on first append even if
        # the explicit ALTER hasn't landed yet.
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    log.info("    loaded %s rows into %s", len(rows), table_id)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def _parse_date(value, label):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        log.error("Invalid %s %r; expected YYYY-MM-DD", label, value)
        sys.exit(2)


def _account_floor(account):
    """Per-account start_date if present, else the global START_DATE."""
    return _parse_date(account.get("start_date", START_DATE), "start_date")


def _window_start(client, account_name, report, floor, until, backfill):
    """Decide how far back this run reaches for one account/report.

    The day's FIRST run -> DAILY_LOOKBACK_DAYS (7). A deep sweep that re-pulls
        the past week, so anything the API restated since yesterday gets
        corrected. The very first run the pipeline ever performs takes this
        path too, since it has no marker either.

    Every run after that, same day -> LOOKBACK_DAYS (1), i.e. today only. This
        morning's sweep already settled the older days; there is nothing to
        gain from re-pulling them 23 more times.

    "First run of the day" is decided by comparing today against the marker,
    which records the last day a run completed. last < today means no run has
    covered today yet. The marker also makes an outage self-correcting: come
    back after three days down and the next run is a first-of-day run, so it
    sweeps a week and closes the gap without anyone asking.

    Nothing here derives a window from how STALE the marker is, so no amount of
    downtime or dormancy can widen a run beyond DAILY_LOOKBACK_DAYS. Gaps older
    than a week are a ?backfill=true job.
    """
    if backfill:
        return floor

    last = read_state(client, account_name, report)

    if last is None:
        log.info("    first run ever -> %s day sweep", DAILY_LOOKBACK_DAYS)
        start = until - timedelta(days=DAILY_LOOKBACK_DAYS - 1)
    elif last < until:
        log.info("    first run of %s -> %s day sweep", until, DAILY_LOOKBACK_DAYS)
        start = until - timedelta(days=DAILY_LOOKBACK_DAYS - 1)
    else:
        start = until - timedelta(days=LOOKBACK_DAYS - 1)

    return max(floor, start)


def _run_account(client, account, backfill):
    """Run all three reports for a single account. Raises on failure."""
    name = account["name"]
    floor = _account_floor(account)
    session = _session(account["api_key"])

    # Through the CURRENT day, not yesterday: the job runs hourly, so today's
    # partial numbers are the whole point, and each run replaces its window
    # rather than adding to it so the partial day firms up as the day fills in.
    # Which day is "current" is the API's call, not ours — see resolve_until.
    until = resolve_until(session, date.today())

    log.info("  [%s] %s through %s", name, "BACKFILL" if backfill else "refresh", until)

    ensure_state_table(client)
    total = 0
    for report_name, cfg in REPORTS.items():
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{cfg['table']}"
        schema = build_schema(cfg)

        ensure_table(client, table_id, schema)
        ensure_account_column(client, table_id)

        # Tracked per (account, report), so a report added later takes its own
        # wide first refresh without dragging the others back through days they
        # already hold.
        since = _window_start(client, name, report_name, floor, until, backfill)
        if since > until:
            log.info("  [%s] %s: nothing to pull.", name, report_name)
            continue

        log.info("  [%s] report: %s (%s to %s)", name, report_name, since, until)
        raw = fetch_report_rows(
            session, cfg["aggregation_level"], cfg["api_fields"], since, until
        )
        rows = transform_rows(raw, cfg, name)

        # Never clear the window on an empty response. The load is a DELETE
        # followed by an INSERT, so if the API hands back nothing the delete
        # would wipe the window and the insert would put nothing back — a week
        # of history gone on the daily sweep, with no error to show for it.
        # Treat empty as "this run told us nothing" and leave the table alone.
        # Skipping write_state too means the next run retries this same window
        # instead of stepping over it.
        if not rows:
            log.warning(
                "  [%s] %s: API returned 0 rows for %s..%s — leaving existing "
                "data untouched and retrying next run.",
                name, report_name, since, until,
            )
            continue

        replace_window(client, table_id, since, until, name)
        load_rows(client, table_id, rows, schema)

        # Only now, with the rows committed, does this run count as done. A
        # crash anywhere above leaves the marker where it was, so the next run
        # covers the same window again rather than stepping over it.
        write_state(client, name, report_name, until)
        total += len(rows)

    log.info("  [%s] done: %s rows.", name, total)
    return total


def run(backfill=False, only=None):
    """Run every (selected) account independently; one failure won't block others."""
    _require("GCP_PROJECT", GCP_PROJECT)
    _require("BQ_DATASET", BQ_DATASET)

    accounts = load_accounts(only=only)
    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)

    results = {}
    grand_total = 0
    for account in accounts:
        name = account["name"]
        try:
            loaded = _run_account(client, account, backfill)
            results[name] = {"status": "ok", "rows_loaded": loaded}
            grand_total += loaded
        except Exception as exc:                        # isolate per account
            log.exception("Account %r failed", name)
            results[name] = {"status": "error", "detail": str(exc)}

    return {"rows_loaded": grand_total, "accounts": results}


def migrate(legacy_account):
    """One-time: add the Account-name column and label legacy NULL rows.

    Run ONCE after deploy, before the first normal run, so existing
    single-account data is attributed to `legacy_account`. Idempotent: the
    column add is a no-op once present, and the relabel only touches rows that
    are still NULL.
    """
    _require("GCP_PROJECT", GCP_PROJECT)
    _require("BQ_DATASET", BQ_DATASET)

    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)
    results = {}
    for report_name, cfg in REPORTS.items():
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{cfg['table']}"
        try:
            ensure_account_column(client, table_id)
            relabeled = relabel_null_account(client, table_id, legacy_account)
            results[cfg["table"]] = {"status": "ok", "relabeled_rows": relabeled}
            log.info("  migrated %s: relabeled %s rows -> %r",
                     table_id, relabeled, legacy_account)
        except NotFound:
            results[cfg["table"]] = {"status": "skipped", "detail": "table does not exist yet"}
            log.info("  %s does not exist yet — nothing to migrate.", table_id)
        except Exception as exc:
            log.exception("Migration failed for %s", table_id)
            results[cfg["table"]] = {"status": "error", "detail": str(exc)}
    return {"legacy_account": legacy_account, "tables": results}


# --------------------------------------------------------------------------- #
# Web service wrapper (Cloud Run Service)
# --------------------------------------------------------------------------- #

from flask import Flask, jsonify, request

app = Flask(__name__)


def _truthy(value):
    return str(value).lower() in ("1", "true", "yes")


@app.route("/", methods=["GET", "POST"])
def trigger():
    try:
        # One-time migration path: add column + relabel legacy rows. No pull.
        if _truthy(request.args.get("migrate", "")):
            legacy = request.args.get("legacy_account")
            if not legacy:
                return jsonify({
                    "status": "error",
                    "detail": "migrate=true requires legacy_account=<name>",
                }), 400
            return jsonify({"status": "ok", "migrate": migrate(legacy)}), 200

        # Normal / backfill run, optionally restricted to one account.
        backfill = _truthy(request.args.get("backfill", ""))
        only = request.args.get("account") or None
        summary = run(backfill=backfill, only=only)

        statuses = [a["status"] for a in summary["accounts"].values()]
        if statuses and all(s == "error" for s in statuses):
            overall, code = "error", 500
        elif any(s == "error" for s in statuses):
            overall, code = "partial", 207
        else:
            overall, code = "ok", 200

        return jsonify({"status": overall, "backfill": backfill, **summary}), code

    except Exception as exc:
        log.exception("Pipeline failed")
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
