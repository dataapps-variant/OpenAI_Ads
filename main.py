"""
OpenAI Ads Manager -> BigQuery daily insights pipeline.

Pulls daily-granularity insights at four aggregation levels
(ad_account, campaign, ad_group, ad) from the OpenAI Advertiser API
and loads them into BigQuery, idempotently, over a configurable
lookback window.

Designed to run as a Cloud Run Job triggered daily by Cloud Scheduler.

Environment variables
----------------------
OPENAI_ADS_API_KEY   (required)  Bearer token issued in Ads Manager > Settings.
GCP_PROJECT          (required)  BigQuery project id.
BQ_DATASET           (required)  BigQuery dataset (must already exist).
BQ_LOCATION          (optional)  Dataset location, default "US".
LOOKBACK_DAYS        (optional)  How many days back to refresh, default 7.
TABLE_PREFIX         (optional)  Table name prefix, default "openai_ads".
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("openai_ads_bq")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

API_KEY = os.environ.get("OPENAI_ADS_API_KEY")
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
TABLE_PREFIX = os.environ.get("TABLE_PREFIX", "openai_ads")
API_BASE_URL = os.environ.get("API_BASE_URL", "https://api.ads.openai.com/v1").rstrip("/")

PAGE_LIMIT = 1000          # rows per page (API max is 10000)
MAX_RETRIES = 6            # retry attempts for 429 / 5xx
REQUEST_TIMEOUT = 60       # seconds

# Metric fields shared by every aggregation level.
METRIC_FIELDS = ["impressions", "clicks", "spend", "ctr", "cpc", "cpm"]

# Per-level field projection. "readable_time" is the daily bucket label.
# Each level adds the id/name metadata appropriate to its scope.
LEVEL_CONFIG = {
    "ad_account": {
        "table": f"{TABLE_PREFIX}_account_insights",
        "id_fields": [],
    },
    "campaign": {
        "table": f"{TABLE_PREFIX}_campaign_insights",
        "id_fields": ["campaign_id", "campaign_name"],
    },
    "ad_group": {
        "table": f"{TABLE_PREFIX}_adgroup_insights",
        "id_fields": ["campaign_id", "campaign_name", "ad_group_id", "ad_group_name"],
    },
    "ad": {
        "table": f"{TABLE_PREFIX}_ad_insights",
        "id_fields": [
            "campaign_id", "campaign_name",
            "ad_group_id", "ad_group_name",
            "ad_id", "ad_name",
        ],
    },
}


def _require(name, value):
    if not value:
        log.error("Missing required environment variable: %s", name)
        sys.exit(2)


# --------------------------------------------------------------------------- #
# OpenAI Ads API client
# --------------------------------------------------------------------------- #

def _session():
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {API_KEY}",
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
            # Respect a reset header if present, else exponential backoff.
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

        # Non-retryable error.
        raise RuntimeError(
            f"OpenAI Ads API error {resp.status_code}: {resp.text[:500]}"
        )

    raise RuntimeError(f"Exhausted retries calling {url}")


def fetch_insights(session, aggregation_level, fields, since, until):
    """
    Pull all daily insight rows for one aggregation level over [since, until].

    Uses the account-level endpoint with an aggregation_level breakdown so a
    single endpoint covers every scope. Handles cursor pagination.
    """
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
        for f in fields:
            params.append(("fields[]", f))
        if after:
            params.append(("after", after))

        payload = _get_with_retries(session, url, params)
        data = payload.get("data", [])
        rows.extend(data)
        log.info(
            "  [%s] page %s -> %s rows (running total %s)",
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

def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    f = _to_float(value)
    return int(f) if f is not None else None


def _report_date(row):
    """Derive the DATE for the daily bucket from readable_time or start_time."""
    rt = row.get("readable_time")
    if rt:
        # readable_time is typically an ISO date or datetime string.
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(rt[: len(fmt) + 2], fmt).date().isoformat()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(rt.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass
    st = row.get("start_time")
    if st is not None:
        try:
            return datetime.fromtimestamp(int(st), tz=timezone.utc).date().isoformat()
        except (TypeError, ValueError, OSError):
            pass
    return None


def transform_rows(raw_rows, level_cfg):
    """Map raw API rows into BigQuery-ready dicts matching the table schema."""
    loaded_at = datetime.now(tz=timezone.utc).isoformat()
    out = []
    for row in raw_rows:
        record = {
            "report_date": _report_date(row),
            "row_id": row.get("id"),
            "start_time": _to_int(row.get("start_time")),
            "end_time": _to_int(row.get("end_time")),
            "timezone": row.get("timezone"),
            "impressions": _to_int(row.get("impressions")),
            "clicks": _to_int(row.get("clicks")),
            "spend": _to_float(row.get("spend")),
            "ctr": _to_float(row.get("ctr")),
            "cpc": _to_float(row.get("cpc")),
            "cpm": _to_float(row.get("cpm")),
            "_loaded_at": loaded_at,
        }
        for f in level_cfg["id_fields"]:
            record[f] = row.get(f)
        out.append(record)
    return out


# --------------------------------------------------------------------------- #
# BigQuery
# --------------------------------------------------------------------------- #

def build_schema(level_cfg):
    schema = [
        bigquery.SchemaField("report_date", "DATE"),
        bigquery.SchemaField("row_id", "STRING"),
        bigquery.SchemaField("start_time", "INTEGER"),
        bigquery.SchemaField("end_time", "INTEGER"),
        bigquery.SchemaField("timezone", "STRING"),
    ]
    for f in level_cfg["id_fields"]:
        schema.append(bigquery.SchemaField(f, "STRING"))
    schema.extend(
        [
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
        ]
    )
    return schema


def ensure_table(client, table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="report_date",
    )
    client.create_table(table, exists_ok=True)
    log.info("  ensured table %s", table_id)


def replace_window(client, table_id, since, until):
    """Delete existing rows in the lookback window so the load is idempotent."""
    query = f"""
        DELETE FROM `{table_id}`
        WHERE report_date BETWEEN @since AND @until
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("since", "DATE", since.isoformat()),
                bigquery.ScalarQueryParameter("until", "DATE", until.isoformat()),
            ]
        ),
    )
    job.result()
    log.info("  cleared %s rows in window from %s", job.num_dml_affected_rows, table_id)


def load_rows(client, table_id, rows, schema):
    if not rows:
        log.info("  no rows to load into %s", table_id)
        return
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    log.info("  loaded %s rows into %s", len(rows), table_id)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def run():
    _require("OPENAI_ADS_API_KEY", API_KEY)
    _require("GCP_PROJECT", GCP_PROJECT)
    _require("BQ_DATASET", BQ_DATASET)

    until = date.today() - timedelta(days=1)          # through yesterday (complete day)
    since = until - timedelta(days=LOOKBACK_DAYS - 1)  # inclusive lookback window
    log.info("Pulling OpenAI Ads insights from %s to %s", since, until)

    session = _session()
    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)

    total = 0
    for level, cfg in LEVEL_CONFIG.items():
        log.info("Level: %s", level)
        fields = ["readable_time", "timezone"] + cfg["id_fields"] + METRIC_FIELDS

        raw = fetch_insights(session, level, fields, since, until)
        rows = transform_rows(raw, cfg)

        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{cfg['table']}"
        schema = build_schema(cfg)

        ensure_table(client, table_id, schema)
        replace_window(client, table_id, since, until)
        load_rows(client, table_id, rows, schema)
        total += len(rows)

    log.info("Done. Loaded %s rows across %s levels.", total, len(LEVEL_CONFIG))
    return total


# --------------------------------------------------------------------------- #
# Web service wrapper (Cloud Run Service)
# --------------------------------------------------------------------------- #
# Cloud Run Services must listen for HTTP requests. Cloud Scheduler calls this
# endpoint daily to trigger the pipeline. The ETL logic above is unchanged.

from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def trigger():
    """Run the pipeline when invoked, and report how many rows were loaded."""
    try:
        total = run()
        return jsonify({"status": "ok", "rows_loaded": total}), 200
    except Exception as exc:  # surface failures to the caller / logs
        log.exception("Pipeline failed")
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200


if __name__ == "__main__":
    # Cloud Run provides the port via the PORT env var (defaults to 8080).
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
