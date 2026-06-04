"""
OpenAI Ads Manager -> BigQuery daily reports pipeline.

Pulls daily-granularity insights from the OpenAI Advertiser API and loads
them into BigQuery as three reports, idempotently, over a configurable
lookback window. Runs as a Cloud Run Service triggered daily by Cloud
Scheduler (HTTP).

Reports
-------
Report 1  openai_ads_landing_page_report   (ad level)
    Day, Campaign tracking ID, Ad group tracking ID, Landing Page URL, Cost
Report 2  openai_ads_campaign_report        (campaign level)
    Day, Campaign tracking ID, Campaign name, Cost
Report 3  openai_ads_geo_report             (ad group level)
    Day, Campaign tracking ID, Ad group tracking ID, Country, Cost
    (Country requested from the API; currently returns empty for this
     account, so the column may be null until OpenAI populates it. Country
     is derived downstream in a BigQuery view via the AFID dim table.)

Environment variables
----------------------
OPENAI_ADS_API_KEY   (required)  Bearer token issued in Ads Manager > Settings.
GCP_PROJECT          (required)  BigQuery project id.
BQ_DATASET           (required)  BigQuery dataset (must already exist).
BQ_LOCATION          (optional)  Dataset location, default "US".
LOOKBACK_DAYS        (optional)  How many days back to refresh, default 7.
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
API_BASE_URL = os.environ.get("API_BASE_URL", "https://api.ads.openai.com/v1").rstrip("/")

PAGE_LIMIT = 1000          # rows per page (API max is 10000)
MAX_RETRIES = 6            # retry attempts for 429 / 5xx
REQUEST_TIMEOUT = 60       # seconds

# Each report is one API pull at a given aggregation level, projecting a
# specific set of fields, written to one BigQuery table.
#
# "api_fields"   -> exactly what we request from the API (validated names).
# "columns"      -> ordered (BigQuery column label, BigQuery type, source key
#                   in the API response row) tuples. The column labels match
#                   the report spec exactly ("Day", "Cost", etc.).
#
# The API normalizes dotted request names (ad.spend) to flat response keys
# (spend), so the source keys below are the flat keys seen in real responses.
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


def transform_rows(raw_rows, report_cfg):
    """Map raw API rows into BigQuery-ready dicts keyed by report column label."""
    loaded_at = datetime.now(tz=timezone.utc).isoformat()
    out = []
    for row in raw_rows:
        # Make the derived report date available under a synthetic source key.
        row = dict(row)
        row["_report_date"] = _report_date(row)

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
    log.info("  ensured table %s", table_id)


def replace_window(client, table_id, since, until):
    """Delete rows in the lookback window so each load is idempotent."""
    query = f"""
        DELETE FROM `{table_id}`
        WHERE `Day` BETWEEN @since AND @until
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

    until = date.today() - timedelta(days=1)           # through yesterday
    since = until - timedelta(days=LOOKBACK_DAYS - 1)   # inclusive window
    log.info("Pulling OpenAI Ads reports from %s to %s", since, until)

    session = _session()
    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)

    total = 0
    for name, cfg in REPORTS.items():
        log.info("Report: %s", name)
        raw = fetch_report_rows(
            session, cfg["aggregation_level"], cfg["api_fields"], since, until
        )
        rows = transform_rows(raw, cfg)

        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{cfg['table']}"
        schema = build_schema(cfg)

        ensure_table(client, table_id, schema)
        replace_window(client, table_id, since, until)
        load_rows(client, table_id, rows, schema)
        total += len(rows)

    log.info("Done. Loaded %s rows across %s reports.", total, len(REPORTS))
    return total


# --------------------------------------------------------------------------- #
# Web service wrapper (Cloud Run Service)
# --------------------------------------------------------------------------- #

from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def trigger():
    try:
        total = run()
        return jsonify({"status": "ok", "rows_loaded": total}), 200
    except Exception as exc:
        log.exception("Pipeline failed")
        return jsonify({"status": "error", "detail": str(exc)}), 500


@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
