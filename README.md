# OpenAI Ads → BigQuery (hourly refresh, all levels)

A small ETL pipeline that pulls **daily-granularity insights** from the OpenAI
Advertiser API at all four aggregation levels — **ad account, campaign, ad
group, and ad** — and loads them into BigQuery. It runs as a **Cloud Run Job**
triggered once a day by **Cloud Scheduler**.

## How it works

1. For each level, it calls `GET /ad_account/insights` with
   `aggregation_level` set to that level and `time_granularity=daily`.
2. It refreshes a rolling **lookback window** (default 7 days, so late
   restatements get corrected) and writes to one date-partitioned table per
   level:
   - `openai_ads_account_insights`
   - `openai_ads_campaign_insights`
   - `openai_ads_adgroup_insights`
   - `openai_ads_ad_insights`
3. Loads are **idempotent**: it deletes rows in the window, then appends fresh
   rows, so re-running never duplicates data.

## Prerequisites

- A GCP project with BigQuery enabled.
- An OpenAI Ads API key (Ads Manager → **Settings → API Keys → Create New
  Key**). The key is shown only once — copy it immediately. It is scoped to one
  ad account.
- `gcloud` CLI installed and authenticated.

Set some shell variables to reuse below:

```bash
export PROJECT_ID="your-gcp-project"
export REGION="us-central1"
export DATASET="marketing"          # BigQuery dataset to hold the tables
export REPO="ads-pipelines"         # Artifact Registry repo name
export JOB="openai-ads-bq"
export SA="openai-ads-bq-sa"
```

## 1. One-time GCP setup

```bash
# Enable APIs
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com \
  secretmanager.googleapis.com \
  --project "$PROJECT_ID"

# BigQuery dataset (skip if it already exists)
bq --location=US mk --dataset "${PROJECT_ID}:${DATASET}"

# Artifact Registry repo for the container image
gcloud artifacts repositories create "$REPO" \
  --repository-format=docker --location="$REGION" --project "$PROJECT_ID"

# Service account the job runs as
gcloud iam service-accounts create "$SA" --project "$PROJECT_ID"
export SA_EMAIL="${SA}@${PROJECT_ID}.iam.gserviceaccount.com"

# Let it write to BigQuery
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"
```

## 2. Store the API key in Secret Manager

Never bake the key into the image or env vars in plain text.

```bash
printf '%s' 'sk-svc-XXXXXXXX' | gcloud secrets create openai-ads-api-key \
  --data-file=- --project "$PROJECT_ID"

gcloud secrets add-iam-policy-binding openai-ads-api-key \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" \
  --project "$PROJECT_ID"
```

## 3. Build and push the image

```bash
export IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${JOB}:latest"
gcloud builds submit --tag "$IMAGE" --project "$PROJECT_ID"
```

## 4. Create the Cloud Run Job

```bash
gcloud run jobs create "$JOB" \
  --image "$IMAGE" \
  --region "$REGION" \
  --service-account "$SA_EMAIL" \
  --max-retries 1 \
  --task-timeout 1800 \
  --set-env-vars "GCP_PROJECT=${PROJECT_ID},BQ_DATASET=${DATASET},BQ_LOCATION=US,LOOKBACK_DAYS=7" \
  --set-secrets "OPENAI_ADS_API_KEY=openai-ads-api-key:latest" \
  --project "$PROJECT_ID"
```

Run it once manually to confirm it works:

```bash
gcloud run jobs execute "$JOB" --region "$REGION" --project "$PROJECT_ID"
```

Check the tables:

```bash
bq query --use_legacy_sql=false \
  "SELECT report_date, COUNT(*) rows
   FROM \`${PROJECT_ID}.${DATASET}.openai_ads_ad_insights\`
   GROUP BY report_date ORDER BY report_date DESC LIMIT 10"
```

## 5. Schedule it hourly

The live deployment is a Cloud Run **Service** (`openai-ads`, region `us-east1`)
that Cloud Scheduler triggers over plain HTTP against the service URL. Point the
schedule at the service and set an hourly cron:

```bash
gcloud scheduler jobs update http <SCHEDULER_JOB_NAME> \
  --location <SCHEDULER_LOCATION> \
  --schedule "0 * * * *" \
  --time-zone "America/New_York" \
  --project variant-finance-data-project
```

Each day's first run sweeps the last 7 days; the other 23 pull only today. Every
run replaces its window rather than appending to it, so hourly execution
corrects today's partial numbers in place and never duplicates rows. See
**Refresh model** below.

### Legacy: Cloud Run Job scheduling

The sections below describe the original Cloud Run **Job** setup, which the
service replaced. Kept for reference.

```bash
# Allow Scheduler's service account to invoke the job
gcloud run jobs add-iam-policy-binding "$JOB" \
  --region "$REGION" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker" \
  --project "$PROJECT_ID"

gcloud scheduler jobs create http openai-ads-bq-daily \
  --location "$REGION" \
  --schedule "0 9 * * *" \
  --time-zone "America/New_York" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB}:run" \
  --http-method POST \
  --oauth-service-account-email "$SA_EMAIL" \
  --project "$PROJECT_ID"
```

## Refresh model

### How much each run pulls

Each day opens with one wide sweep, then costs a single day per hour for the
remaining 23 runs:

| Situation | Window pulled |
| --- | --- |
| **The day's first run** (00:00) | last **7** days (`DAILY_LOOKBACK_DAYS`) |
| **Every later run that day** (01:00–23:00) | **today only** (`LOOKBACK_DAYS`) |
| The very first run ever | last **7** days — it has no marker either |
| Back after an outage | 7 days, since it counts as a first-of-day run |
| `?backfill=true` | account `start_date` → today |

So a normal day looks like:

```
00:00   7 days   <- sweep: re-pulls the week, correcting anything restated
01:00   today
02:00   today
 ...
23:00   today
00:00   7 days   <- next day's sweep, which settles yesterday for good
```

The sweep is what makes the numbers trustworthy. Ad platforms revise figures
after the fact — clicks flagged as invalid, billing corrections — and the
sweep re-pulls the last week every morning so those land automatically. The
hourly runs in between only chase today, which is the only day still moving.

### How it knows which run is the day's first

A small bookkeeping table, `openai_ads_sync_state`, holds one row per account
per report:

| account_name | report | last_until | last_run_at |
| --- | --- | --- | --- |
| Variant Group LLC | campaign | 2026-08-04 | 2026-08-04 14:00:03 UTC |

Compare `last_until` against today:

- `last_until` **missing or older than today** → no run has covered today yet →
  **7-day sweep**
- `last_until` **is today** → already swept this morning → **today only**

It has to be tracked explicitly, because the report data can't answer the
question. Rows being present only proves *something* loaded them once, and an
account with paused campaigns returns no rows at all no matter how often the
pipeline runs — so a `MAX(Day)` over the report tables would freeze in place.
The marker advances on every successful run either way.

The marker is written **after** the rows are committed, so a run that crashes
part-way leaves it untouched and the next run redoes that window rather than
stepping over it.

Because no window is ever sized from *how stale* the marker is, downtime can't
inflate a run — coming back after a month still sweeps 7 days, not 30. Gaps
older than a week need `?backfill=true`.

To force a sweep on the next run, delete the relevant rows:

```sql
DELETE FROM `variant-finance-data-project.OpenAI_Ads.openai_ads_sync_state`
WHERE account_name = 'Variant Group LLC'
```

### The two knobs

`DAILY_LOOKBACK_DAYS` (default `7`) — how far the day's first run reaches back.
Raise it if the API restates figures more than a week late.

`LOOKBACK_DAYS` (default `1`) — how far every later run that day reaches back.
`1` means today only. Raising it costs API traffic on all 23 runs, so prefer
widening the daily sweep instead.

Tracking is **per (account, report)**, so adding a fourth report later takes
its own sweep without dragging the existing three back through days they
already have.

### Why re-running never duplicates

Every run, for each account and each table, does two things in order:

1. `DELETE` the rows in the window belonging to **that account** (scoped by
   `Account name`, so one account never clears another's data).
2. `INSERT` the freshly pulled rows for that window.

The window is *replaced*, not appended to. Running once an hour rewrites today
with progressively better numbers instead of stacking 24 copies of it. Days
outside the window are never touched.

Two consequences worth knowing:

- **Today's row values are partial** and climb through the day. Anything
  downstream that needs only settled days should filter `Day < CURRENT_DATE()`.
- **The delete and the insert are separate statements.** For a few seconds each
  hour today's rows are absent from BigQuery. A dashboard querying at that
  instant sees a gap. If that matters, stage the load and swap it in inside a
  `BEGIN TRANSACTION` / `COMMIT` block.

## Configuration reference

| Env var | Required | Default | Purpose |
| --- | --- | --- | --- |
| `OPENAI_ADS_API_KEY` | yes | — | Bearer token from Ads Manager |
| `GCP_PROJECT` | yes | — | BigQuery project id |
| `BQ_DATASET` | yes | — | Target dataset (must exist) |
| `BQ_LOCATION` | no | `US` | Dataset location |
| `DAILY_LOOKBACK_DAYS` | no | `7` | Window for the day's first run (the sweep) |
| `LOOKBACK_DAYS` | no | `1` | Window for every later run that day |
| `TABLE_PREFIX` | no | `openai_ads` | Table name prefix |
| `API_BASE_URL` | no | `https://api.ads.openai.com/v1` | API base |

## Notes & gotchas

- **Single ad account per key.** Each Ads API key is scoped to one ad account.
  For multiple accounts, deploy one job (and one secret) per account, or
  contact OpenAI about multi-account access.
- **Prepaid credits gate.** The Ads API uses a prepaid credit model. If the
  balance hits zero, billable calls fail with `429`/quota errors that retries
  won't fix — top up credits.
- **Field names.** The projected `fields[]` follow OpenAI's documented insight
  fields (`impressions`, `clicks`, `spend`, `ctr`, `cpc`, `cpm`, plus
  `*_id`/`*_name` metadata). If the API returns a field under a slightly
  different name, adjust `LEVEL_CONFIG` / `METRIC_FIELDS` in `main.py`; the
  transform reads defensively with `.get()` so unknown fields are simply null.
- **Lookback window.** 7 days catches late-arriving restatements without
  reprocessing the full history. Increase `LOOKBACK_DAYS` for a wider safety
  net or set it large for an initial backfill.

## Local test

```bash
pip install -r requirements.txt
export OPENAI_ADS_API_KEY=sk-svc-...
export GCP_PROJECT=your-project
export BQ_DATASET=marketing
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
python main.py
```
