# OpenAI Ads → BigQuery (daily, all levels)

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

## 5. Schedule it daily

Cloud Scheduler invokes the job through the Cloud Run Admin API.

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

This runs at 09:00 ET daily and pulls through *yesterday* (the last complete
day). Adjust `--schedule` / `--time-zone` as needed.

## Configuration reference

| Env var | Required | Default | Purpose |
| --- | --- | --- | --- |
| `OPENAI_ADS_API_KEY` | yes | — | Bearer token from Ads Manager |
| `GCP_PROJECT` | yes | — | BigQuery project id |
| `BQ_DATASET` | yes | — | Target dataset (must exist) |
| `BQ_LOCATION` | no | `US` | Dataset location |
| `LOOKBACK_DAYS` | no | `7` | Days re-pulled each run |
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
