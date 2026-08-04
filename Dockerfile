FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Bake non-secret config into the image so auto-deploys from Git never lose it.
# (The API key is NOT here — it stays in Secret Manager, attached on the service.)
ENV GCP_PROJECT=variant-finance-data-project \
    BQ_DATASET=OpenAI_Ads \
    BQ_LOCATION=US \
    LOOKBACK_DAYS=7 \
    PORT=8080

# Cloud Run Services listen on $PORT. gunicorn serves the Flask app; the long
# timeout lets the daily ETL finish within one request.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 4 --timeout 1800 main:app
