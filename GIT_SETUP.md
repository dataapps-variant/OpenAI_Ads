# Git & CI/CD setup

The repo is already initialized with an initial commit. Two paths below:
**(A)** just host the code on GitHub, or **(B)** also auto-deploy to Cloud Run
on every push.

## A. Push to GitHub

Create an empty repo on GitHub (no README/license, to avoid conflicts), then:

```bash
git remote add origin git@github.com:<org-or-user>/openai-ads-bq.git
git branch -M main
git push -u origin main
```

That's enough if you'll keep deploying manually with the `gcloud` commands in
`README.md`.

## B. Auto-deploy on push (GitHub Actions)

`.github/workflows/deploy.yml` rebuilds the image and updates the Cloud Run Job
whenever you push to `main`. It authenticates with **Workload Identity
Federation** — no long-lived service-account key is ever stored in GitHub.

### 1. Create a deploy service account

```bash
export PROJECT_ID="your-gcp-project"
gcloud iam service-accounts create gh-deployer --project "$PROJECT_ID"
export DEPLOY_SA="gh-deployer@${PROJECT_ID}.iam.gserviceaccount.com"

# Roles needed to build images and manage the Cloud Run Job
for ROLE in roles/run.admin roles/cloudbuild.builds.editor \
            roles/artifactregistry.writer roles/iam.serviceAccountUser \
            roles/storage.admin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${DEPLOY_SA}" --role="$ROLE"
done
```

### 2. Set up Workload Identity Federation

```bash
gcloud iam workload-identity-pools create github-pool \
  --location=global --project "$PROJECT_ID"

gcloud iam workload-identity-pools providers create-oidc github-provider \
  --location=global --workload-identity-pool=github-pool \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='<org-or-user>/openai-ads-bq'" \
  --project "$PROJECT_ID"

# Project number for the resource path
export PNUM=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

# Let the GitHub repo impersonate the deploy SA
gcloud iam service-accounts add-iam-policy-binding "$DEPLOY_SA" \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/${PNUM}/locations/global/workloadIdentityPools/github-pool/attribute.repository/<org-or-user>/openai-ads-bq" \
  --project "$PROJECT_ID"

# This is the value for the WIF_PROVIDER variable below:
echo "projects/${PNUM}/locations/global/workloadIdentityPools/github-pool/providers/github-provider"
```

### 3. Add repository variables in GitHub

Settings → **Secrets and variables → Actions → Variables** tab:

| Variable | Example |
| --- | --- |
| `GCP_PROJECT_ID` | `your-gcp-project` |
| `GCP_REGION` | `us-central1` |
| `AR_REPO` | `ads-pipelines` |
| `JOB_NAME` | `openai-ads-bq` |
| `RUN_SA_EMAIL` | `openai-ads-bq-sa@your-gcp-project.iam.gserviceaccount.com` |
| `BQ_DATASET` | `marketing` |
| `BQ_LOCATION` | `US` |
| `DEPLOY_SA_EMAIL` | `gh-deployer@your-gcp-project.iam.gserviceaccount.com` |
| `WIF_PROVIDER` | *(the path printed above)* |

The API key still lives in **Secret Manager** (`openai-ads-api-key`), not in
GitHub — the workflow references it via `--set-secrets`. Run the Secret Manager
and one-time GCP setup steps from `README.md` first if you haven't.

### 4. Push and watch it deploy

```bash
git push -u origin main
```

Open the **Actions** tab to watch the build + deploy. The daily Cloud Scheduler
trigger (see `README.md` step 5) is set up once and keeps invoking the job
regardless of deploys.

## Notes

- **Nothing secret is committed.** `.gitignore` excludes `.env`, `*-key.json`,
  and similar. Double-check with `git status` before pushing if you ever add
  local config files.
- **Branch protection.** Since a push to `main` ships to production, consider
  requiring PRs and using `workflow_dispatch` for manual control.
