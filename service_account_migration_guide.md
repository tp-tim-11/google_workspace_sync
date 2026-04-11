# Service Account Credential Rotation — Migration Guide

> **Purpose:** Step-by-step guide for migrating the Google Sheets / Google Docs sync to a new service account.  
> **Audience:** Patrik a spol.
> **Last updated:** 11.04.2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Background — Google Cloud Platform Concepts](#2-background--google-cloud-platform-concepts)
3. [Setting Up a Google Cloud Account and Project](#3-setting-up-a-google-cloud-account-and-project)
4. [Prerequisites](#4-prerequisites)
5. [Step 1 — Create the New Service Account](#5-step-1--create-the-new-service-account)
6. [Step 2 — Grant Required Permissions](#6-step-2--grant-required-permissions)
7. [Step 3 — Generate and Secure the New Key](#7-step-3--generate-and-secure-the-new-key)
8. [Step 4 — Update Application Configuration](#8-step-4--update-application-configuration)
9. [Step 5 — Validate the New Credentials](#9-step-5--validate-the-new-credentials)
10. [Step 6 — Revoke the Old Service Account Key](#10-step-6--revoke-the-old-service-account-key)
11. [Rollback Procedure](#11-rollback-procedure)
12. [Troubleshooting](#12-troubleshooting)
13. [Checklist](#13-checklist)

---

## 1. Overview

This guide covers rotating the Google service account used by the sync process that reads data from **Google Sheets** and **Google Docs**.

**Why rotate?**
- Key compromise
- Team/ownership handover

---

## 2. Background — Google Cloud Platform Concepts

**Google Cloud Platform (GCP)** is Google's cloud infrastructure. For this sync service, GCP is used for one thing only: managing *service accounts* and enabling the Drive/Sheets APIs. The actual data (Sheets, Docs) lives in Google Workspace (Drive/Sheets), not in GCP.

**GCP project** — a container that groups cloud resources together. It has a human-readable name and a unique *project ID* (e.g. `my-team-sync-prod`). Every service account belongs to a project. APIs must be explicitly enabled per project.

**Service account** — a non-human Google identity used by applications instead of a personal Google account. It has an email address like `name@project-id.iam.gserviceaccount.com` and authenticates via a downloaded JSON key file. This is what the sync service uses to call the Drive and Sheets APIs without any user interaction.

**API key vs. service account** — this project uses a service account JSON key (not an API key). They are different things. The JSON file contains a private key and metadata that lets the application prove its identity to Google.

---

## 3. Setting Up a Google Cloud Account and Project

> Skip this section if a GCP project already exists for this service.

### 3.1 Create a Google Account

A GCP account is tied to a Google account (Gmail or Google Workspace). If you don't have one:

1. Go to [accounts.google.com/signup](https://accounts.google.com/signup) and create an account.

### 3.2 Create a GCP Account and Enable Billing

1. Go to [console.cloud.google.com](https://console.cloud.google.com).
2. Sign in with your Google account.
3. Accept the Terms of Service if prompted.

### 3.3 Create a GCP Project

1. In the top navigation bar, click the project selector dropdown (next to the Google Cloud logo).
2. Click **New Project**.
3. Fill in:
   - **Project name:** e.g. `sheets-docs-sync-prod` (human-readable label can be whatever you like)
   - **Project ID:** auto-generated or customized — this is the permanent unique identifier, e.g. `sheets-docs-sync-prod-123`. **Write this down.**
   - **Organization / Location:** select your Google Workspace org if applicable, otherwise leave as "No organization".
4. Click **Create**.
5. Wait a few seconds, then select the new project from the dropdown.

### 3.4 Enable the Required APIs

With the new project selected:

1. Go to **APIs & Services → Library** (left-side nav, or search for "API Library").
2. Search for **Google Drive API** → click it → click **Enable**.
3. Search for **Google Sheets API** → click it → click **Enable**.

Both APIs must be enabled before a service account can use them.

---

## 4. Prerequisites

Before starting, ensure you have:

- [ ] Access to **Google Cloud Console** for the relevant GCP project (`<!-- PROJECT_ID -->`)
- [ ] The `roles/iam.serviceAccountAdmin` or `roles/owner` IAM role in that project
- [ ] Editor/Owner access to all Google Sheets and Docs that the sync reads from
- [ ] Access to the environment where credentials are stored (e.g., secret manager, `.env` file, CI/CD secrets)
- [ ] Python environment set up with `google-auth` / `google-api-python-client` installed
- [ ] Ability to run the sync manually for validation

---

## 5. Step 1 — Create the New Service Account

> Skip this step if you are only rotating the **key** on the existing service account (see Step 3).

1. Go to [Google Cloud Console → IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts).
2. Select the project: `<!-- PROJECT_ID -->`.
3. Click **Create Service Account**.
4. Fill in the details:
   - **Name:** `<!-- e.g. sheets-docs-sync-prod -->`
   - **Description:** `Service account for Google Sheets/Docs sync`
5. Click **Create and Continue**.
6. Assign roles (see Step 2 before finishing).
7. Click **Done**.

Note the new service account email — it follows the format:
```
<name>@<project-id>.iam.gserviceaccount.com
```

---

## 6. Step 2 — Grant Required Permissions

The service account needs two levels of access:

### 6.1 GCP APIs

No GCP project-level IAM roles are required. Access is controlled entirely via resource-level sharing (Section 6.2).

You do need the following APIs **enabled** in your GCP project (APIs & Services → Enabled APIs):

- **Google Drive API**
- **Google Sheets API**

These are covered in Section 3.4 if you are setting up from scratch.

### 6.2 Google Drive Folder — grant Viewer (note to self change to editor if we implement save manual skill)

The sync reads from Drive using the `drive.readonly` scope, so **Viewer** is sufficient.

1. In Google Drive, right-click the target folder → **Share** → **Share**.
2. In the **"Add people and groups"** field at the top of the dialog, paste the service account email:
   `<name>@<project-id>.iam.gserviceaccount.com`
3. Press **Enter** — a row appears with a role dropdown on the right.
4. In that dropdown, select **Viewer**.
5. Uncheck **"Notify people"**.
6. Click **Share**.

> **Note on the dialog:** "Owner" only appears in the *General access* section at the bottom (for link-sharing settings). The **Viewer / Commenter / Editor** dropdown appears only after you've added a specific email in step 3 above.

### 6.3 Google Sheet — grant Editor

The sync writes back to the Sheet (`spreadsheets` scope, not read-only), so **Editor** is required.

1. Open the target Google Sheet.
2. Click **Share** in the top-right corner.
3. In the **"Add people and groups"** field, paste the service account email.
4. Press **Enter** — a row appears with a role dropdown.
5. Select **Editor**.
6. Uncheck **"Notify people"**.
7. Click **Share**.

> **Tip:** Share the Drive *folder* rather than individual files — files added to the folder later inherit access automatically.

---

## 7. Step 3 — Generate and Secure the New Key

1. In Google Cloud Console, navigate to the service account.
2. Klick on the **Actions** tab → **Manage Keys** → **Add Key** -> **Create New Key**.
3. Select **JSON** format and click **Create**.
4. The key file is downloaded automatically — **this is the only time it is available in plain text**.
5. Store it securely:

```bash
# Place the file in the credentials/ directory (already gitignored): (launch this from the google workspace dir or change the path)
mv ~/Downloads/<downloaded-key>.json .google_workspace_sync/credentials/google_service_account_api_key.json
```

> ⚠️ **Never commit the JSON key file to version control.** The `credentials/` directory is already listed in `.gitignore` — do not move the file outside it.

---

## 8. Step 4 — Update Application Configuration

The credential path is configured via `GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE` in the `.env` file at the project root (loaded by `google_workspace_sync/settings.py` via pydantic-settings).

### 8.1 Using a file path (standard setup)

Update `GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE` in `.env`:

```bash
# Before
GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE=./credentials/google_service_account_api_key.json

# After (if you renamed/replaced the file)
GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE=./credentials/google_service_account_api_key.json
```

The file name convention is `google_service_account_api_key.json` inside `credentials/`. If you are simply replacing the file contents in-place, no `.env` change is needed.

Or as a shell environment variable (overrides `.env`):

```bash
export GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE="./credentials/google_service_account_api_key.json"
```


## 9. Step 5 — Validate the New Credentials

Before decommissioning the old key, run a validation to confirm the new credentials work end-to-end.

### 9.1 Quick connectivity test

```python
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Must match google_clients.py exactly
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

credentials = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE"],
    scopes=SCOPES,
)

# Test Drive access (list files in target folder)
drive_service = build("drive", "v3", credentials=credentials)
result = drive_service.files().list(
    q=f"'{os.environ['GOOGLE_DRIVE_DOCUMENTS_FOLDER_ID']}' in parents",
    pageSize=1,
    fields="files(id, name)",
).execute()
print("Drive OK: folder accessible, sample file:", result.get("files", [{"name": "(empty)"}])[0]["name"])

# Test Sheets access
sheets_service = build("sheets", "v4", credentials=credentials)
sheet = sheets_service.spreadsheets().get(
    spreadsheetId=os.environ["GOOGLE_SHEETS_ID"]
).execute()
print("Sheets OK:", sheet["properties"]["title"])
```


## 10. Step 6 — Revoke the Old Service Account Key

Only do this **after** Step 5 passes completely.

1. Go to Google Cloud Console → IAM & Admin → Service Accounts.
2. Select the **old** service account.
3. Go to the **Keys** tab.
4. Find the old key (match by Key ID noted before the migration).
5. Click the three-dot menu → **Delete key**.
6. Confirm deletion.

> If you created an entirely new service account (not just rotating the key), also remove the old service account's access from all Sheets/Docs it was shared with.

---

## 11. Rollback Procedure

If the new credentials cause issues, roll back immediately:

1. Revert the configuration change (restore old key path / secret value).
2. Redeploy or restart the sync service.
3. Confirm the old credentials still work (they should, unless already revoked).
4. Investigate the failure before retrying the migration.

> ⚠️ Do **not** revoke the old key (Step 6) until you are confident the new credentials are fully operational.

---

## 12. Troubleshooting

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `google.auth.exceptions.DefaultCredentialsError` | Credential file not found or env var not set | Check `GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_FILE` path in `.env`; the file must exist at that path |
| `403 The caller does not have permission` | Service account not shared on the Sheet/Doc | Share the file with the service account email |
| `403 Request had insufficient authentication scopes` | Missing OAuth scope | Add the required scope to the credentials init |
| `400 Unable to parse range` | Wrong spreadsheet ID or range | Verify spreadsheet ID and named ranges |
| `404 Requested entity was not found` | Wrong document/spreadsheet ID | Double-check IDs in config |
| Key file downloaded but authentication fails | Wrong GCP project or key already deleted | Regenerate key from the correct service account |

---

## 13. Checklist

Use this as a sign-off checklist before closing the migration:

### Preparation
- [ ] New service account created (or existing one confirmed)
- [ ] New JSON key generated and stored securely
- [ ] Old key ID noted for later revocation

### Access
- [ ] Google Drive API and Sheets API enabled in GCP project
- [ ] Drive folder shared with new service account as **Viewer**
- [ ] Google Sheet shared with new service account as **Editor**

### Deployment
- [ ] Application config updated with new credentials
- [ ] Secret updated in deployment environment
- [ ] Service restarted / redeployed

### Validation
- [ ] Connectivity test script passes
- [ ] Full sync dry-run succeeds
- [ ] Production sync monitored for at least one full cycle

### Cleanup
- [ ] Old key revoked in Google Cloud Console
- [ ] Old service account access removed from Sheets/Docs (if replaced entirely)
- [ ] Migration documented in team runbook / changelog

---

*Guide prepared by: Adam V. and Opus 4.6
