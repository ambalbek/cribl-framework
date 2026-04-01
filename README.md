# Cribl Framework

Unified platform for application onboarding into **Cribl Stream** and **ELK**. Combines the **Onboarding Portal** (client-facing request form) and **Cribl Pusher** (route/destination/ELK-role automation) into a single Flask application.

---

## Table of Contents

1. [What It Does](#what-it-does)
2. [End-to-End Workflow](#end-to-end-workflow)
3. [Prerequisites](#prerequisites)
4. [File Structure](#file-structure)
5. [First-Time Setup](#first-time-setup)
6. [Configuration Reference](#configuration-reference)
7. [Template Files](#template-files)
8. [App Input Format](#app-input-format)
9. [Running the Application](#running-the-application)
10. [Web UI](#web-ui)
11. [rode_rm.py — ELK Roles + Cribl](#rode_rmpy--elk-roles--cribl)
12. [Docker](#docker)
13. [Serving via Apache httpd (bastion)](#serving-via-apache-httpd-bastion)
14. [All CLI Flags](#all-cli-flags)
15. [Logging](#logging)
16. [Safety Features](#safety-features)
17. [Rolling Back a Change](#rolling-back-a-change)
18. [Troubleshooting](#troubleshooting)

---

## What It Does

### Onboarding Portal

Clients submit structured onboarding requests via a web form:

- **LAN ID** and **Name / Last Name** of the requester
- **APM ID** and **App Name** of the application
- **Region** (Azure North or Azure South)
- **Log Destination** (Dynatrace and/or ELK)
- **Log Type** (Application Logs and/or Metrics)
- **Entitlement Groups** (AD groups for access)

Each submission is stored as a document in an Elasticsearch index with a unique **Request ID** (`REQ-YYYYMMDD-XXXXXXXX`).

### Cribl Pusher

For each application you provide (by ID and name), the script:

1. Fetches the current route table from Cribl (`GET /api/v1/m/{worker_group}/routes/{routes_table}`)
2. Fetches all existing destinations (`GET /system/outputs`) to build a skip-list
3. Inserts a new route above the catch-all/default route — skipping any that already exist
4. Shows a full unified diff so you can review exactly what will change
5. Asks for confirmation before writing anything
6. Saves a rollback snapshot of the original route table
7. Creates any destination that does not already exist (`POST /system/outputs`) — skips if present
8. Patches the route table back to Cribl (`PATCH /api/v1/m/{worker_group}/routes/{routes_table}`)

### ELK Roles + Cribl Routes (rode_rm.py)

`rode_rm.py` applies **ELK roles/role-mappings** and **Cribl routes/destinations** in a single command:

1. Generates ELK role and role-mapping templates (always saved to `ops_rm_r_templates_output/`)
2. Pushes roles and role-mappings to Elasticsearch via `PUT /_security/role/{name}` and `PUT /_security/role_mapping/{name}`
3. Runs the same route + destination upsert logic as `cribl-pusher.py`
4. Runs the two sides in the configured order (`elk-first` by default)

### Automatic Status Update

After a successful run (non-dry-run), the framework **automatically updates the onboarding request status to `done`** in the Elasticsearch index. The operator simply pastes the `REQ-YYYYMMDD-XXXXXXXX` ID into the Portal Request ID field before running.

---

## End-to-End Workflow

```
1. Client opens the Onboarding Portal (/portal)
   → Fills in LAN ID, Name, App ID, App Name, Region, Log Destination, Log Type, Entitlement Groups
   → Receives Request ID: REQ-20260327-A1B2C3D4

2. Platform team opens Cribl Pusher (/cribl/app)
   → Pastes REQ-20260327-A1B2C3D4 in "Portal Request ID"
   → Selects workspace, worker group(s), region
   → Enters app details (or uploads bulk file)
   → Runs with Dry Run first to preview changes

3. Unchecks Dry Run, clicks Run
   → Routes created in Cribl
   → Destinations created in Cribl
   → ELK roles/role-mappings created (if using rode_rm)
   → Request status auto-updated to "done" in Elasticsearch

4. Client's request is marked as completed
```

---

## Prerequisites

- **Python 3.10 or newer** *(not needed if running via Docker)*
- **Docker Desktop** *(optional — for the containerised option)*
- **pip** packages:

```bash
pip install -r requirements.txt
```

Verify your Python version:

```bash
python --version
# Should print Python 3.10.x or higher
```

---

## File Structure

```
cribl-framework/
│
├── app.py                          # Unified Flask app — portal + pusher + admin
├── cribl-pusher.py                 # CLI — add routes + upsert destinations
├── rode_rm.py                      # CLI — pushes ELK roles + Cribl routes together
├── _validate.py                    # Offline validation script
├── cribl_api.py                    # Cribl API + route logic
├── cribl_config.py                 # Config loading and workspace resolution
├── cribl_utils.py                  # Shared utilities (I/O, prompts, HTTP session)
├── cribl_logger.py                 # Logging setup
│
├── Dockerfile                      # Container image — python:3.13-slim
├── docker-compose.yml              # One service on port 5000
├── .dockerignore                   # Excludes config.json, snapshots, logs
├── requirements.txt                # Pinned pip dependencies
│
├── config.json                     # YOUR config (credentials + workspaces) — never commit
├── config.example.json             # Safe-to-commit template — copy to config.json
│
├── route_template_azn.json         # Route shape for Azure North
├── route_template_azs.json         # Route shape for Azure South
├── blob_dest_template_azn_dev.json # Dest shape — AZN dev
├── blob_dest_template_azs_dev.json # Dest shape — AZS dev
├── blob_dest_template_azn_test.json
├── blob_dest_template_azs_test.json
├── blob_dest_template_azn_prod.json
├── blob_dest_template_azs_prod.json
│
├── elk-index-template.json         # ES index template for onboarding requests
├── elk-role.json                   # ES role for portal writer
│
├── templates/
│   ├── index.html                  # Unified landing page
│   ├── request.html                # Onboarding portal form
│   ├── admin.html                  # Admin status update form
│   └── app.html                    # Cribl Pusher UI (2 tabs)
│
├── ops_rm_r_templates_output/      # Auto-created by rode_rm.py
│
└── cribl_snapshots/                # Auto-created — rollback snapshots
    ├── dev/
    ├── test/
    └── prod/
```

> `config.json` and `cribl_snapshots/` are in `.gitignore` and will never be committed.

---

## First-Time Setup

### Step 1 — Clone / copy the files

Make sure all `.py` files, template `.json` files, and `config.example.json` are in the same folder.

### Step 2 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 3 — Create your config file

```bash
# Windows
copy config.example.json config.json

# Mac / Linux
cp config.example.json config.json
```

### Step 4 — Edit config.json

Open `config.json` and fill in your values. See [Configuration Reference](#configuration-reference) for all fields.

### Step 5 — Apply the ES index template (for the portal)

```bash
curl -k -X PUT "https://YOUR_ELK:9200/_index_template/cribl-onboarding-requests" \
  -H "Content-Type: application/json" \
  -d @elk-index-template.json
```

### Step 6 — Do a dry run

```bash
python cribl-pusher.py --workspace dev --worker-group default --region azn --dry-run --appid TEST001 --appname "Test App"
```

You should see the `=== TARGET ===` banner and a diff preview with no errors. **Nothing is written on a dry run.**

---

## Configuration Reference

### Top-level fields

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url` | string | — | Default Cribl root URL (overridden per workspace or via `--cribl-url`) |
| `cribl_urls` | list | `[]` | Cribl URLs shown as a dropdown in the UI |
| `elk_urls` | list | `[]` | ELK URLs shown as a dropdown in the UI |
| `skip_ssl` | bool | `false` | Disable SSL cert verification globally |
| `credentials.token` | string | `""` | Bearer token — if set, skips username/password login |
| `credentials.username` | string | `""` | Login username |
| `credentials.password` | string | `""` | Login password |
| `route_templates` | object | — | Map of region to route template path |
| `dest_prefixes` | object | — | Map of region to destination ID prefix |
| `snapshot_dir` | string | `cribl_snapshots` | Directory where rollback snapshots are saved |
| `min_existing_total_routes` | int | `1` | Refuse to PATCH if fewer than this many routes are loaded |
| `diff_lines` | int | `3` | Lines of context shown in the diff preview |
| `admin_secret` | string | — | Secret for the admin status update API |
| `datastream.elk_url` | string | — | Elasticsearch URL for the onboarding requests index |
| `datastream.token` | string | `""` | ES API key (base64) — overrides username/password |
| `datastream.username` | string | `""` | ES username (basic auth) |
| `datastream.password` | string | `""` | ES password (basic auth) |
| `datastream.index` | string | `cribl-onboarding-requests` | ES index name |
| `datastream.skip_ssl` | bool | `false` | Disable SSL for ES connections |
| `datastream.timeout` | int | `30` | ES request timeout in seconds |

### Workspace fields

Each key under `workspaces` is a name you choose (e.g. `"dev"`, `"prod"`).

| Field | Required | Description |
|---|---|---|
| `worker_groups` | yes | List of Cribl worker group names (e.g. `["default", "wg-dev-01"]`) |
| `dest_templates` | yes* | Object mapping region to dest template path |
| `dest_template` | yes* | Alternative: single dest template path (skips region lookup) |
| `base_url` | no | Overrides global `base_url` for this workspace |
| `routes_table` | no | Route table name. Defaults to `"default"` |
| `description` | no | Human-readable label shown in the UI |
| `require_allow` | no | If `true`, user must confirm before writes (recommended for prod) |
| `skip_ssl` | no | Overrides global `skip_ssl` for this workspace |

*One of `dest_templates` or `dest_template` is required.

### Credential priority (highest to lowest)

```
1. --token / --username / --password  CLI flags
2. CRIBL_TOKEN / CRIBL_USERNAME / CRIBL_PASSWORD  environment variables
3. credentials block in config.json
```

---

## Template Files

### route_template_azn.json / route_template_azs.json

One file per region. The script fills in `id`, `filter`, `output`, and `name` for each app. Minimum working example:

```json
{
  "pipeline": "passthru",
  "final": false,
  "disabled": false,
  "clones": [],
  "description": "",
  "enableOutputExpression": false
}
```

### blob_dest_template_{region}_{workspace}.json

One file per region x workspace. The script fills in `id`, `name`, `containerName`, and `description` automatically.

---

## App Input Format

### Single app — via CLI flags

```bash
python cribl-pusher.py --appid APP001 --appname "My Application"
```

### Bulk apps — via text file

Create a file with one app per line:

```
# Lines starting with # are comments
APP001, My First Application
APP002, My Second Application
APP003, Another App
```

Format: `appid, appname` (comma-separated). Blank lines and `#` comments are skipped.

---

## Running the Application

### Web UI (recommended)

```bash
python app.py
```

Opens `http://localhost:5000`. All features are available:

| URL | What |
|---|---|
| `/` | Unified landing page |
| `/portal` | Onboarding request form |
| `/portal/admin/update-status` | Admin status update |
| `/cribl/app` | Cribl Pusher + ELK Roles UI |
| `/health` | Health check |
| `/health/es` | Elasticsearch health check |

### CLI — single app

```bash
python cribl-pusher.py \
  --workspace dev \
  --worker-group default \
  --region azn \
  --appid APP001 \
  --appname "My Application" \
  --yes
```

### CLI — bulk file

```bash
python cribl-pusher.py \
  --workspace dev \
  --worker-group default \
  --region azn \
  --from-file \
  --appfile appids.txt \
  --yes
```

### CLI — dry run

```bash
python cribl-pusher.py --workspace dev --worker-group default --region azn --dry-run --from-file
```

---

## Web UI

### Landing Page (/)

Links to all three sections: Onboarding Portal, Cribl Pusher, and Admin.

### Onboarding Portal (/portal)

Client-facing form with the following fields:

| Field | Required | Description |
|---|---|---|
| LAN ID | yes | Requester's LAN ID |
| Name / Last Name | yes | Requester's full name |
| APM ID | yes | Application ID |
| App Name | yes | Application name (single word, underscores allowed) |
| Region | yes | Azure North (azn) or Azure South (azs) |
| Log Destination | yes | Dynatrace and/or ELK |
| Log Type | yes | Application Logs and/or Metrics |
| Entitlement Groups | yes | AD groups for access (tag input) |

Returns a `REQ-YYYYMMDD-XXXXXXXX` Request ID on success.

### Cribl Pusher (/cribl/app)

Two tabs:

**Tab 1 — Cribl Pusher**
- Portal Request ID (optional — auto-updates status on success)
- Workspace, Worker Group(s), Region
- App Input (single or bulk file)
- Options: Dry Run, Skip SSL, Log Level
- Credentials override, Advanced Options

**Tab 2 — ELK Roles + Cribl Routes**
- Portal Request ID (optional — auto-updates status on success)
- App Input (single or bulk)
- ELK Nonprod/Prod URLs + credentials
- Cribl Workspace, Worker Group, Region
- Options: Dry Run, Order, Skip ELK/Cribl

> **Dry Run defaults to ON** in both tabs. Uncheck it to perform actual writes.

### Admin (/portal/admin/update-status)

Manual status update form. Requires `X-Admin-Secret` header matching `admin_secret` in config.json.

---

## rode_rm.py — ELK Roles + Cribl

### Generated ELK templates

Every run saves four files per app to `ops_rm_r_templates_output/`:

| File | Description |
|---|---|
| `roles_{apmid}.json` | Kibana Dev Console format (human review) |
| `role_mappings_{apmid}.json` | Kibana Dev Console format (human review) |
| `roles_{apmid}_pushable.json` | JSON array ready to push via API |
| `role_mappings_{apmid}_pushable.json` | JSON array ready to push via API |

### Basic usage

```bash
python rode_rm.py \
  --app_name "My Application" \
  --apmid    "app00001234" \
  --elk-url  "https://elk.company.com:9200" \
  --elk-user elastic \
  --elk-url-prod "https://elk-prod.company.com:9200" \
  --elk-user-prod elastic \
  --workspace dev \
  --dry-run
```

### Generate templates only (no API calls)

```bash
python rode_rm.py \
  --app_name "My Application" \
  --apmid    "app00001234" \
  --skip-elk \
  --skip-cribl
```

---

## Docker

### Build

```bash
docker build -t cribl-framework .
```

### Run

```bash
docker run -d --name cribl-framework \
  -p 5000:5000 \
  -v $(pwd)/config.json:/app/config.json:ro \
  -v $(pwd)/cribl_snapshots:/app/cribl_snapshots \
  cribl-framework
```

Then open `http://localhost:5000`.

### Docker Compose (recommended)

```bash
docker compose up -d
```

---

## Serving via Apache httpd (bastion)

Docker and Apache both run on the bastion host. Docker binds to loopback only.

```
Browser → https://bastion/cribl/app
          Apache ProxyPass → http://127.0.0.1:5000/cribl/app
          Docker container → Flask :5000  (loopback only)
```

| URL | What |
|---|---|
| `https://bastion/` | Landing page |
| `https://bastion/portal` | Onboarding Portal |
| `https://bastion/cribl/app` | Cribl Pusher UI |
| `https://bastion/portal/admin/update-status` | Admin panel |

---

## All CLI Flags

### cribl-pusher.py

| Flag | Default | Description |
|---|---|---|
| `--config` | `config.json` | Path to the config file |
| `--cribl-url` | `""` | Cribl base URL override |
| `--workspace` | *(prompts)* | Workspace name |
| `--worker-group` | *(prompts)* | Worker group |
| `--region` | *(prompts)* | Region: `azn` or `azs` |
| `--allow-prod` | false | Skip ALLOW prompt for protected workspaces |
| `--token` | `""` | Bearer token override |
| `--username` | `""` | Username override |
| `--password` | `""` | Password override |
| `--skip-ssl` | false | Disable SSL verification |
| `--dry-run` | false | Preview only — no writes |
| `--yes` | false | Skip confirmation prompt |
| `--appid` | *(prompts)* | Single app ID |
| `--appname` | *(prompts)* | Single app name |
| `--from-file` | false | Load apps from file |
| `--appfile` | `appids.txt` | Path to apps file |
| `--group-id` | `""` | Insert into route group |
| `--create-missing-group` | false | Create group if missing |
| `--group-name` | `""` | Display name for new group |
| `--min-existing-total-routes` | *(config)* | Safety minimum route count |
| `--diff-lines` | *(config)* | Diff context lines |
| `--snapshot-dir` | *(config)* | Snapshot directory |
| `--log-level` | `INFO` | Log verbosity |
| `--log-file` | `""` | Append logs to file |

### rode_rm.py

| Flag | Default | Description |
|---|---|---|
| `--app_name` | *(required)* | Application name |
| `--apmid` | *(required)* | App ID |
| `--from-file` | false | Read from file |
| `--appfile` | `appids.txt` | App list file |
| `--elk-url` | *(required unless --skip-elk)* | ELK nonprod URL |
| `--elk-url-prod` | *(required unless --skip-elk)* | ELK prod URL |
| `--elk-user` / `--elk-password` / `--elk-token` | `""` | ELK nonprod credentials |
| `--elk-user-prod` / `--elk-password-prod` / `--elk-token-prod` | `""` | ELK prod credentials |
| `--cribl-url` | `""` | Cribl URL override |
| `--workspace` | *(required unless --skip-cribl)* | Workspace name |
| `--worker-group` | *(prompts)* | Worker group |
| `--region` | `""` | Region: `azn` or `azs` |
| `--allow-prod` | false | Skip ALLOW prompt |
| `--order` | `elk-first` | `elk-first` or `cribl-first` |
| `--skip-elk` | false | Skip ELK side |
| `--skip-cribl` | false | Skip Cribl side |
| `--dry-run` | false | Preview only |
| `--skip-ssl` | false | Disable SSL |
| `--log-level` | `INFO` | Log verbosity |
| `--yes` | false | Skip confirmation |

---

## Logging

All output uses Python's `logging` module.

| Level | What you see |
|---|---|
| `ERROR` | Only errors and fatal messages |
| `WARNING` | Errors + warnings |
| `INFO` | Normal run output — targets, plan, OK/SKIP lines *(default)* |
| `DEBUG` | Everything above + HTTP verb/URL + per-route detail |

---

## Safety Features

| Guard | What it does |
|---|---|
| **Diff preview** | Shows a full unified diff before confirmation |
| **Minimum routes check** | Refuses to PATCH if fewer than `min_existing_total_routes` |
| **No-shrink check** | Refuses to PATCH if new total < current total |
| **Duplicate skip** | Skips apps whose route name or filter already exist |
| **require_allow** | Protected workspaces require `ALLOW` confirmation |
| **Dry run** | Runs full logic but never writes |
| **Rollback snapshot** | Original routes saved before every PATCH |

---

## Rolling Back a Change

Find the snapshot file from the run output:

```
[SNAPSHOT] cribl_snapshots/prod/routes_snapshot_20260327T143022Z.json
```

Restore it:

```bash
curl -k -X PATCH \
  "https://YOUR_CRIBL:9000/api/v1/m/{worker_group}/routes/{routes_table}" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d @cribl_snapshots/prod/routes_snapshot_20260327T143022Z.json
```

---

## Troubleshooting

### `Config file not found: config.json`

```bash
cp config.example.json config.json
```

### `datastream.elk_url is not configured in config.json`

Add the `datastream` block to your `config.json`:

```json
"datastream": {
  "elk_url": "https://localhost:9200",
  "index": "cribl-onboarding-requests",
  "skip_ssl": true,
  "timeout": 30
}
```

### `FileNotFoundError: route_template_azn.json`

The template files must exist in the same folder. See [Template Files](#template-files).

### `[ERR] login failed: 401`

Wrong username/password. Generate a token in Cribl UI under **Settings > API tokens** and set `credentials.token`.

### `SSL: CERTIFICATE_VERIFY_FAILED`

Set `"skip_ssl": true` in config.json or pass `--skip-ssl` at runtime.

### `[SAFETY] Refusing to PATCH: total_before=0 < min=1`

The GET returned an empty route table. Check `base_url`, `worker_group`, and permissions.

### Portal status not updating

1. `admin_secret` is set in `config.json`
2. `datastream.elk_url` points to the correct ELK cluster
3. Portal Request ID was filled in before clicking Run
4. **Dry Run was unchecked**

### Docker container can't reach Cribl/ELK

Use `host.docker.internal` instead of `localhost`:

```json
"base_url": "https://host.docker.internal:9000"
```
