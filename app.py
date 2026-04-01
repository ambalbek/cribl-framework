#!/usr/bin/env python3
"""
app.py — Unified Cribl Framework

Combines:
  - cribl-flask  (Cribl Pusher + ELK Roles + Cribl Routes)
  - cribl-portal (Client-facing onboarding request portal)

Run with:
    flask run --host=0.0.0.0 --port=5000
  or:
    python app.py

Environment variables:
    LOG_LEVEL   DEBUG / INFO / WARNING / ERROR  (default: INFO)
    LOG_FILE    Path to log file  (default: none, console only)
"""
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import requests as http_client
import urllib3
from flask import Flask, g, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

SCRIPT_DIR  = Path(__file__).parent.resolve()
CONFIG_PATH = SCRIPT_DIR / "config.json"
PUSHER      = SCRIPT_DIR / "cribl-pusher.py"
RODE_RM     = SCRIPT_DIR / "rode_rm.py"


# ── Logging setup ──────────────────────────────────────────────────────────────

def setup_app_logging(app: Flask) -> logging.Logger:
    """
    Configure a dedicated 'cribl-framework' logger for the web layer.

    - Console handler always attached (stdout).
    - File handler attached when LOG_FILE env var is set
      (daily rotation, 30-day retention).
    - Flask's default werkzeug request logger is left intact but its
      level is raised to WARNING so it doesn't double-print every request.
    """
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        log_level = "INFO"

    fmt       = "%(asctime)s  %(levelname)-8s  [framework]  %(message)s"
    datefmt   = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)

    logger = logging.getLogger("cribl-framework")
    logger.setLevel(getattr(logging, log_level))
    logger.handlers.clear()
    logger.propagate = False

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File (optional)
    log_file = os.environ.get("LOG_FILE", "").strip()
    if log_file:
        fh = TimedRotatingFileHandler(
            log_file, when="midnight", backupCount=30, encoding="utf-8"
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.info("File logging enabled: %s", log_file)

    # Silence werkzeug's per-request lines (we log our own)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    return logger


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10 MB

log = setup_app_logging(app)


# ── Request lifecycle hooks ────────────────────────────────────────────────────

@app.before_request
def _before():
    g.start_time = time.monotonic()
    log.info("→ %s %s  [%s]", request.method, request.path,
             request.remote_addr or "-")


@app.after_request
def _after(response):
    elapsed_ms = (time.monotonic() - g.start_time) * 1000
    level = logging.WARNING if response.status_code >= 400 else logging.INFO
    log.log(level, "← %s %s  %d  %.0fms",
            request.method, request.path,
            response.status_code, elapsed_ms)
    return response


# ── Unhandled exception handler — always return JSON, never bare HTML ──────────

@app.errorhandler(404)
def _not_found(exc):
    log.warning("404 Not Found: %s %s", request.method, request.path)
    return jsonify({"errors": [f"Not found: {request.path}"]}), 404


@app.errorhandler(Exception)
def _handle_exception(exc):
    # Let Flask handle standard HTTP errors (404, 405, etc.) normally
    if isinstance(exc, HTTPException):
        return exc

    if isinstance(exc, SystemExit):
        # sys.exit() called inside a route (e.g. cribl die()) — treat as 500
        msg = f"Internal process exited unexpectedly (code={exc.code})"
    else:
        msg = str(exc)

    log.error("Unhandled exception on %s %s:\n%s",
              request.method, request.path,
              traceback.format_exc())
    return jsonify({"errors": [f"Server error: {msg}"]}), 500


# ── Helpers ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def run_subprocess(cmd: list, masked: str = "") -> tuple:
    log.info("  subprocess: %s", masked or " ".join(cmd))
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
        env=env,
        cwd=str(SCRIPT_DIR),
    )
    log.info("  subprocess exit code: %d", result.returncode)
    if result.returncode != 0:
        log.warning("  subprocess failed — first 500 chars: %s",
                    (result.stdout or "")[:500])
    return result.stdout or "", result.returncode


def mask_cmd(cmd: list, sensitive: set) -> str:
    masked = [
        "***" if i > 0 and cmd[i - 1] in sensitive else part
        for i, part in enumerate(cmd)
    ]
    return " ".join(masked)


# ── Portal helpers ─────────────────────────────────────────────────────────────

def es_index(doc: dict, config: dict) -> str:
    """Write a document to the configured ES datastream. Returns the ES _id."""
    ds       = config.get("datastream", {})
    base_url = ds.get("elk_url", "").strip().rstrip("/")
    index    = ds.get("index", "logs-cribl-onboarding-requests")
    skip_ssl = ds.get("skip_ssl", False)
    timeout  = ds.get("timeout", 30)

    if not base_url:
        raise ValueError(
            'datastream.elk_url is not configured in config.json. '
            'Example: "datastream": { "elk_url": "https://localhost:9200", "index": "cribl-onboarding-requests", ... }'
        )

    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url
        log.debug("elk_url had no scheme — prepended https://: %s", base_url)

    if skip_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = http_client.Session()
    session.verify = not skip_ssl

    headers = {"Content-Type": "application/json"}
    token    = ds.get("token",    "").strip()
    username = ds.get("username", "").strip()
    password = ds.get("password", "").strip()
    if token:
        headers["Authorization"] = f"ApiKey {token}"
    elif username:
        session.auth = (username, password)

    resp = session.post(
        f"{base_url}/{index}/_doc",
        json=doc,
        headers=headers,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json().get("_id", "unknown")


def portal_update_status_internal(request_id: str, status: str, config: dict) -> dict:
    """
    Update the status of a portal request directly in Elasticsearch.
    Used by cribl-pusher after a successful run to mark requests as done.
    """
    ds       = config.get("datastream", {})
    base_url = ds.get("elk_url", "").strip().rstrip("/")
    index    = ds.get("index", "logs-cribl-onboarding-requests")
    skip_ssl = ds.get("skip_ssl", False)
    timeout  = ds.get("timeout", 30)

    if not base_url:
        log.warning("portal_update_status — datastream.elk_url not configured; skipping")
        return {"skipped": True, "reason": "datastream not configured"}

    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url

    if skip_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    headers = {"Content-Type": "application/json"}
    token    = ds.get("token",    "").strip()
    username = ds.get("username", "").strip()
    password = ds.get("password", "").strip()
    if token:
        headers["Authorization"] = f"ApiKey {token}"

    session = http_client.Session()
    session.verify = not skip_ssl
    if not token and username:
        session.auth = (username, password)

    payload = {
        "query":  {"term": {"request_id": request_id}},
        "script": {"source": f"ctx._source.status = '{status}'", "lang": "painless"},
    }

    try:
        resp = session.post(
            f"{base_url}/{index}/_update_by_query",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code == 200:
            result  = resp.json()
            updated = result.get("updated", 0)
            log.info("portal_update_status — request_id=%s  status=%s  updated=%d",
                     request_id, status, updated)
            return {"ok": True, "updated": updated}
        else:
            log.warning("portal_update_status — %d %s", resp.status_code, resp.text[:200])
            return {"ok": False, "status_code": resp.status_code, "body": resp.text[:500]}
    except Exception as exc:
        log.error("portal_update_status — failed: %s", exc)
        return {"ok": False, "error": str(exc)}


# ── Command builders (for subprocess calls to CLI scripts) ─────────────────────

def build_pusher_cmd(form: dict, appfile_path: str) -> tuple:
    cmd = [
        sys.executable, str(PUSHER),
        "--yes",
        "--workspace",    form["workspace"],
        "--worker-group", form["worker_group"],
        "--region",       form["region"],
        "--log-level",    form.get("log_level", "INFO"),
        "--config",       str(CONFIG_PATH),
    ]

    if form.get("cribl_url", "").strip():
        cmd += ["--cribl-url", form["cribl_url"].strip()]
    if form.get("allow_prod"):
        cmd.append("--allow-prod")
    if form.get("dry_run"):
        cmd.append("--dry-run")
    if form.get("skip_ssl"):
        cmd.append("--skip-ssl")

    token    = form.get("token", "").strip()
    username = form.get("username", "").strip()
    password = form.get("password", "").strip()
    if token:
        cmd += ["--token", token]
    elif username and password:
        cmd += ["--username", username, "--password", password]

    if form.get("mode") == "bulk":
        cmd += ["--from-file", "--appfile", appfile_path or ""]
    else:
        cmd += ["--appid",   form.get("appid", "").strip(),
                "--appname", form.get("appname", "").strip()]

    group_id = form.get("group_id", "").strip()
    if group_id:
        cmd += ["--group-id", group_id]
        if form.get("create_missing_group"):
            cmd.append("--create-missing-group")
        if form.get("group_name", "").strip():
            cmd += ["--group-name", form["group_name"].strip()]

    if form.get("min_routes", "").strip():
        cmd += ["--min-existing-total-routes", form["min_routes"].strip()]
    if form.get("diff_lines", "").strip():
        cmd += ["--diff-lines", form["diff_lines"].strip()]
    if form.get("snapshot_dir", "").strip():
        cmd += ["--snapshot-dir", form["snapshot_dir"].strip()]
    if form.get("log_file", "").strip():
        cmd += ["--log-file", form["log_file"].strip()]

    sensitive = {"--password", "--token"}
    return cmd, mask_cmd(cmd, sensitive)


def build_rode_rm_cmd(form: dict, appfile_path: str) -> tuple:
    cmd = [sys.executable, str(RODE_RM), "--yes", "--config", str(CONFIG_PATH)]

    if form.get("mode") == "bulk":
        cmd += ["--from-file", "--appfile", appfile_path or ""]
    else:
        cmd += ["--app_name", form.get("app_name", "").strip(),
                "--apmid",    form.get("apmid", "").strip()]

    cribl_token    = form.get("cribl_token", "").strip()
    cribl_username = form.get("cribl_username", "").strip()
    cribl_password = form.get("cribl_password", "").strip()
    if cribl_token:
        cmd += ["--token", cribl_token]
    elif cribl_username and cribl_password:
        cmd += ["--username", cribl_username, "--password", cribl_password]

    skip_elk = bool(form.get("skip_elk"))
    if not skip_elk:
        cmd += ["--elk-url", form.get("elk_url_nonprod", "").strip()]
        np_token = form.get("elk_token_nonprod", "").strip()
        np_user  = form.get("elk_user_nonprod", "").strip()
        np_pass  = form.get("elk_password_nonprod", "").strip()
        if np_token:
            cmd += ["--elk-token", np_token]
        elif np_user:
            cmd += ["--elk-user", np_user]
            if np_pass:
                cmd += ["--elk-password", np_pass]

        cmd += ["--elk-url-prod", form.get("elk_url_prod", "").strip()]
        p_token = form.get("elk_token_prod", "").strip()
        p_user  = form.get("elk_user_prod", "").strip()
        p_pass  = form.get("elk_password_prod", "").strip()
        if p_token:
            cmd += ["--elk-token-prod", p_token]
        elif p_user:
            cmd += ["--elk-user-prod", p_user]
            if p_pass:
                cmd += ["--elk-password-prod", p_pass]

    if form.get("cribl_url", "").strip():
        cmd += ["--cribl-url", form["cribl_url"].strip()]
    cmd += ["--workspace", form.get("workspace", "")]
    if form.get("worker_group", "").strip():
        cmd += ["--worker-group", form["worker_group"].strip()]
    if form.get("region", "").strip():
        cmd += ["--region", form["region"].strip()]
    if form.get("allow_prod"):
        cmd.append("--allow-prod")
    cmd += ["--order", form.get("order", "elk-first")]
    if skip_elk:
        cmd.append("--skip-elk")
    if form.get("skip_cribl"):
        cmd.append("--skip-cribl")
    if form.get("dry_run"):
        cmd.append("--dry-run")
    if form.get("skip_ssl"):
        cmd.append("--skip-ssl")
    cmd += ["--log-level", form.get("log_level", "INFO")]

    sensitive = {
        "--elk-password", "--elk-token",
        "--elk-password-prod", "--elk-token-prod",
        "--password", "--token",
    }
    return cmd, mask_cmd(cmd, sensitive)


# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES — Landing / Navigation
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def landing():
    return render_template("index.html")


@app.route("/health")
def health():
    return "ok", 200


# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES — Portal (onboarding requests)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/portal")
@app.route("/portal/")
def portal_index():
    config = load_config()
    return render_template("request.html", iiq_url=config.get("iiq_url", ""))


@app.route("/portal/api/submit", methods=["POST"])
@app.route("/api/submit", methods=["POST"])
def portal_submit():
    log.debug("submit — Content-Type: %s  body: %s",
              request.content_type, request.get_data(as_text=True)[:500])

    data     = request.get_json(silent=True) or {}
    lan_id   = (data.get("lan_id")   or "").strip()
    req_name = (data.get("requester_name") or "").strip()
    app_id   = (data.get("apmid")    or "").strip()
    app_name = (data.get("appname")  or "").strip()
    region   = (data.get("region")   or "").strip()
    log_dests = [d for d in (data.get("log_destinations") or []) if d]
    log_types = [t for t in (data.get("log_types") or []) if t]
    groups   = [grp for grp in (data.get("groups") or []) if grp]

    log.info("submit — lan_id=%r  requester_name=%r  apmid=%r  appname=%r  region=%r  log_dest=%s  log_types=%s  groups=%s",
             lan_id, req_name, app_id, app_name, region, log_dests, log_types, groups)

    errors = []
    if not lan_id:                        errors.append("LAN ID is required.")
    if not req_name:                      errors.append("Name / Last Name is required.")
    if not app_id:                        errors.append("APM ID is required.")
    if not app_name:                      errors.append("App Name is required.")
    elif not re.match(r"^\w+$", app_name):
                                          errors.append("App Name must be a single word using only letters, numbers, and underscores.")
    if region not in ("azn", "azs"):      errors.append("Region must be azn or azs.")
    if not log_dests:                     errors.append("Select at least one log destination.")
    if not log_types:                     errors.append("Select at least one log type.")
    if not groups:                        errors.append("Select at least one entitlement group.")
    if errors:
        return jsonify({"errors": errors}), 400

    try:
        config = load_config()
    except Exception as exc:
        return jsonify({"errors": [f"Could not load config.json: {exc}"]}), 500

    now        = datetime.now(timezone.utc)
    request_id = f"REQ-{now.strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    doc = {
        "@timestamp":         now.isoformat(),
        "request_id":         request_id,
        "lan_id":             lan_id,
        "requester_name":     req_name,
        "apmid":              app_id,
        "appname":            app_name,
        "region":             region,
        "log_destinations":   log_dests,
        "log_types":          log_types,
        "entitlement_groups": groups,
        "status":             "pending",
    }

    try:
        log.info("indexing to ES — index=%s  request_id=%s",
                 config.get("datastream", {}).get("index", "logs-cribl-onboarding-requests"),
                 request_id)
        es_id = es_index(doc, config)
        log.info("ES index OK — request_id=%s  es_id=%s", request_id, es_id)
    except Exception as exc:
        log.error("ES index failed — %s: %s", type(exc).__name__, exc)
        return jsonify({"errors": [f"Failed to store request: {exc}"]}), 500

    return jsonify({"request_id": request_id})


@app.route("/portal/admin/update-status", methods=["GET", "POST"])
@app.route("/admin/update-status", methods=["GET", "POST"])
def portal_admin_update_status():
    if request.method == "GET":
        return render_template("admin.html")
    try:
        config = load_config()
    except Exception as exc:
        return jsonify({"errors": [f"Could not load config.json: {exc}"]}), 500

    secret = config.get("admin_secret", "").strip()
    if not secret:
        return jsonify({"errors": ["admin_secret is not configured"]}), 500

    if request.headers.get("X-Admin-Secret", "") != secret:
        log.warning("admin/update-status — unauthorized attempt from %s", request.remote_addr)
        return jsonify({"errors": ["Unauthorized"]}), 403

    data       = request.get_json(silent=True) or {}
    request_id = (data.get("request_id") or "").strip()
    status     = (data.get("status")     or "").strip()

    if not request_id:
        return jsonify({"errors": ["request_id is required"]}), 400
    if status not in ("pending", "done", "rejected"):
        return jsonify({"errors": ["status must be one of: pending, done, rejected"]}), 400

    ds       = config.get("datastream", {})
    base_url = ds.get("elk_url", "").strip().rstrip("/")
    index    = ds.get("index", "logs-cribl-onboarding-requests")
    skip_ssl = ds.get("skip_ssl", False)
    timeout  = ds.get("timeout", 30)

    if not base_url.startswith(("http://", "https://")):
        base_url = "https://" + base_url

    if skip_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    headers = {"Content-Type": "application/json"}
    token    = ds.get("token",    "").strip()
    username = ds.get("username", "").strip()
    password = ds.get("password", "").strip()
    if token:
        headers["Authorization"] = f"ApiKey {token}"

    session = http_client.Session()
    session.verify = not skip_ssl
    if not token and username:
        session.auth = (username, password)

    payload = {
        "query":  {"term": {"request_id": request_id}},
        "script": {"source": f"ctx._source.status = '{status}'", "lang": "painless"},
    }

    try:
        resp = session.post(
            f"{base_url}/{index}/_update_by_query",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        result  = resp.json()
        updated = result.get("updated", 0)
        if updated == 0:
            log.warning("admin/update-status — request_id=%s not found", request_id)
            return jsonify({"errors": [f"Request ID {request_id!r} not found"]}), 404
        log.info("admin/update-status — request_id=%s  status=%s  updated=%d", request_id, status, updated)
        return jsonify({"request_id": request_id, "status": status, "updated": updated})
    except Exception as exc:
        log.error("admin/update-status failed — %s: %s", type(exc).__name__, exc)
        return jsonify({"errors": [f"Failed to update status: {exc}"]}), 500


@app.route("/health/es")
def health_es():
    try:
        config  = load_config()
        ds      = config.get("datastream", {})
        base_url = ds.get("elk_url", "").strip().rstrip("/")
        skip_ssl = ds.get("skip_ssl", False)
        timeout  = ds.get("timeout", 30)

        if not base_url.startswith(("http://", "https://")):
            base_url = "https://" + base_url

        if skip_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        headers = {"Content-Type": "application/json"}
        token    = ds.get("token",    "").strip()
        username = ds.get("username", "").strip()
        password = ds.get("password", "").strip()
        if token:
            headers["Authorization"] = f"ApiKey {token}"

        session = http_client.Session()
        session.verify = not skip_ssl
        if not token and username:
            session.auth = (username, password)

        resp = session.get(f"{base_url}/_cluster/health", headers=headers, timeout=timeout)
        return jsonify({"status": "ok", "es_status": resp.status_code, "es_body": resp.json()}), 200
    except Exception as exc:
        log.error("ES health check failed: %s", exc)
        return jsonify({"status": "error", "error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES — Cribl Pusher (automation UI)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/cribl")
@app.route("/cribl/")
def cribl_landing():
    return render_template("index.html")


@app.route("/cribl/app")
@app.route("/cribl/app/")
def cribl_app_page():
    try:
        config = load_config()
    except Exception as exc:
        log.error("Failed to load config.json: %s", exc)
        return f"Error loading config.json: {exc}", 500
    workspaces = {
        k: v for k, v in config.get("workspaces", {}).items()
        if not k.startswith("_")
    }
    return render_template("app.html", workspaces=workspaces, config=config)


@app.route("/cribl/api/run-pusher", methods=["POST"])
def run_pusher():
    form       = request.form
    file       = request.files.get("appfile")
    mode       = form.get("mode", "single")
    request_id = (form.get("request_id") or "").strip()

    errors = []
    if mode == "single":
        if not form.get("appid", "").strip():   errors.append("App ID is required.")
        if not form.get("appname", "").strip(): errors.append("App Name is required.")
    else:
        if not file or not file.filename:
            errors.append("Please upload an app list file (.txt).")

    worker_groups = form.getlist("worker_groups")
    if not worker_groups:
        errors.append("Select at least one worker group.")

    try:
        config = load_config()
    except Exception as exc:
        log.error("Config load error: %s", exc)
        return jsonify({"errors": [f"Could not load config.json: {exc}"]}), 500

    ws_cfg = config.get("workspaces", {}).get(form.get("workspace", ""), {})
    if ws_cfg.get("require_allow") and not form.get("allow_prod"):
        errors.append(
            f"Workspace '{form.get('workspace')}' requires the "
            "'Allow production writes' checkbox."
        )

    if errors:
        log.warning("run-pusher validation failed: %s", errors)
        return jsonify({"errors": errors}), 400

    log.info("run-pusher  workspace=%s  wgs=%s  mode=%s  dry_run=%s",
             form.get("workspace"), worker_groups, mode,
             bool(form.get("dry_run")))

    tmp_path = None
    try:
        if mode == "bulk" and file:
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".txt", delete=False, dir=SCRIPT_DIR
            ) as tmp:
                file.save(tmp)
                tmp_path = tmp.name

        all_output = ""
        last_rc    = 0
        commands   = []

        for wg in worker_groups:
            form_dict = form.to_dict()
            form_dict["worker_group"] = wg
            cmd, masked = build_pusher_cmd(form_dict, tmp_path or "")
            commands.append({"wg": wg, "cmd": masked})
            output, rc = run_subprocess(cmd, masked)
            all_output += f"\n{'='*60}\n Worker group: {wg}\n{'='*60}\n{output}"
            if rc != 0:
                last_rc = rc

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    portal_result = None
    dry_run = bool(form.get("dry_run"))
    if last_rc == 0 and not dry_run and request_id:
        portal_result = portal_update_status_internal(request_id, "done", config)

    return jsonify({
        "output":        all_output.strip(),
        "returncode":    last_rc,
        "commands":      commands,
        "portal_update": portal_result,
    })


@app.route("/cribl/api/run-rode-rm", methods=["POST"])
def run_rode_rm():
    form       = request.form
    file       = request.files.get("appfile")
    mode       = form.get("mode", "single")
    request_id = (form.get("request_id") or "").strip()

    errors    = []
    skip_elk   = bool(form.get("skip_elk"))
    skip_cribl = bool(form.get("skip_cribl"))

    if mode == "single":
        if not form.get("app_name", "").strip(): errors.append("App Name is required.")
        if not form.get("apmid", "").strip():    errors.append("App ID is required.")
    else:
        if not file or not file.filename:
            errors.append("Please upload an app list file (.txt).")

    if skip_elk and skip_cribl:
        errors.append("Nothing to do: both Skip ELK and Skip Cribl are checked.")

    if not skip_cribl and not form.get("worker_group", "").strip():
        errors.append("Worker Group is required when Cribl is not skipped.")

    if not skip_elk:
        if not form.get("elk_url_nonprod", "").strip():
            errors.append("ELK Nonprod URL is required.")
        if not form.get("elk_token_nonprod", "").strip() and not form.get("elk_user_nonprod", "").strip():
            errors.append("ELK Nonprod: provide User or Token.")
        if not form.get("elk_url_prod", "").strip():
            errors.append("ELK Prod URL is required.")
        if not form.get("elk_token_prod", "").strip() and not form.get("elk_user_prod", "").strip():
            errors.append("ELK Prod: provide User or Token.")

    try:
        config = load_config()
    except Exception as exc:
        log.error("Config load error: %s", exc)
        return jsonify({"errors": [f"Could not load config.json: {exc}"]}), 500

    ws_cfg = config.get("workspaces", {}).get(form.get("workspace", ""), {})
    if ws_cfg.get("require_allow") and not form.get("allow_prod"):
        errors.append(
            f"Workspace '{form.get('workspace')}' requires the "
            "'Allow production writes' checkbox."
        )

    if errors:
        log.warning("run-rode-rm validation failed: %s", errors)
        return jsonify({"errors": errors}), 400

    log.info("run-rode-rm  workspace=%s  wg=%s  mode=%s  skip_elk=%s  skip_cribl=%s  dry_run=%s",
             form.get("workspace"), form.get("worker_group"), mode,
             skip_elk, skip_cribl, bool(form.get("dry_run")))

    tmp_path = None
    try:
        if mode == "bulk" and file:
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".txt", delete=False, dir=SCRIPT_DIR
            ) as tmp:
                file.save(tmp)
                tmp_path = tmp.name

        cmd, masked = build_rode_rm_cmd(form.to_dict(), tmp_path or "")
        output, rc  = run_subprocess(cmd, masked)

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    portal_result = None
    dry_run = bool(form.get("dry_run"))
    if rc == 0 and not dry_run and request_id:
        portal_result = portal_update_status_internal(request_id, "done", config)

    return jsonify({
        "output":        output,
        "returncode":    rc,
        "command":       masked,
        "portal_update": portal_result,
    })


if __name__ == "__main__":
    log.info("Starting Cribl Framework on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
