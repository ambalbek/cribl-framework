#!/usr/bin/env python3
"""
cribl-pusher  —  Add routes + upsert destinations across Cribl workspaces.

Usage examples:
  python cribl-pusher.py                                                          # fully interactive
  python cribl-pusher.py --workspace dev --worker-group wg-dev-01 --region azn --appid APP1 --appname "My App"
  python cribl-pusher.py --workspace prod --worker-group wg-prod-01 --region azn --allow-prod --from-file --appfile appids.txt --yes
  python cribl-pusher.py --workspace test --worker-group wg-test-01 --region azs --dry-run --from-file
  python cribl-pusher.py --workspace dev --worker-group wg-dev-02 --region azn --log-level DEBUG --log-file run.log --dry-run --from-file
"""
import os
import json
import copy
import argparse
from pathlib import Path

from cribl_logger import setup_logging
from cribl_utils import (
    die, short_id, now_stamp, pretty_json, unified_diff,
    read_json, read_apps_from_file,
    prompt_choice, prompt_text, prompt_password, confirm_or_exit,
    make_session,
)
from cribl_api import (
    cribl_login_token, normalize_route, find_default_route_index,
    get_routes_target, create_group_if_missing, count_all_routes,
    unwrap_response,
)
from cribl_config import (
    load_config, get_workspace_names, get_workspace,
    build_workspace_urls, resolve_credentials, get_cribl_urls,
    get_route_template_path, get_dest_template_path, get_dest_prefix,
    get_worker_groups,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Cribl: add routes + upsert destinations across configurable workspaces.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Config
    p.add_argument("--config", default="config.json",
                   help="Path to config file (default: config.json)")
    # Cribl URL
    p.add_argument("--cribl-url", default="",
                   help="Cribl base URL (overrides config cribl_urls list and workspace base_url)")

    # Workspace
    p.add_argument("--workspace",
                   help="Workspace/environment name from config (if omitted, prompts interactively)")
    p.add_argument("--worker-group", dest="worker_group", default="",
                   help="Worker group within the workspace (if omitted, prompts interactively)")
    p.add_argument("--region", choices=["azn", "azs"],
                   help="Region: azn or azs (selects route + dest templates; prompts if omitted)")
    p.add_argument("--allow-prod", action="store_true",
                   help="Required for workspaces marked require_allow=true")

    # Auth overrides — lowest priority; config.json and env vars are checked first
    p.add_argument("--token", default="", help="Bearer token override (overrides config + env)")
    p.add_argument("--username", default="", help="Username override (overrides config + env)")
    p.add_argument("--password", default="", help="Password override (overrides config + env)")

    # SSL / execution
    p.add_argument("--skip-ssl", action="store_true",
                   help="Skip SSL verification (overrides config)")
    p.add_argument("--dry-run", action="store_true",
                   help="Preview changes only — no API writes")
    p.add_argument("--yes", action="store_true",
                   help="Non-interactive: skip YES confirmation prompt")

    # App selection
    p.add_argument("--appid", help="Single app ID")
    p.add_argument("--appname", help="Single app name (required with --appid)")
    p.add_argument("--from-file", action="store_true",
                   help="Bulk mode: load apps from file")
    p.add_argument("--appfile", default="appids.txt",
                   help="Apps file path (default: appids.txt)")

    # Route group
    p.add_argument("--group-id", default="",
                   help="Insert routes into this route-group ID")
    p.add_argument("--create-missing-group", action="store_true",
                   help="Create the route group if it does not exist")
    p.add_argument("--group-name", default="",
                   help="Display name when creating a missing group")

    # Safety overrides (all have config.json defaults)
    p.add_argument("--min-existing-total-routes", type=int, default=None,
                   help="Override config min_existing_total_routes")
    p.add_argument("--diff-lines", type=int, default=None,
                   help="Override config diff_lines")
    p.add_argument("--snapshot-dir", default="",
                   help="Override config snapshot_dir")

    # Logging
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Log verbosity (default: INFO)")
    p.add_argument("--log-file", default="",
                   help="Optional path to write logs to a file (appended)")

    return p


def main():
    args = build_parser().parse_args()

    # ── Logging — set up first so every subsequent call can use the logger ────
    log = setup_logging(args.log_level, args.log_file)

    # ── Config ────────────────────────────────────────────────────────────────
    config = load_config(args.config)
    workspace_names = get_workspace_names(config)
    if not workspace_names:
        die("[ERR] No workspaces defined in config.json")

    # ── Workspace selection ────────────────────────────────────────────────────
    if not args.workspace:
        args.workspace = prompt_choice("Select workspace", workspace_names)

    workspace_cfg = get_workspace(config, args.workspace)

    worker_groups = get_worker_groups(workspace_cfg)
    if not args.worker_group:
        args.worker_group = prompt_choice("Select worker group", worker_groups)
    elif args.worker_group not in worker_groups:
        die(f"[ERR] Worker group '{args.worker_group}' not in workspace '{args.workspace}'. "
            f"Available: {worker_groups}")

    if not args.region:
        args.region = prompt_choice("Select region", ["azn", "azs"])

    if workspace_cfg.get("require_allow") and not args.allow_prod:
        log.warning(f"Workspace '{args.workspace}' requires explicit confirmation.")
        answer = prompt_text('Type "ALLOW" to proceed (anything else aborts)', "")
        if answer.strip() != "ALLOW":
            die("Refusing to run: ALLOW not confirmed.")
        args.allow_prod = True

    # ── App selection ─────────────────────────────────────────────────────────
    if args.appid:
        if not args.appname:
            args.appname = prompt_text("appname")
    else:
        if not args.from_file:
            mode = prompt_choice("Mode", ["single", "file"])
            if mode == "single":
                args.appid = prompt_text("appid")
                args.appname = prompt_text("appname")
            else:
                args.from_file = True

        if args.from_file and not args.appid and not os.path.exists(args.appfile):
            args.appfile = prompt_text("appfile", args.appfile)

    # ── Credentials ───────────────────────────────────────────────────────────
    token, username, password = resolve_credentials(config, args)

    if not token:
        if not username:
            username = prompt_text("Username")
        if not password:
            password = prompt_password()

    # ── Load apps ─────────────────────────────────────────────────────────────
    if args.appid:
        apps = [(args.appid.strip(), (args.appname or "").strip())]
        if not apps[0][1]:
            die("appname is required.")
        mode_desc = "single"
    else:
        if not args.from_file:
            die("Refusing to run: choose --appid/--appname or --from-file.")
        apps = read_apps_from_file(args.appfile)
        if not apps:
            die(f"App file is empty: {args.appfile}")
        mode_desc = f"bulk({len(apps)})"

    # ── Resolve settings (CLI > workspace > global config) ───────────────────
    skip_ssl   = args.skip_ssl or workspace_cfg.get("skip_ssl", config.get("skip_ssl", False))
    min_routes = (args.min_existing_total_routes
                  if args.min_existing_total_routes is not None
                  else config.get("min_existing_total_routes", 1))
    diff_lines = (args.diff_lines
                  if args.diff_lines is not None
                  else config.get("diff_lines", 3))
    snapshot_dir = args.snapshot_dir or config.get("snapshot_dir", "cribl_snapshots")

    # ── URLs ──────────────────────────────────────────────────────────────────
    root_url, api_base = build_workspace_urls(config, workspace_cfg, args.worker_group)

    # ── Cribl URL selection ───────────────────────────────────────────────────
    cribl_urls = get_cribl_urls(config)
    if args.cribl_url.strip():
        root_url = args.cribl_url.rstrip("/")
        api_base = f"{root_url}/api/v1/m/{args.worker_group}"
    elif cribl_urls:
        selected_url = prompt_choice("Select Cribl URL", cribl_urls)
        root_url = selected_url
        api_base = f"{root_url}/api/v1/m/{args.worker_group}"

    # ── Templates ─────────────────────────────────────────────────────────────
    route_tmpl_path = get_route_template_path(config, workspace_cfg, args.region)
    dest_tmpl_path  = get_dest_template_path(config, workspace_cfg, args.region)
    dest_prefix     = get_dest_prefix(config, workspace_cfg, args.region)

    route_template    = read_json(route_tmpl_path)
    dest_template     = read_json(dest_tmpl_path)
    fallback_pipeline = route_template.get("pipeline") or "passthru"

    # ── Session + auth ────────────────────────────────────────────────────────
    session = make_session(skip_ssl, no_proxy=True)
    if not token:
        token = cribl_login_token(session, root_url, username, password)

    def H():
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def GET(url):
        log.debug(f"GET  {url}")
        return session.get(url, headers=H(), timeout=60)

    def POST(url, payload):
        log.debug(f"POST {url}")
        return session.post(url, headers=H(), json=payload, timeout=60)

    def PATCH(url, payload):
        log.debug(f"PATCH {url}")
        return session.patch(url, headers=H(), json=payload, timeout=60)

    outputs_url  = f"{api_base}/system/outputs"
    routes_table = workspace_cfg.get("routes_table", "default")
    routes_url   = f"{api_base}/routes/{routes_table}"
    group_id     = (args.group_id or "").strip() or None

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=== TARGET ===")
    log.info(f"workspace    : {args.workspace}  ({workspace_cfg.get('description', '')})")
    log.info(f"worker_group : {args.worker_group}")
    log.info(f"region       : {args.region}")
    log.info(f"api_base     : {api_base}")
    log.info(f"routes_url   : {routes_url}")
    log.info(f"mode         : {mode_desc}")
    log.info(f"apps         : {len(apps)}")
    log.info(f"group-id     : {group_id or '(none)'}")
    log.info(f"dry-run      : {args.dry_run}")
    log.info(f"skip-ssl     : {skip_ssl}")
    if args.log_file:
        log.info(f"log-file     : {args.log_file}")

    # ── 1) GET current routes ─────────────────────────────────────────────────
    rget = GET(routes_url)
    if rget.status_code != 200:
        die(f"[ERR] GET {routes_url}: {rget.status_code} {rget.text}")

    current_obj  = rget.json()
    total_before = count_all_routes(current_obj)
    log.info(f"Loaded total routes (all groups): {total_before}")

    # ── 1b) GET existing destinations ─────────────────────────────────────────
    dget = GET(outputs_url)
    if dget.status_code != 200:
        die(f"[ERR] GET {outputs_url}: {dget.status_code} {dget.text}")

    dget_data = dget.json()
    existing_dest_ids = {
        item["id"]
        for item in dget_data.get("items", [])
        if isinstance(item, dict) and item.get("id")
    }
    log.info(f"Loaded existing destinations: {len(existing_dest_ids)}")

    if total_before < min_routes:
        die(f"[SAFETY] Refusing to PATCH: total_before={total_before} < min={min_routes}")

    # ── Ensure group exists if requested ──────────────────────────────────────
    if group_id:
        tgt, tgt_key, _ = get_routes_target(current_obj, group_id)
        if tgt is None:
            if not args.create_missing_group:
                die(
                    f"[SAFETY] group-id '{group_id}' not found. "
                    f"Use --create-missing-group to create it."
                )
            create_group_if_missing(current_obj, group_id, args.group_name.strip() or None)
            log.info(f"Created missing group '{group_id}'")
            tgt, tgt_key, _ = get_routes_target(current_obj, group_id)
            if tgt is None:
                die(f"[ERR] Failed to create/locate group '{group_id}' after creation")

    # ── 2) Build the patched object ───────────────────────────────────────────
    patch_obj = copy.deepcopy(current_obj)
    target_container, routes_key, _ = get_routes_target(patch_obj, group_id)
    routes_list_raw = target_container.get(routes_key)
    if not isinstance(routes_list_raw, list):
        die("[ERR] Target routes list is not a list (unexpected API shape)")

    # Existing routes are passed back exactly as received from Cribl.
    # Do NOT normalize them — normalizing risks stripping fields Cribl requires.
    # Also exclude any dict that has no "filter" key: Cribl's JS reads route.filter
    # on every entry in the array; a missing key returns JS undefined and throws
    # "Cannot read properties of undefined (reading 'filter')".
    all_dict_routes   = [r for r in routes_list_raw if isinstance(r, dict)]
    existing_routes   = [r for r in all_dict_routes if r.get("filter") is not None]
    filterless_dropped = len(all_dict_routes) - len(existing_routes)
    if filterless_dropped:
        log.warning(
            f"[WARN] {filterless_dropped} route(s) in scope are missing the 'filter' field "
            f"and will be excluded from the PATCH (they would crash Cribl's route processor)."
        )
        total_before -= filterless_dropped  # keep safety baseline consistent

    default_idx      = find_default_route_index(existing_routes)
    existing_names   = {r.get("name")   for r in existing_routes if r.get("name")}
    existing_filters = {r.get("filter") for r in existing_routes if r.get("filter")}

    log.debug(f"Insertion point (before catch-all): index {default_idx} of {len(existing_routes)}")

    new_routes = []
    for appid, appname in apps:
        route           = copy.deepcopy(route_template)
        route["id"]     = appid
        route["filter"] = f'apmId == "{appid}"'
        route["output"] = f"{dest_prefix}-{appid}"
        route["name"]   = f"{dest_prefix}-route-{appid}"
        route           = normalize_route(route, fallback_pipeline)

        if route["name"] in existing_names or route["filter"] in existing_filters:
            log.info(f"[SKIP] route already exists for {appid} — skipping")
            continue

        new_routes.append(route)
        existing_names.add(route["name"])
        existing_filters.add(route["filter"])
        log.debug(f"Queued new route: {route['name']}")

    updated_routes = existing_routes[:default_idx] + new_routes + existing_routes[default_idx:]
    target_container[routes_key] = updated_routes

    # ── 3) Preview diff ───────────────────────────────────────────────────────
    # Diff the unwrapped objects so the output shows routes directly,
    # not the {"items":[…]} wrapper Cribl uses in GET responses.
    before_text = pretty_json(unwrap_response(current_obj))
    after_text  = pretty_json(unwrap_response(patch_obj))
    diff        = unified_diff(
        before_text, after_text,
        "routes_before.json", "routes_after.json",
        n=diff_lines,
    )
    total_after = count_all_routes(patch_obj)

    log.info("=== ROUTE PLAN ===")
    log.info(f"target scope      : {'group:' + group_id if group_id else 'top-level routes'}")
    log.info(f"existing in scope : {len(existing_routes)}")
    log.info(f"new routes        : {len(new_routes)}")
    log.info(f"final in scope    : {len(updated_routes)}")
    log.info(f"total routes all  : {total_before} -> {total_after}")

    if total_after < total_before:
        die(f"[SAFETY] Refusing to PATCH: total_after ({total_after}) < total_before ({total_before})")

    if diff.strip():
        log.info("--- FULL OBJECT DIFF (preview) ---")
        log.info(diff)
    else:
        log.info("No route changes detected.")

    # ── 4) Confirmation ───────────────────────────────────────────────────────
    confirm_or_exit("\nProceed to APPLY these changes?", args.yes)

    if args.dry_run:
        log.info("[DRY RUN] No API writes performed.")
        return

    # ── 5) Snapshot for rollback ──────────────────────────────────────────────
    snap_dir  = Path(snapshot_dir) / args.workspace
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap_file = snap_dir / f"routes_snapshot_{now_stamp()}.json"
    with open(snap_file, "w", encoding="utf-8") as f:
        json.dump(current_obj, f, indent=2)
    log.info(f"[SNAPSHOT] {snap_file}")

    # ── 6) Create destinations ────────────────────────────────────────────────
    for appid, appname in apps:
        dest_id = f"{dest_prefix}-{appid}"

        if dest_id in existing_dest_ids:
            log.info(f"[SKIP] Destination already exists: {dest_id} — skipping")
            continue

        dest                  = copy.deepcopy(dest_template)
        dest["id"]            = dest_id
        dest["containerName"] = appid
        dest["description"]   = appname
        if "name" in dest:
            dest["name"] = dest_id

        rp = POST(outputs_url, dest)
        if rp.status_code in (200, 201):
            log.info(f"[OK] Created destination {dest_id}")
        else:
            die(f"[ERR] Create destination {dest_id}: {rp.status_code} {rp.text}")

    # ── 7) PATCH routes ───────────────────────────────────────────────────────
    # Cribl's GET /routes returns {"count":N,"items":[{inner}]}.
    # The PATCH endpoint expects only the inner route-table object
    # {"id":…,"routes":[…],"groups":{…}}.  Sending the outer wrapper causes
    # Cribl's JS handler to call undefined.filter() (Array method) because
    # payload.routes does not exist at the wrapper level.
    rpatch = PATCH(routes_url, unwrap_response(patch_obj))
    if rpatch.status_code in (200, 204):
        log.info(f"[OK] PATCH {routes_url} -- added {len(new_routes)} new routes.")
        log.info(f"[ROLLBACK] Restore snapshot: {snap_file}")
    else:
        die(f"[ERR] PATCH {routes_url}: {rpatch.status_code} {rpatch.text}")


if __name__ == "__main__":
    main()
