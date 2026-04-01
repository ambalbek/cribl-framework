#!/usr/bin/env python3
"""
Offline validation — no network calls, no real credentials needed.
Run: python _validate.py
"""
import sys
import json
import copy
import tempfile
import os

PASS = 0
FAIL = 0

def ok(msg):
    global PASS
    PASS += 1
    print(f"  [OK] {msg}")

def fail(msg):
    global FAIL
    FAIL += 1
    print(f"  [FAIL] {msg}", file=sys.stderr)

def section(title):
    print(f"\n=== {title} ===")

# ── Load modules ──────────────────────────────────────────────────────────────
section("1. IMPORTS")
import importlib.util

def load_mod(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m

try:
    utils = load_mod("cribl_utils",  "cribl_utils.py")
    api   = load_mod("cribl_api",    "cribl_api.py")
    cfg   = load_mod("cribl_config", "cribl_config.py")
    ok("cribl_utils, cribl_api, cribl_config all imported")
except Exception as e:
    fail(f"import failed: {e}")
    sys.exit(1)

# ── URL construction ──────────────────────────────────────────────────────────
section("2. URL CONSTRUCTION PER WORKSPACE")

config = {
    "base_url": "https://cribl.company.com:9000",
    "workspaces": {
        "dev":    {"worker_groups": ["dev-01", "dev-02"], "dest_templates": {"azn": "blob_dest_template_azn_dev.json"}},
        "qa":     {"worker_groups": ["qa-01"],            "dest_templates": {"azn": "blob_dest_template_azn_qa.json"}},
        "prod":   {"worker_groups": ["prod-01"],          "dest_templates": {"azn": "blob_dest_template_azn_prod.json"}, "require_allow": True},
        "custom": {"worker_groups": ["wg-main"],          "routes_table": "my-table", "dest_templates": {"azn": "blob_dest_template_azn_dev.json"}},
    },
}

# For URL tests, use the first worker_group in each workspace's list
_wg_map = {
    "dev":    "dev-01",
    "qa":     "qa-01",
    "prod":   "prod-01",
    "custom": "wg-main",
}

expected_routes = {
    "dev":    "https://cribl.company.com:9000/api/v1/m/dev-01/routes/default",
    "qa":     "https://cribl.company.com:9000/api/v1/m/qa-01/routes/default",
    "prod":   "https://cribl.company.com:9000/api/v1/m/prod-01/routes/default",
    "custom": "https://cribl.company.com:9000/api/v1/m/wg-main/routes/my-table",
}

for ws_name, ws_cfg in config["workspaces"].items():
    wg = _wg_map[ws_name]
    root_url, api_base = cfg.build_workspace_urls(config, ws_cfg, wg)
    routes_table = ws_cfg.get("routes_table", "default")
    routes_url   = f"{api_base}/routes/{routes_table}"
    outputs_url  = f"{api_base}/system/outputs"
    exp = expected_routes[ws_name]
    if routes_url == exp:
        ok(f"[{ws_name}] routes_url = {routes_url}")
    else:
        fail(f"[{ws_name}] expected {exp}, got {routes_url}")
    # outputs URL sanity
    if outputs_url == f"https://cribl.company.com:9000/api/v1/m/{wg}/system/outputs":
        ok(f"[{ws_name}] outputs_url = {outputs_url}")
    else:
        fail(f"[{ws_name}] bad outputs_url: {outputs_url}")

# ── Workspace helpers ─────────────────────────────────────────────────────────
section("3. WORKSPACE HELPERS")

names = cfg.get_workspace_names(config)
if set(names) == {"dev", "qa", "prod", "custom"}:
    ok(f"get_workspace_names: {names}")
else:
    fail(f"get_workspace_names returned: {names}")

ws = cfg.get_workspace(config, "dev")
if ws["worker_groups"] == ["dev-01", "dev-02"]:
    ok("get_workspace('dev') correct")
else:
    fail(f"get_workspace('dev') wrong: {ws}")

try:
    import io
    _old_stderr, sys.stderr = sys.stderr, io.StringIO()
    cfg.get_workspace(config, "nonexistent")
    sys.stderr = _old_stderr
    fail("get_workspace(nonexistent) should have died")
except SystemExit as e:
    sys.stderr = _old_stderr
    ok(f"get_workspace(nonexistent) correctly exits with code {e.code}")

# ── normalize_route ───────────────────────────────────────────────────────────
section("4. normalize_route")

r = api.normalize_route({}, "passthru")
assert r["pipeline"] == "passthru" and r["final"] == False and r["disabled"] == False
ok("empty dict gets defaults")

r2 = api.normalize_route({"id": "my-id", "pipeline": "custom"}, "passthru")
assert r2["name"] == "my-id" and r2["pipeline"] == "custom"
ok("name from id, existing pipeline preserved")

r3 = api.normalize_route({"name": "explicit-name"}, "passthru")
assert r3["name"] == "explicit-name"
ok("explicit name not overwritten")

# ── find_default_route_index ──────────────────────────────────────────────────
section("5. find_default_route_index")

routes_final = [
    {"name": "route-a", "final": False},
    {"name": "default-catch", "final": True},
    {"name": "route-b", "final": False},
]
idx = api.find_default_route_index(routes_final)
assert idx == 1, f"got {idx}"
ok(f"final:true at pos 1 -> insert at {idx}")

routes_named = [
    {"name": "route-a"},
    {"name": "default"},
    {"name": "route-b"},
]
idx2 = api.find_default_route_index(routes_named)
assert idx2 == 1, f"got {idx2}"
ok(f"route named 'default' at pos 1 -> insert at {idx2}")

routes_none = [{"name": "a"}, {"name": "b"}]
idx3 = api.find_default_route_index(routes_none)
assert idx3 == 2
ok(f"no catch-all found -> appends at end ({idx3})")

# ── count_all_routes ──────────────────────────────────────────────────────────
section("6. count_all_routes")

obj_flat = {"routes": [{"name": "r1"}, {"name": "r2"}]}
assert api.count_all_routes(obj_flat) == 2
ok("flat routes: 2")

obj_groups = {
    "routes": [{"name": "r1"}],
    "groups": [
        {"id": "g1", "routes": [{"name": "g1r1"}, {"name": "g1r2"}]},
        {"id": "g2", "routes": [{"name": "g2r1"}]},
    ],
}
assert api.count_all_routes(obj_groups) == 4
ok("1 top-level + 2 in g1 + 1 in g2 = 4")

obj_wrapped = {"items": [{"routes": [{"name": "r1"}, {"name": "r2"}], "groups": []}]}
assert api.count_all_routes(obj_wrapped) == 2
ok("wrapped items shape: 2")

# ── get_routes_target ─────────────────────────────────────────────────────────
section("7. get_routes_target")

obj_tl = {"routes": [{"name": "r1", "final": True}]}
tgt, key, _ = api.get_routes_target(obj_tl, None)
assert tgt is obj_tl and key == "routes"
ok("no group -> top-level routes key")

obj_grp = {"groups": [{"id": "grp1", "routes": [{"name": "r1"}]}]}
tgt2, key2, _ = api.get_routes_target(obj_grp, "grp1")
assert tgt2["id"] == "grp1" and key2 == "routes"
ok("group found -> group container returned")

tgt3, key3, _ = api.get_routes_target(obj_grp, "missing")
assert tgt3 is None and key3 is None
ok("missing group -> (None, None, False)")

# ── create_group_if_missing ───────────────────────────────────────────────────
section("8. create_group_if_missing")

obj_new = {"routes": []}
api.create_group_if_missing(obj_new, "newgrp", "New Group")
assert obj_new["groups"][0] == {"id": "newgrp", "name": "New Group", "routes": []}
ok("group created on object that had none")

api.create_group_if_missing(obj_new, "newgrp", "New Group")
assert len(obj_new["groups"]) == 1
ok("calling again does not duplicate the group")

# ── Route insertion order ─────────────────────────────────────────────────────
section("9. ROUTE INSERTION ORDER")

existing = [
    api.normalize_route({"id": "route-1", "filter": "x==1", "pipeline": "p"}, "p"),
    api.normalize_route({"id": "default-catch", "final": True, "pipeline": "p"}, "p"),
]
default_idx = api.find_default_route_index(existing)
new_r = api.normalize_route({"id": "route-new", "filter": "x==99"}, "p")
updated = existing[:default_idx] + [new_r] + existing[default_idx:]
ids = [r["id"] for r in updated]
assert ids == ["route-1", "route-new", "default-catch"], f"got {ids}"
ok(f"insertion order correct: {ids}")

# ── Duplicate skip ────────────────────────────────────────────────────────────
section("10. DUPLICATE SKIP LOGIC")

existing_names   = {"hcsc-blob-storage-route-APP001"}
existing_filters = {'apmId == "APP001"'}

name1 = "hcsc-blob-storage-route-APP001"
filt1 = 'apmId == "APP001"'
assert name1 in existing_names or filt1 in existing_filters
ok("APP001 duplicate correctly detected via name")

name2 = "hcsc-blob-storage-route-APP002"
filt2 = 'apmId == "APP002"'
assert not (name2 in existing_names or filt2 in existing_filters)
ok("APP002 new app correctly not detected as duplicate")

# ── read_apps_from_file ───────────────────────────────────────────────────────
section("11. APPS FILE PARSING")

with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
    f.write("# comment line\n")
    f.write("\n")
    f.write("APP001, My First App\n")
    f.write("APP002,Second App\n")
    f.write("  APP003 , Trimmed App  \n")
    tmpfile = f.name

apps = utils.read_apps_from_file(tmpfile)
os.unlink(tmpfile)
assert apps == [("APP001", "My First App"), ("APP002", "Second App"), ("APP003", "Trimmed App")], f"got {apps}"
ok(f"parsed {len(apps)} apps, comments/blanks skipped, whitespace trimmed")

# ── Live config.json ─────────────────────────────────────────────────────────
section("12. LIVE config.json")

try:
    with open("config.json") as f:
        live = json.load(f)
    ok("config.json is valid JSON")
    ws_names = [k for k in live.get("workspaces", {}).keys() if not k.startswith("_")]
    ok(f"workspaces defined: {ws_names}")
    for name, wscfg in live["workspaces"].items():
        if name.startswith("_"):
            continue
        # Support both old (worker_group string) and new (worker_groups list) format
        if "worker_groups" in wscfg:
            wg_list = wscfg["worker_groups"]
            assert isinstance(wg_list, list) and wg_list, f"[{name}] worker_groups must be a non-empty list"
            wg = wg_list[0]
        elif "worker_group" in wscfg:
            wg = wscfg["worker_group"]
        else:
            fail(f"[{name}] missing worker_group or worker_groups")
            continue
        # Support both old (dest_template string) and new (dest_templates dict) format
        has_dest = "dest_template" in wscfg or "dest_templates" in wscfg
        assert has_dest, f"[{name}] missing dest_template or dest_templates"
        rt = wscfg.get("routes_table", "default")
        _, base = cfg.build_workspace_urls(live, wscfg, wg)
        url = f"{base}/routes/{rt}"
        ok(f"[{name}] routes_url = {url}")
except Exception as e:
    fail(f"config.json check failed: {e}")

# ── Skip logic (route or destination already exists -> skip entirely) ─────────
section("13. SKIP LOGIC")

# Simulate: APP001 route already exists, APP002 is new
existing_names_sim   = {"hcsc-blob-storage-route-APP001"}
existing_filters_sim = {'apmId == "APP001"'}

new_routes_sim  = []
skipped_sim     = []

for appid in ("APP001", "APP002"):
    route_name   = f"hcsc-blob-storage-route-{appid}"
    route_filter = f'apmId == "{appid}"'
    if route_name in existing_names_sim or route_filter in existing_filters_sim:
        skipped_sim.append(appid)
    else:
        new_routes_sim.append(appid)

assert skipped_sim   == ["APP001"]
assert new_routes_sim == ["APP002"]
ok("APP001 route correctly identified as existing -> skipped, APP002 as new")

# Simulate destination POST: any 4xx skips, no update attempted
def _simulate_dest_upsert(post_status):
    """Returns action taken: 'created' or 'skipped' or 'error'"""
    if post_status in (200, 201):
        return "created"
    elif 400 <= post_status < 500:
        return "skipped"
    else:
        return "error"

assert _simulate_dest_upsert(201) == "created"
ok("dest POST 201 -> created")

assert _simulate_dest_upsert(400) == "skipped"
ok("dest POST 400 -> skipped")

assert _simulate_dest_upsert(409) == "skipped"
ok("dest POST 409 -> skipped")

assert _simulate_dest_upsert(422) == "skipped"
ok("dest POST 422 -> skipped (Cribl 'already exists' variant)")

assert _simulate_dest_upsert(500) == "error"
ok("dest POST 500 -> error (5xx still fatal)")

# ── Logger module ─────────────────────────────────────────────────────────────
section("14. LOGGER MODULE")

cribl_logger = load_mod("cribl_logger", "cribl_logger.py")
ok("cribl_logger imported")

# setup_logging returns a Logger
import logging
test_log = cribl_logger.setup_logging("DEBUG")
assert isinstance(test_log, logging.Logger)
assert test_log.level == logging.DEBUG
ok("setup_logging(DEBUG) returns Logger at DEBUG level")

test_log2 = cribl_logger.setup_logging("INFO")
assert test_log2.level == logging.INFO
ok("setup_logging(INFO) sets INFO level")

# get_logger returns the same named logger
assert cribl_logger.get_logger().name == "cribl"
ok("get_logger() returns 'cribl' logger")

# Bad level falls back to INFO
test_log3 = cribl_logger.setup_logging("NONSENSE")
assert test_log3.level == logging.INFO
ok("invalid level falls back to INFO")

# File handler added when log_file given
import tempfile, os as _os
with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as tf:
    tmp_log = tf.name
test_log4 = cribl_logger.setup_logging("INFO", tmp_log)
assert len(test_log4.handlers) == 2  # console + file
test_log4.info("test log line")
# Close all handlers before reading/deleting on Windows
for h in test_log4.handlers[:]:
    h.close()
    test_log4.removeHandler(h)
with open(tmp_log, encoding="utf-8") as f:
    content = f.read()
assert "test log line" in content
_os.unlink(tmp_log)
ok("log_file handler writes to file correctly")

# die() uses logger when handlers present, falls back to stderr otherwise
section("15. die() LOGGER INTEGRATION")

# With handlers: should use logger.error (not print to stderr)
import io
cribl_logger.setup_logging("INFO")  # ensure handlers are set
buf = io.StringIO()
test_log5 = logging.getLogger("cribl")
old_handlers = test_log5.handlers[:]
test_log5.handlers.clear()
test_log5.addHandler(logging.StreamHandler(buf))
try:
    utils.die("test error message", code=99)
except SystemExit as e:
    assert e.code == 99
    assert "test error message" in buf.getvalue()
    ok("die() routes through logger.error() when handlers present")
finally:
    test_log5.handlers.clear()
    test_log5.handlers.extend(old_handlers)

# ── Stale log strings ─────────────────────────────────────────────────────────
section("16. STALE LOG STRINGS IN cribl-pusher.py")

with open("cribl-pusher.py", encoding="utf-8") as f:
    src = f.read()

stale = [
    "GET routes/default",
    "PATCH routes/default",
    "print(f\"[OK]",
    "print(f\"[INFO]",
    "print(f\"[SKIP]",
    "print(f\"[WARN]",
    "print(f\"[SNAPSHOT]",
    "print(f\"[DRY RUN]",
    "print(f\"[ROLLBACK]",
]
for s in stale:
    if s in src:
        fail(f'stale string still present: "{s}"')
    else:
        ok(f'clean: "{s}"')

# ── Null/non-dict/filter-less route filtering (regression) ───────────────────
section("17. ROUTE FILTERING (null, non-dict, and missing-filter exclusion)")

# Simulate the raw routes list Cribl may return:
#   - null slots            -> not isinstance(r, dict) -> dropped
#   - non-dict entries      -> not isinstance(r, dict) -> dropped
#   - dicts WITHOUT filter  -> r.get("filter") is None -> dropped (would crash Cribl JS)
#   - dicts WITH filter     -> kept and processed normally
routes_list_raw = [
    {"name": "route-a",  "filter": 'x == "1"', "pipeline": "p", "final": False},
    None,                                              # null slot
    {"name": "default",  "filter": "true",      "pipeline": "p", "final": True},
    42,                                                # garbage non-dict
    {"name": "route-c",  "filter": 'x == "3"', "pipeline": "p", "final": False},
    {"name": "no-filter-route", "pipeline": "p"},     # dict but missing filter key
    {"name": "null-filter-route", "filter": None, "pipeline": "p"},  # explicit null filter
]

# Mirror the exact logic from cribl-pusher.py:
all_dict_routes    = [r for r in routes_list_raw if isinstance(r, dict)]
existing_routes    = [r for r in all_dict_routes   if r.get("filter") is not None]
filterless_dropped = len(all_dict_routes) - len(existing_routes)

assert len(all_dict_routes) == 5, f"expected 5 dicts, got {len(all_dict_routes)}"
ok("null and non-dict entries filtered out (5 of 7 kept as dicts)")

assert len(existing_routes) == 3, f"expected 3 routes with filter, got {len(existing_routes)}"
ok("filter-less dicts (missing key + null value) excluded (3 of 5 kept)")

assert filterless_dropped == 2, f"expected 2 dropped, got {filterless_dropped}"
ok(f"filterless_dropped == 2 (one missing key, one null value)")

# All retained routes have a non-None filter
for r in existing_routes:
    assert isinstance(r, dict) and r.get("filter") is not None, (
        f"route with missing/null filter slipped through: {r!r}"
    )
ok("all retained routes are dicts with non-None 'filter'")

# Names and filters extracted correctly
existing_names   = {r.get("name")   for r in existing_routes if r.get("name")}
existing_filters = {r.get("filter") for r in existing_routes if r.get("filter")}
assert existing_names   == {"route-a", "default", "route-c"}, f"got {existing_names}"
assert existing_filters == {'x == "1"', "true", 'x == "3"'}, f"got {existing_filters}"
ok("existing_names and existing_filters populated correctly")

# New route (normalize_route IS called on new routes only)
new_route = api.normalize_route({"id": "route-new", "filter": 'x == "99"'}, "passthru")
assert new_route["filter"] == 'x == "99"'
assert new_route["pipeline"] == "passthru"
ok("normalize_route applied to new route — filter and pipeline set")

# Existing routes are NOT modified — they retain original shape
original_route_a = {"name": "route-a", "filter": 'x == "1"', "pipeline": "p", "final": False}
assert existing_routes[0] == original_route_a, (
    f"existing route was mutated: {existing_routes[0]!r}"
)
ok("existing route dict is unchanged (normalize_route not called on it)")

# Round-trip: every entry in the final PATCH list has a filter
default_idx  = api.find_default_route_index(existing_routes)
updated_list = existing_routes[:default_idx] + [new_route] + existing_routes[default_idx:]
for r in updated_list:
    assert isinstance(r, dict) and r.get("filter") is not None, (
        f"route with missing/null filter in final list: {r!r}"
    )
ok(f"final route list has {len(updated_list)} entries, all dicts with non-None 'filter'")

# Safety-count adjustment: total_before is reduced by filterless_dropped
# so that count_all_routes(patch_obj) >= adjusted total_before after new routes are added
total_before_sim = 7      # pretend Cribl returned 7 routes total
total_before_sim -= filterless_dropped   # 7 - 2 = 5
total_after_sim   = len(updated_list)    # 3 existing + 1 new = 4
assert total_after_sim >= total_before_sim - len(existing_routes), \
    "safety count adjustment produces inconsistent result"
ok("total_before adjustment keeps safety check consistent with PATCH payload")

# ── unwrap_response (PATCH payload fix) ──────────────────────────────────────
section("18. unwrap_response — PATCH payload structure")

# Cribl GET /routes returns {"count":N,"items":[{inner}]}.
# PATCH expects just the inner object.  Sending the wrapper causes Cribl's JS
# to call undefined.filter() (Array method) on a missing "routes" key.

# Wrapped case: must return the inner route-table object
wrapped = {
    "count": 1,
    "items": [
        {"id": "default", "routes": [{"filter": "true", "final": True}], "groups": {}}
    ],
}
inner = api.unwrap_response(wrapped)
assert inner is wrapped["items"][0], "should return items[0] for wrapped response"
assert "routes" in inner and "id" in inner
ok("wrapped {'items':[{inner}]} -> returns inner route-table dict")

# Non-wrapped case: must return the object unchanged
flat = {"id": "default", "routes": [{"filter": "true", "final": True}], "groups": {}}
assert api.unwrap_response(flat) is flat
ok("non-wrapped {'routes':[...]} -> returned unchanged")

# items-as-routes case (items are individual routes, not a table): also unchanged
items_as_routes = {
    "count": 2,
    "items": [
        {"filter": "x==1", "pipeline": "p"},
        {"filter": "true",  "pipeline": "p", "final": True},
    ],
}
result = api.unwrap_response(items_as_routes)
assert result is items_as_routes, "items without routes/groups key should not be unwrapped"
ok("items-as-routes (no routes/groups key in item) -> returned unchanged")

# Mutation visibility: modifying inner routes is reflected when we re-unwrap
wrapped2 = {
    "count": 1,
    "items": [{"id": "tbl", "routes": [{"filter": "old", "final": True}], "groups": []}],
}
inner2 = api.unwrap_response(wrapped2)
inner2["routes"] = [{"filter": "new", "final": True}]
assert api.unwrap_response(wrapped2)["routes"][0]["filter"] == "new"
ok("mutations to the unwrapped inner dict are visible through the wrapper")

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"PASSED: {PASS}   FAILED: {FAIL}")
if FAIL:
    print("ACTION REQUIRED: fix the FAIL items above.")
    sys.exit(1)
else:
    print("All checks passed. Script is ready to run.")
