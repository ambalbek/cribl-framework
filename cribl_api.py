#!/usr/bin/env python3
import uuid

import requests

from cribl_utils import die


def cribl_login_token(session: requests.Session, root_url: str, username: str, password: str) -> str:
    url = root_url.rstrip("/") + "/api/v1/auth/login"
    r = session.post(
        url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"username": username, "password": password},
        timeout=60,
    )
    if r.status_code != 200:
        die(f"[ERR] login failed: {r.status_code} {r.text}")
    data = r.json()
    token = data.get("token")
    if not token:
        die(f"[ERR] login response missing token: {data}")
    return token


def normalize_route(route: dict, fallback_pipeline: str) -> dict:
    if not isinstance(route, dict):
        route = {}

    if not route.get("name"):
        rid = str(route.get("id", "")).strip()
        route["name"] = rid if rid else f"route_{uuid.uuid4().hex[:8]}"

    if not route.get("pipeline"):
        route["pipeline"] = fallback_pipeline

    route.setdefault("final", False)
    route.setdefault("disabled", False)
    route.setdefault("clones", [])
    route.setdefault("description", "")
    route.setdefault("enableOutputExpression", False)
    return route


def find_default_route_index(routes: list) -> int:
    # Insert above first final:true route
    for i, r in enumerate(routes):
        if isinstance(r, dict) and r.get("final") is True:
            return i
    # Fallback: any route containing "default" in id/name
    for i, r in enumerate(routes):
        if not isinstance(r, dict):
            continue
        rid = str(r.get("id", "")).lower()
        rname = str(r.get("name", "")).lower()
        if "default" in rid or "default" in rname:
            return i
    return len(routes)


def _unwrap(obj: dict) -> dict:
    """Some responses are wrapped as {"items":[{...}]}. Return the inner dict."""
    if isinstance(obj, dict) and isinstance(obj.get("items"), list) and obj["items"]:
        if isinstance(obj["items"][0], dict) and any(k in obj["items"][0] for k in ("routes", "groups")):
            return obj["items"][0]
    return obj


# Public alias — used in cribl-pusher.py when building the PATCH payload.
# Cribl's GET /routes returns {"count":N,"items":[{inner}]} but its PATCH handler
# expects just the inner object {"id":…,"routes":[…],"groups":{…}}.
# Sending the outer wrapper causes Cribl's JS to call undefined.filter() (Array
# method) because payload.routes doesn't exist at the outer level.
unwrap_response = _unwrap


def get_routes_target(obj: dict, group_id: str | None):
    """
    Returns (target_container_dict, routes_key, group_created_bool).

    - group_id is None  -> target is the top-level routes list
    - group_id is set   -> target is that group's routes list
    """
    created = False
    root = _unwrap(obj)

    if group_id:
        for attr in ("groups", "routeGroups", "routesGroups"):
            groups = root.get(attr)
            if isinstance(groups, list):
                for g in groups:
                    if isinstance(g, dict) and str(g.get("id", "")) == group_id:
                        if not isinstance(g.get("routes"), list):
                            g["routes"] = []
                        return g, "routes", created
                return None, None, created
        return None, None, created

    # Non-group mode: plain routes list
    if isinstance(root.get("routes"), list):
        return root, "routes", created

    # items-as-routes shape (heuristic)
    if isinstance(obj.get("items"), list) and obj["items"] and isinstance(obj["items"][0], dict):
        item0 = obj["items"][0]
        if any(k in item0 for k in ("filter", "pipeline", "output", "final", "disabled", "name", "id")):
            return obj, "items", created

    die(f"Cannot locate routes array/group in GET response keys={list(obj.keys())}")


def create_group_if_missing(obj: dict, group_id: str, group_name: str | None = None) -> None:
    root = _unwrap(obj)
    groups = root.setdefault("groups", [])
    if not isinstance(groups, list):
        die("[ERR] groups exists but is not a list; cannot create group safely")

    for g in groups:
        if isinstance(g, dict) and str(g.get("id", "")) == group_id:
            if not isinstance(g.get("routes"), list):
                g["routes"] = []
            return

    groups.append({"id": group_id, "name": group_name or group_id, "routes": []})


def count_all_routes(obj: dict) -> int:
    root = _unwrap(obj)
    total = 0

    if isinstance(root.get("routes"), list):
        total += len(root["routes"])

    if isinstance(root.get("groups"), list):
        for g in root["groups"]:
            if isinstance(g, dict) and isinstance(g.get("routes"), list):
                total += len(g["routes"])

    # items-as-routes shape
    if isinstance(obj.get("items"), list) and obj["items"] and isinstance(obj["items"][0], dict):
        item0 = obj["items"][0]
        if any(k in item0 for k in ("filter", "pipeline", "output", "final", "disabled", "name", "id")):
            total += len(obj["items"])

    return total
