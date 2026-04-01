#!/usr/bin/env python3
"""
Config loader for cribl-pusher.
Reads config.json and resolves workspace URLs and credentials.
"""
import os

from cribl_utils import die, read_json


def load_config(config_path: str = "config.json") -> dict:
    if not os.path.exists(config_path):
        die(
            f"[ERR] Config file not found: {config_path}\n"
            f"      Copy config.example.json to config.json and fill in your values."
        )
    return read_json(config_path)


def get_workspace_names(config: dict) -> list[str]:
    return list(config.get("workspaces", {}).keys())


def get_workspace(config: dict, name: str) -> dict:
    workspaces = config.get("workspaces", {})
    if name not in workspaces:
        die(
            f"[ERR] Workspace '{name}' not found in config.\n"
            f"      Available: {list(workspaces.keys())}"
        )
    return workspaces[name]


def get_worker_groups(workspace_cfg: dict) -> list[str]:
    """Returns the list of worker groups defined for a workspace."""
    groups = workspace_cfg.get("worker_groups", [])
    if not groups:
        die("[ERR] No worker_groups defined for this workspace in config")
    return groups


def build_workspace_urls(config: dict, workspace_cfg: dict, worker_group: str) -> tuple[str, str]:
    """
    Returns (root_url, api_base_url) for the workspace + selected worker group.

    root_url     — used for /api/v1/auth/login
    api_base_url — used for all other API calls, scoped to the worker group:
                   {base_url}/api/v1/m/{worker_group}

    Workspace-level base_url overrides the global one, allowing different
    workspaces to point to different Cribl clusters.
    """
    root_url = workspace_cfg.get("base_url", config.get("base_url", "")).rstrip("/")
    api_base = f"{root_url}/api/v1/m/{worker_group}"
    return root_url, api_base


def get_cribl_urls(config: dict) -> list[str]:
    return [u.rstrip("/") for u in config.get("cribl_urls", []) if u]


def get_route_template_path(config: dict, workspace_cfg: dict, region: str) -> str:
    """
    Returns the route template path to use.
    Priority: workspace-level single override > workspace route_templates[region] map > top-level route_templates[region].
    """
    override = workspace_cfg.get("route_template", "")
    if override:
        return override
    # workspace-level per-region map (e.g. {"azn": "route_template_azn_dev.json"})
    templates = workspace_cfg.get("route_templates", {})
    path = templates.get(region, "")
    if path:
        return path
    # fall back to top-level map
    templates = config.get("route_templates", {})
    path = templates.get(region, "")
    if not path:
        die(f"[ERR] No route_template defined for region '{region}' in workspace or config route_templates")
    return path


def get_dest_template_path(config: dict, workspace_cfg: dict, region: str) -> str:
    """
    Returns the dest template path to use.
    Priority: workspace-level single override > workspace dest_templates[region] map.
    """
    override = workspace_cfg.get("dest_template", "")
    if override:
        return override
    templates = workspace_cfg.get("dest_templates", {})
    path = templates.get(region, "")
    if not path:
        die(f"[ERR] No dest_template defined for region '{region}' in workspace config")
    return path


def get_dest_prefix(config: dict, workspace_cfg: dict, region: str) -> str:
    """
    Returns the destination ID prefix for the given region.
    Falls back to a workspace-level override if set.
    """
    override = workspace_cfg.get("dest_prefix", "")
    if override:
        return override
    prefixes = config.get("dest_prefixes", {})
    return prefixes.get(region, f"hcsc-blob-storage-{region}")


def get_workspace_url(config: dict, workspace_cfg: dict) -> str:
    """Returns the effective base_url for a workspace (for display purposes)."""
    return workspace_cfg.get("base_url", config.get("base_url", "")).rstrip("/")


def resolve_credentials(config: dict, args) -> tuple[str, str, str]:
    """
    Returns (token, username, password).

    Priority order (highest first):
      1. CLI arg  (--token / --username / --password)
      2. Environment variable (CRIBL_TOKEN / CRIBL_USERNAME / CRIBL_PASSWORD)
      3. config.json credentials block
    """
    creds = config.get("credentials", {})

    token = (
        getattr(args, "token", "") or
        os.environ.get("CRIBL_TOKEN", "") or
        creds.get("token", "")
    ).strip()

    username = (
        getattr(args, "username", "") or
        os.environ.get("CRIBL_USERNAME", "") or
        creds.get("username", "")
    ).strip()

    password = (
        getattr(args, "password", "") or
        os.environ.get("CRIBL_PASSWORD", "") or
        creds.get("password", "")
    ).strip()

    return token, username, password
