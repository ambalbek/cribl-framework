#!/usr/bin/env python3
import sys
import json
import uuid
import logging
import datetime as dt
import difflib
import getpass

import requests
import urllib3


def short_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def now_stamp() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def die(msg: str, code: int = 1):
    """Log msg as ERROR then exit. Falls back to stderr if logger not set up yet."""
    logger = logging.getLogger("cribl")
    if logger.hasHandlers():
        logger.error(msg)
    else:
        print(msg, file=sys.stderr)
    raise SystemExit(code)


def pretty_json(obj) -> str:
    return json.dumps(obj, indent=2, sort_keys=True)


def unified_diff(before: str, after: str, fromfile="before", tofile="after", n=3) -> str:
    return "\n".join(difflib.unified_diff(
        before.splitlines(), after.splitlines(),
        fromfile=fromfile, tofile=tofile, lineterm="", n=n
    ))


def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_apps_from_file(path: str):
    apps = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "," not in line:
                die(f"Invalid line (expected appid,appname): {line}")
            appid, appname = [x.strip() for x in line.split(",", 1)]
            if not appid or not appname:
                die(f"Invalid line (empty appid/appname): {line}")
            apps.append((appid, appname))
    return apps


def prompt_choice(label: str, choices):
    choices_lower = [c.lower() for c in choices]
    while True:
        print(f"\n{label}:")
        for i, c in enumerate(choices, 1):
            print(f"  {i:>2}. {c}")
        val = input("Enter number or name: ").strip()
        # accept numeric shortcut
        if val.isdigit():
            idx = int(val) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        elif val.lower() in choices_lower:
            return choices[choices_lower.index(val.lower())]
        print(f"Invalid choice. Enter a number (1-{len(choices)}) or a workspace name.")


def prompt_text(label: str, default: str | None = None) -> str:
    if default is not None and default != "":
        val = input(f"{label} [{default}]: ").strip()
        return val if val else default
    return input(f"{label}: ").strip()


def prompt_password(label: str = "Password") -> str:
    return getpass.getpass(f"{label}: ")


def confirm_or_exit(prompt: str, yes_flag: bool):
    if yes_flag:
        return
    print(prompt)
    resp = input('Type "YES" to continue: ').strip()
    if resp != "YES":
        die("Aborted (confirmation not received).", 2)


def make_session(skip_ssl: bool, no_proxy: bool = False) -> requests.Session:
    if skip_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.Session()
    s.verify = not skip_ssl
    if no_proxy:
        s.proxies = {"http": "", "https": ""}
    return s
