"""
services/report_storage.py
File-system helpers for storing and loading employee report JSON files.
"""
import glob
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import HTTPException

from core.config import Config


# ─── Filename helpers ──────────────────────────────────────────────────────────

def sanitize_filename_token(value: str, fallback: str) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "_", (value or "").strip())
    token = token.strip("._-")
    return token or fallback


def build_employee_report_path(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    employee_token = sanitize_filename_token(employee_id, "employee")
    if submission_id:
        key_token = sanitize_filename_token(submission_id, "submission")
        key_hash = hashlib.sha1(submission_id.encode("utf-8")).hexdigest()[:10]
        filename = f"{employee_token}__submission_{key_token}_{key_hash}.json"
    elif cycle_name:
        key_token = sanitize_filename_token(cycle_name, "cycle")
        key_hash = hashlib.sha1(cycle_name.encode("utf-8")).hexdigest()[:10]
        filename = f"{employee_token}__cycle_{key_token}_{key_hash}.json"
    else:
        filename = f"{employee_token}.json"
    return os.path.join(Config.HTML_DATA_DIR, filename)


def append_identity_query(
    url: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    parts = []
    if submission_id:
        parts.append(f"submission_id={quote(submission_id, safe='')}")
    if cycle_name:
        parts.append(f"cycle_name={quote(cycle_name, safe='')}")
    if not parts:
        return url
    return f"{url}?{'&'.join(parts)}"


def build_report_urls(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, str]:
    report_url = append_identity_query(
        f"/html-report/{employee_id}",
        submission_id=submission_id,
        cycle_name=cycle_name,
    )
    return {"report": report_url}


def build_auto_download_pdf_url(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    return append_identity_query(
        f"/api/html-report/{employee_id}/pdf",
        submission_id=submission_id,
        cycle_name=cycle_name,
    )


# ─── Load / save ──────────────────────────────────────────────────────────────

def save_employee_json(
    json_payload: Dict[str, Any],
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)
    path = build_employee_report_path(employee_id, submission_id=submission_id, cycle_name=cycle_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2)
    return path


def load_employee_json(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Load the stored JSON for an employee, or raise HTTP 404."""
    direct_path = build_employee_report_path(employee_id, submission_id=submission_id, cycle_name=cycle_name)
    latest_alias = build_employee_report_path(employee_id)

    candidates: List[str] = []
    if submission_id or cycle_name:
        candidates.append(direct_path)
    candidates.append(latest_alias)

    seen: set = set()
    for path in candidates:
        if path in seen or not os.path.exists(path):
            seen.add(path)
            continue
        seen.add(path)
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        header = payload.get("header", {})
        if submission_id and (header.get("submission_id") or "").strip() != submission_id:
            continue
        if cycle_name and (header.get("cycle_name") or "").strip() != cycle_name:
            continue
        return payload

    # Glob fallback
    employee_token = sanitize_filename_token(employee_id, "employee")
    pattern = os.path.join(Config.HTML_DATA_DIR, f"{employee_token}__*.json")
    matches = [p for p in glob.glob(pattern) if os.path.isfile(p)]

    if submission_id or cycle_name:
        for path in sorted(matches, key=os.path.getmtime, reverse=True):
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            header = payload.get("header", {})
            if submission_id and (header.get("submission_id") or "").strip() != submission_id:
                continue
            if cycle_name and (header.get("cycle_name") or "").strip() != cycle_name:
                continue
            return payload
    elif matches:
        latest = max(matches, key=os.path.getmtime)
        with open(latest, "r", encoding="utf-8") as f:
            return json.load(f)

    detail: Dict[str, Any] = {
        "error": f"No report found for '{employee_id}'.",
        "action": "Call POST /generate-employee-report first.",
        "body": {"employee": employee_id},
    }
    if submission_id:
        detail["error"] = f"No report found for '{employee_id}' with submission_id '{submission_id}'."
        detail["body"]["submission_id"] = submission_id
    if cycle_name:
        detail["body"]["cycle_name"] = cycle_name
    raise HTTPException(status_code=404, detail=detail)


def cleanup_old_per_type_files() -> None:
    """Remove legacy per-type files (e.g. employee_employee.json) superseded by combined files."""
    if not os.path.isdir(Config.HTML_DATA_DIR):
        return
    for rtype in ("employee", "boss", "team", "organization"):
        for old_file in glob.glob(os.path.join(Config.HTML_DATA_DIR, f"*_{rtype}.json")):
            base = os.path.basename(old_file)
            emp_id = base.replace(f"_{rtype}.json", "")
            if os.path.exists(os.path.join(Config.HTML_DATA_DIR, f"{emp_id}.json")):
                os.remove(old_file)
                print(f"🧹 Cleaned up old file: {old_file}")
