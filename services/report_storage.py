"""
services/report_storage.py
Report storage helpers:
- Primary storage: PostgreSQL JSONB (when DATABASE_URL is configured)
- Legacy fallback: local files in html_data/
"""
import glob
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from fastapi import HTTPException

from core.config import Config


_POSTGRES_SCHEMA_READY = False


# Filename helpers -------------------------------------------------------------

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
        f"/api/html-report/{employee_id}",
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


# Shared helpers ---------------------------------------------------------------

def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _storage_uses_postgres() -> bool:
    return bool((Config.DATABASE_URL or "").strip())


def storage_backend_name() -> str:
    return "postgresql" if _storage_uses_postgres() else "filesystem"


def _build_not_found_detail(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
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
    return detail


def _raise_not_found(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> None:
    raise HTTPException(
        status_code=404,
        detail=_build_not_found_detail(employee_id, submission_id=submission_id, cycle_name=cycle_name),
    )


# PostgreSQL helpers -----------------------------------------------------------

def _import_psycopg():
    try:
        import psycopg  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "DATABASE_URL is set but dependency 'psycopg[binary]' is not installed."
        ) from exc
    return psycopg


def _ensure_postgres_schema() -> None:
    global _POSTGRES_SCHEMA_READY
    if _POSTGRES_SCHEMA_READY or not _storage_uses_postgres():
        return

    psycopg = _import_psycopg()
    with psycopg.connect(Config.DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS employee_reports (
                    id BIGSERIAL PRIMARY KEY,
                    employee_id TEXT NOT NULL,
                    submission_id TEXT NOT NULL DEFAULT '',
                    cycle_name TEXT NOT NULL DEFAULT '',
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (employee_id, submission_id, cycle_name)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_employee_reports_employee_updated
                ON employee_reports (employee_id, updated_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_employee_reports_payload_gin
                ON employee_reports USING GIN (payload)
                """
            )
    _POSTGRES_SCHEMA_READY = True


def initialize_report_storage() -> None:
    """
    Initialize report storage backend.
    - PostgreSQL mode: ensures table/indexes exist.
    - Filesystem mode: ensures html_data directory exists.
    """
    if _storage_uses_postgres():
        _ensure_postgres_schema()
    else:
        os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)


def _payload_from_row(row: Optional[Tuple[Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    payload = row[0]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if isinstance(payload, dict):
        return payload
    return None


def _matches_header_identity(
    payload: Dict[str, Any],
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> bool:
    header = payload.get("header", {})
    if not isinstance(header, dict):
        return False
    if submission_id and _normalize_optional_str(header.get("submission_id")) != submission_id:
        return False
    if cycle_name and _normalize_optional_str(header.get("cycle_name")) != cycle_name:
        return False
    return True


def _resolve_identity(
    json_payload: Dict[str, Any],
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Tuple[str, str]:
    header = json_payload.get("header", {})
    if not isinstance(header, dict):
        header = {}
    resolved_submission = _normalize_optional_str(submission_id) or _normalize_optional_str(header.get("submission_id")) or ""
    resolved_cycle = _normalize_optional_str(cycle_name) or _normalize_optional_str(header.get("cycle_name")) or ""
    return resolved_submission, resolved_cycle


def _save_employee_json_db(
    json_payload: Dict[str, Any],
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    _ensure_postgres_schema()
    psycopg = _import_psycopg()

    normalized_submission, normalized_cycle = _resolve_identity(
        json_payload,
        submission_id=submission_id,
        cycle_name=cycle_name,
    )
    payload_text = json.dumps(json_payload, ensure_ascii=False)

    with psycopg.connect(Config.DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO employee_reports (employee_id, submission_id, cycle_name, payload)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (employee_id, submission_id, cycle_name)
                DO UPDATE
                SET payload = EXCLUDED.payload,
                    updated_at = NOW()
                """,
                (employee_id, normalized_submission, normalized_cycle, payload_text),
            )

    return (
        "postgresql://employee_reports/"
        f"{quote(employee_id, safe='')}?submission_id={quote(normalized_submission, safe='')}"
        f"&cycle_name={quote(normalized_cycle, safe='')}"
    )


def _load_employee_json_db(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    _ensure_postgres_schema()
    psycopg = _import_psycopg()

    normalized_submission = _normalize_optional_str(submission_id)
    normalized_cycle = _normalize_optional_str(cycle_name)

    with psycopg.connect(Config.DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Exact identity match first.
            if normalized_submission or normalized_cycle:
                if normalized_submission and normalized_cycle:
                    cur.execute(
                        """
                        SELECT payload
                        FROM employee_reports
                        WHERE employee_id = %s
                          AND submission_id = %s
                          AND cycle_name = %s
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (employee_id, normalized_submission, normalized_cycle),
                    )
                elif normalized_submission:
                    cur.execute(
                        """
                        SELECT payload
                        FROM employee_reports
                        WHERE employee_id = %s
                          AND submission_id = %s
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (employee_id, normalized_submission),
                    )
                else:
                    cur.execute(
                        """
                        SELECT payload
                        FROM employee_reports
                        WHERE employee_id = %s
                          AND cycle_name = %s
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (employee_id, normalized_cycle),
                    )
                payload = _payload_from_row(cur.fetchone())
                if payload:
                    return payload

            # Latest row for employee (same fallback behavior as legacy file loader).
            cur.execute(
                """
                SELECT payload
                FROM employee_reports
                WHERE employee_id = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (employee_id,),
            )
            latest = _payload_from_row(cur.fetchone())
            if latest and _matches_header_identity(
                latest,
                submission_id=normalized_submission,
                cycle_name=normalized_cycle,
            ):
                return latest

            # Header-based fallback for records where identity fields were missing/legacy.
            cur.execute(
                """
                SELECT payload
                FROM employee_reports
                WHERE employee_id = %s
                ORDER BY updated_at DESC
                LIMIT 200
                """,
                (employee_id,),
            )
            for row in cur.fetchall():
                payload = _payload_from_row(row)
                if not payload:
                    continue
                if _matches_header_identity(
                    payload,
                    submission_id=normalized_submission,
                    cycle_name=normalized_cycle,
                ):
                    return payload

    return None


# Filesystem helpers -----------------------------------------------------------

def _save_employee_json_filesystem(
    json_payload: Dict[str, Any],
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)
    normalized_submission, normalized_cycle = _resolve_identity(
        json_payload,
        submission_id=submission_id,
        cycle_name=cycle_name,
    )
    path = build_employee_report_path(
        employee_id,
        submission_id=normalized_submission or None,
        cycle_name=normalized_cycle or None,
    )
    with open(path, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2)
    return path


def _load_employee_json_filesystem(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
    direct_path = build_employee_report_path(
        employee_id,
        submission_id=submission_id,
        cycle_name=cycle_name,
    )
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
        if not isinstance(payload, dict):
            continue
        if _matches_header_identity(payload, submission_id=submission_id, cycle_name=cycle_name):
            return payload

    # Glob fallback
    employee_token = sanitize_filename_token(employee_id, "employee")
    pattern = os.path.join(Config.HTML_DATA_DIR, f"{employee_token}__*.json")
    matches = [p for p in glob.glob(pattern) if os.path.isfile(p)]

    if submission_id or cycle_name:
        for path in sorted(matches, key=os.path.getmtime, reverse=True):
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                continue
            if _matches_header_identity(payload, submission_id=submission_id, cycle_name=cycle_name):
                return payload
    elif matches:
        latest = max(matches, key=os.path.getmtime)
        with open(latest, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload

    _raise_not_found(employee_id, submission_id=submission_id, cycle_name=cycle_name)


# Public load/save -------------------------------------------------------------

def save_employee_json(
    json_payload: Dict[str, Any],
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    if _storage_uses_postgres():
        return _save_employee_json_db(
            json_payload,
            employee_id,
            submission_id=submission_id,
            cycle_name=cycle_name,
        )
    return _save_employee_json_filesystem(
        json_payload,
        employee_id,
        submission_id=submission_id,
        cycle_name=cycle_name,
    )


def load_employee_json(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
    if _storage_uses_postgres():
        try:
            payload = _load_employee_json_db(
                employee_id,
                submission_id=submission_id,
                cycle_name=cycle_name,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "Report database is unavailable.",
                    "action": "Check PostgreSQL connectivity and DATABASE_URL.",
                },
            ) from exc

        if payload:
            return payload

        # One-time migration fallback: if a legacy file exists, return it and
        # backfill into PostgreSQL for next reads.
        try:
            legacy_payload = _load_employee_json_filesystem(
                employee_id,
                submission_id=submission_id,
                cycle_name=cycle_name,
            )
            try:
                _save_employee_json_db(
                    legacy_payload,
                    employee_id,
                    submission_id=submission_id,
                    cycle_name=cycle_name,
                )
            except Exception as backfill_exc:
                print(f"Warning: PostgreSQL backfill failed for '{employee_id}': {backfill_exc}")
            return legacy_payload
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            _raise_not_found(employee_id, submission_id=submission_id, cycle_name=cycle_name)

    return _load_employee_json_filesystem(
        employee_id,
        submission_id=submission_id,
        cycle_name=cycle_name,
    )


def cleanup_old_per_type_files() -> None:
    """
    Remove legacy per-type files (for example: employee_employee.json)
    superseded by combined payload files.
    """
    if not os.path.isdir(Config.HTML_DATA_DIR):
        return
    for rtype in ("employee", "boss", "team", "organization"):
        for old_file in glob.glob(os.path.join(Config.HTML_DATA_DIR, f"*_{rtype}.json")):
            base = os.path.basename(old_file)
            emp_id = base.replace(f"_{rtype}.json", "")
            if os.path.exists(os.path.join(Config.HTML_DATA_DIR, f"{emp_id}.json")):
                os.remove(old_file)
                print(f"Cleaned up old file: {old_file}")
