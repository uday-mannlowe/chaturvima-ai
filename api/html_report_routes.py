"""
api/html_report_routes.py
HTML and PDF report endpoints — all /html-report/* routes.
"""
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, Response

from core.config import Config
from services.report_renderer import render_html_report, render_pdf_from_html
from services.report_storage import (
    append_identity_query,
    build_auto_download_pdf_url,
    load_employee_json,
    sanitize_filename_token,
)

router = APIRouter(tags=["HTML Reports"])

# ─── Dimension view configuration ─────────────────────────────────────────────

_DIMENSION_VIEW_CONFIG: Dict[str, Dict[str, Any]] = {
    "1d": {
        "report_types": ["employee"],
        "heading": "Employee Personal Insights",
        "dimension_label": "1D - Individual Assessment",
    },
    "2d": {
        "report_types": ["boss"],
        "heading": "Employee-Boss Relationship Insights",
        "dimension_label": "2D - Employee-Boss Relationship",
    },
    "3d": {
        "report_types": ["team"],
        "heading": "Employee-Boss-Department Context Insights",
        "dimension_label": "3D - Employee-Boss-Department Context",
    },
    "4d": {
        "report_types": ["organization"],
        "heading": "Organisational Insights",
        "dimension_label": "4D - Organisational Assessment",
    },
}

_REPORT_TYPE_TO_DIMENSION = {
    "employee": "1d", "boss": "2d", "team": "3d", "organization": "4d",
}

_DIMENSION_PRIORITY = {"1d": 1, "2d": 2, "3d": 3, "4d": 4}


# ─── Internal helpers ──────────────────────────────────────────────────────────

def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _required_query_submission_id(value: str) -> str:
    normalized = _normalize_optional_str(value)
    if not normalized:
        raise HTTPException(
            status_code=400,
            detail="'submission_id' query parameter is required and cannot be empty.",
        )
    return normalized


def _extract_dimension_code(value: Any) -> Optional[str]:
    text = _normalize_optional_str(value)
    if not text:
        return None
    m = re.search(r"\b([1-4]D)\b", text.upper())
    return m.group(1).upper() if m else None


def _infer_dimension_key_from_payload(json_payload: Dict[str, Any]) -> str:
    header = json_payload.get("header", {})
    header_dim = _extract_dimension_code(
        header.get("dimension_label") or header.get("report_type")
    )
    if header_dim:
        dim_key = header_dim.lower()
        if dim_key in _DIMENSION_VIEW_CONFIG:
            required_types = set(_DIMENSION_VIEW_CONFIG[dim_key]["report_types"])
            if any(
                str(rep.get("report_type", "")).strip().lower() in required_types
                for rep in json_payload.get("reports", [])
                if isinstance(rep, dict)
            ):
                return dim_key

    available_dims: List[str] = []
    for rep in json_payload.get("reports", []):
        if not isinstance(rep, dict):
            continue
        mapped = _REPORT_TYPE_TO_DIMENSION.get(str(rep.get("report_type", "")).strip().lower())
        if mapped:
            available_dims.append(mapped)
    if available_dims:
        return max(available_dims, key=lambda d: _DIMENSION_PRIORITY.get(d, 0))
    return "1d"


def _filter_reports_for_dimension(
    json_payload: Dict[str, Any], dimension_key: str
) -> Dict[str, Any]:
    view = _DIMENSION_VIEW_CONFIG.get(dimension_key.lower())
    if not view:
        return json_payload

    filtered = {k: v for k, v in json_payload.items() if k != "reports"}
    header = dict(json_payload.get("header", {}))
    header["report_heading"]   = view["heading"]
    header["dimension_label"]  = view["dimension_label"]
    header["report_type"]      = f"{view['dimension_label']} Growth Report"
    filtered["header"]         = header
    filtered["reports"] = [
        r for r in json_payload.get("reports", [])
        if r.get("report_type") in view["report_types"]
    ]
    return filtered


def _ensure_dimension_available(
    employee_id: str,
    dimension_key: str,
    payload: Dict[str, Any],
    filtered: Dict[str, Any],
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> None:
    if filtered.get("reports"):
        return
    header = payload.get("header", {})
    available_dims: List[str] = []
    for rep in payload.get("reports", []):
        mapped = _REPORT_TYPE_TO_DIMENSION.get(str(rep.get("report_type", "")).strip().lower())
        if mapped:
            available_dims.append(mapped)

    raise HTTPException(
        status_code=404,
        detail={
            "error": f"No {dimension_key.upper()} report available.",
            "requested_dimension": dimension_key.upper(),
            "available_dimensions": sorted(set(available_dims)),
            "suggested_auto_url": append_identity_query(
                f"/html-report/{employee_id}",
                submission_id=submission_id,
                cycle_name=cycle_name,
            ),
        },
    )


def _with_download_link(
    json_payload: Dict[str, Any],
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
    augmented = dict(json_payload)
    header = dict(json_payload.get("header", {}))
    if submission_id and not _normalize_optional_str(header.get("submission_id")):
        header["submission_id"] = submission_id
    if cycle_name and not _normalize_optional_str(header.get("cycle_name")):
        header["cycle_name"] = cycle_name
    header["download_pdf_url"] = build_auto_download_pdf_url(
        employee_id,
        submission_id=submission_id or _normalize_optional_str(header.get("submission_id")),
        cycle_name=cycle_name or _normalize_optional_str(header.get("cycle_name")),
    )
    augmented["header"] = header
    return augmented


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get(
    "/html-report/{employee_id}",
    response_class=HTMLResponse,
    summary="📄 Auto Dimension HTML Report",
)
async def html_report_auto(
    employee_id: str,
    submission_id: str = Query(..., description="Submission identifier"),
    cycle_name: Optional[str] = None,
) -> HTMLResponse:
    normalized_submission = _required_query_submission_id(submission_id)
    payload = load_employee_json(employee_id, submission_id=normalized_submission, cycle_name=cycle_name)
    dim_key = _infer_dimension_key_from_payload(payload)
    payload = _with_download_link(payload, employee_id, normalized_submission, cycle_name)
    filtered = _filter_reports_for_dimension(payload, dim_key)
    _ensure_dimension_available(employee_id, dim_key, payload, filtered, normalized_submission, cycle_name)
    header = dict(filtered.get("header", {}))
    header["download_pdf_url"] = build_auto_download_pdf_url(
        employee_id,
        submission_id=normalized_submission,
        cycle_name=_normalize_optional_str(cycle_name) or _normalize_optional_str(header.get("cycle_name")),
    )
    filtered["header"] = header
    return HTMLResponse(content=render_html_report(filtered))


@router.get(
    "/html-report/{employee_id}/pdf",
    summary="📥 Auto Dimension PDF Download",
)
async def html_report_auto_pdf(
    employee_id: str,
    submission_id: str = Query(..., description="Submission identifier"),
    cycle_name: Optional[str] = None,
) -> Response:
    normalized_submission = _required_query_submission_id(submission_id)
    payload = load_employee_json(employee_id, submission_id=normalized_submission, cycle_name=cycle_name)
    dim_key = _infer_dimension_key_from_payload(payload)
    payload = _with_download_link(payload, employee_id, normalized_submission, cycle_name)
    filtered = _filter_reports_for_dimension(payload, dim_key)
    _ensure_dimension_available(employee_id, dim_key, payload, filtered, normalized_submission, cycle_name)

    html_doc = render_html_report(filtered)
    try:
        pdf_bytes = render_pdf_from_html(html_doc)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    header = filtered.get("header", {})
    identity_key = (
        normalized_submission
        or _normalize_optional_str(header.get("submission_id"))
        or _normalize_optional_str(cycle_name)
        or _normalize_optional_str(header.get("cycle_name"))
        or "latest"
    )
    safe_employee = sanitize_filename_token(employee_id, "employee")
    safe_identity = sanitize_filename_token(identity_key, "latest")
    filename = f"{safe_employee}_{dim_key}_{safe_identity}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
