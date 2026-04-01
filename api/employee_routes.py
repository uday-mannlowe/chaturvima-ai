"""
api/employee_routes.py
Employee report generation and retrieval endpoints.
"""
import re
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Body, HTTPException, Request

from core.config import Config
from models.schemas import ReportQueue
from services.frappe_client import (
    frappe_headers,
    frappe_query_params,
    resolve_frappe_auth_token,
)
from services.report_storage import build_report_urls, load_employee_json
from generate_groq import (
    DEFAULT_REPORT_TYPE_BY_DIMENSION,
    detect_dimension,
)

router = APIRouter(tags=["Employee Report"])


def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _error_detail(
    code: str,
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
    retryable: bool = False,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "code": code,
        "message": message,
        "retryable": retryable,
    }
    if details:
        payload["details"] = details
    return payload


def _optional_payload_str(payload: Dict[str, Any], key: str) -> Optional[str]:
    if key not in payload or payload.get(key) is None:
        return None
    value = payload.get(key)
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail=f"'{key}' must be a string.")
    return value.strip() or None


def _resolve_runtime_frappe_auth(payload: Dict[str, Any], request: Request) -> Optional[str]:
    payload_api_key = _optional_payload_str(payload, "frappe_api_key")
    payload_api_secret = _optional_payload_str(payload, "frappe_api_secret")
    if bool(payload_api_key) != bool(payload_api_secret):
        raise HTTPException(
            status_code=400,
            detail="Provide both 'frappe_api_key' and 'frappe_api_secret' together.",
        )

    payload_token = (
        _optional_payload_str(payload, "frappe_auth_token")
        or _optional_payload_str(payload, "frappe_token")
    )
    if payload_token and not payload_token.lower().startswith("token "):
        raise HTTPException(
            status_code=400,
            detail="'frappe_auth_token' must start with 'token '.",
        )

<<<<<<< HEAD
    resolved = resolve_frappe_auth_token(request=request, payload=payload)
    if resolved:
        return resolved

    raise HTTPException(
        status_code=401,
        detail=(
            "Dynamic Frappe credentials are required. Send either "
            "'Authorization: token <api_key>:<api_secret>', "
            "'X-Frappe-Api-Key' + 'X-Frappe-Api-Secret' headers, "
            "or payload keys 'frappe_api_key' and 'frappe_api_secret'."
        ),
    )
=======
    return resolve_frappe_auth_token(request=request, payload=payload)
>>>>>>> 39a36c9 (hardcode  api and secret key)


def _extract_dimension_code(value: Any) -> Optional[str]:
    text = _normalize_optional_str(value)
    if not text:
        return None
    m = re.search(r"\b([1-4]D)\b", text.upper())
    return m.group(1).upper() if m else None


def _questionnaire_text_to_list(value: Any) -> List[str]:
    text = _normalize_optional_str(value)
    if not text:
        return []
    return [p.strip() for p in text.split(",") if p.strip()]


def _apply_1d_swot_override(reports_payload: Any, swot_doc: Optional[Dict[str, Any]]) -> bool:
    """Re-use the same logic from worker_pool — keeps routes thin."""
    from core.worker_pool import _apply_1d_swot_override as _apply
    return _apply(reports_payload, swot_doc)


async def _validate_assessment_exists(
    request: Request,
    employee_id: str,
    cycle_name: Optional[str],
    submission_id: Optional[str],
    frappe_auth: Optional[str] = None,
) -> None:
    params = frappe_query_params(
        employee_id,
        cycle_name=cycle_name,
        submission_id=submission_id,
    )
    common_details = {
        "employee": employee_id,
        "cycle_name": cycle_name,
        "submission_id": submission_id,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                Config.FRAPPE_BASE_URL,
                params=params,
                headers=frappe_headers(request=request, explicit_auth=frappe_auth),
            )
            resp.raise_for_status()
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=504,
                detail=_error_detail(
                    "FRAPPE_TIMEOUT",
                    "Upstream assessment service timed out.",
                    details=common_details,
                    retryable=True,
                ),
            ) from exc
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            body_snippet = (exc.response.text or "").strip().replace("\n", " ")[:300]
            if status == 404:
                raise HTTPException(
                    status_code=404,
                    detail=_error_detail(
                        "ASSESSMENT_NOT_FOUND",
                        "No assessment found for the given employee, cycle name, and submission id.",
                        details=common_details,
                        retryable=False,
                    ),
                ) from exc
            if status in (401, 403):
                raise HTTPException(
                    status_code=403,
                    detail=_error_detail(
                        "FRAPPE_AUTH_DENIED",
                        "Authorization failed while fetching assessment data.",
                        details={**common_details, "upstream_status": status},
                        retryable=False,
                    ),
                ) from exc
            raise HTTPException(
                status_code=502,
                detail=_error_detail(
                    "FRAPPE_UPSTREAM_ERROR",
                    "Upstream assessment service returned an error.",
                    details={
                        **common_details,
                        "upstream_status": status,
                        "upstream_response": body_snippet,
                    },
                    retryable=True,
                ),
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=_error_detail(
                    "FRAPPE_UNREACHABLE",
                    "Unable to reach upstream assessment service.",
                    details=common_details,
                    retryable=True,
                ),
            ) from exc

    try:
        frappe_data = resp.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=_error_detail(
                "FRAPPE_INVALID_RESPONSE",
                "Upstream assessment service returned invalid JSON.",
                details=common_details,
                retryable=True,
            ),
        ) from exc

    msg = frappe_data.get("message", frappe_data)
    if not isinstance(msg, dict):
        raise HTTPException(
            status_code=502,
            detail=_error_detail(
                "FRAPPE_INVALID_RESPONSE",
                "Upstream assessment payload format is invalid.",
                details=common_details,
                retryable=True,
            ),
        )

    questionnaires = [
        str(q).strip()
        for q in msg.get("questionnaires_considered", [])
        if str(q).strip()
    ]
    if not questionnaires:
        raise HTTPException(
            status_code=404,
            detail=_error_detail(
                "ASSESSMENT_NOT_FOUND",
                "No assessment data found for the given employee, cycle name, and submission id.",
                details=common_details,
                retryable=False,
            ),
        )


def setup_routes(report_queue: ReportQueue) -> APIRouter:
    """
    Factory function — binds the shared report_queue to route handlers.
    Call this from main.py: app.include_router(setup_routes(report_queue))
    """

    @router.get("/debug-frappe/{employee_id}", summary="🔍 Debug Frappe data (no LLM)")
    @router.get("/api/debug-frappe/{employee_id}", include_in_schema=False)
    async def debug_frappe(
        request: Request,
        employee_id: str,
        cycle_name: Optional[str] = None,
        submission_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized_cycle = _normalize_optional_str(cycle_name)
        normalized_submission = _normalize_optional_str(submission_id)
        params = frappe_query_params(employee_id, cycle_name=normalized_cycle, submission_id=normalized_submission)

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(
                    Config.FRAPPE_BASE_URL,
                    params=params,
                    headers=frappe_headers(request=request),
                )
                resp.raise_for_status()
                frappe_data = resp.json()
            except httpx.HTTPStatusError as exc:
                raise HTTPException(502, f"Frappe {exc.response.status_code}: {exc.response.text[:300]}")
            except Exception as exc:
                raise HTTPException(502, f"Cannot reach Frappe: {exc}")

        if "message" not in frappe_data:
            raise HTTPException(502, f"Unexpected Frappe structure: {list(frappe_data.keys())}")

        msg = frappe_data["message"]
        questionnaires: List[str] = msg.get("questionnaires_considered", [])
        dimension = detect_dimension(questionnaires)
        return {
            "employee": employee_id,
            "submission_id": normalized_submission,
            "cycle_name": normalized_cycle,
            "questionnaires_considered": questionnaires,
            "questionnaire_count": len(questionnaires),
            "dimension_detected": dimension,
            "dominant_stage": msg.get("dominant_stage", "—"),
            "dominant_sub_stage": msg.get("dominant_sub_stage", "—"),
            "frappe_raw": frappe_data,
        }

    @router.post("/generate-employee-report", summary="🚀 Submit employee report job")
    @router.post("/api/generate-employee-report", include_in_schema=False)
    async def generate_employee_report(
        request: Request,
        payload: Dict[str, Any] = Body(
            ...,
            examples={
                "basic":           {"summary": "Generate report",        "value": {"employee": "HR-EMP-00031"}},
                "with_cycle":      {"summary": "With cycle name",         "value": {"employee": "HR-EMP-00031", "cycle_name": "Assessment Cycle - 0442"}},
                "with_submission": {"summary": "With submission id",      "value": {"employee": "HR-EMP-00031", "submission_id": "SUB-000442"}},
                "with_user_keys":  {"summary": "With runtime Frappe API keys", "value": {"employee": "HR-EMP-00031", "frappe_api_key": "user_api_key", "frappe_api_secret": "user_api_secret"}},
                "force":           {"summary": "Force regenerate",        "value": {"employee": "HR-EMP-00031", "force_regenerate": True}},
            },
        ),
    ) -> Dict[str, Any]:
        employee_id = str(payload.get("employee", "")).strip()
        if not employee_id:
            raise HTTPException(400, "'employee' field is required.")
        submission_id = _optional_payload_str(payload, "submission_id")
        cycle_name = _normalize_optional_str(payload.get("cycle_name"))
        force_regenerate = bool(payload.get("force_regenerate", False))
        runtime_frappe_auth = _resolve_runtime_frappe_auth(payload=payload, request=request)

        if not force_regenerate:
            try:
                cached = load_employee_json(employee_id, submission_id=submission_id, cycle_name=cycle_name)
                cached_header = cached.get("header", {})
                cached_submission_id = _normalize_optional_str(cached_header.get("submission_id"))
                cached_cycle_name = _normalize_optional_str(cached_header.get("cycle_name"))

                cached_questionnaires = _questionnaire_text_to_list(cached_header.get("questionnaire_text"))
                expected_dim = detect_dimension(cached_questionnaires) if cached_questionnaires else None
                cached_dim = (
                    _extract_dimension_code(cached_header.get("dimension_label"))
                    or _extract_dimension_code(cached_header.get("report_type"))
                )
                dimension_mismatch = bool(expected_dim and cached_dim and expected_dim != cached_dim)
                single_questionnaire = len(cached_questionnaires) == 1
                expected_primary = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(expected_dim) if expected_dim else None
                cached_report_types = [
                    str(r.get("report_type", "")).strip().lower()
                    for r in cached.get("reports", []) if isinstance(r, dict)
                ]
                report_scope_mismatch = bool(
                    submission_id and single_questionnaire and expected_primary
                    and (len(cached_report_types) != 1 or cached_report_types[0] != expected_primary)
                )
                cached_stage_scores = cached_header.get("stage_scores", [])
                missing_final_values = any(
                    isinstance(row, dict) and not str(row.get("final_value", "")).strip()
                    for row in cached_stage_scores
                )

                if not dimension_mismatch and not report_scope_mismatch and not missing_final_values:
                    cached_urls = build_report_urls(
                        employee_id,
                        submission_id=cached_submission_id or submission_id,
                        cycle_name=cached_cycle_name or cycle_name,
                    )
                    return {
                        "job_id": None,
                        "status": "cached",
                        "employee": employee_id,
                        "submission_id": cached_submission_id,
                        "cycle_name": cached_cycle_name,
                        "message": "Cached report available.",
                        "report_url": cached_urls.get("report"),
                        "report_urls": cached_urls,
                    }
            except HTTPException as exc:
                if exc.status_code != 404:
                    raise

        job_payload: Dict[str, Any] = {"employee": employee_id}
        if submission_id:
            job_payload["submission_id"] = submission_id
        if cycle_name:
            job_payload["cycle_name"] = cycle_name
        if runtime_frappe_auth:
            job_payload["_frappe_auth"] = runtime_frappe_auth

        # Fail fast for invalid employee/cycle/submission combinations.
        await _validate_assessment_exists(
            request=request,
            employee_id=employee_id,
            cycle_name=cycle_name,
            submission_id=submission_id,
            frappe_auth=runtime_frappe_auth,
        )

        job_id = await report_queue.add_job(payload=job_payload, employee_report=True)
        report_urls = build_report_urls(employee_id, submission_id=submission_id, cycle_name=cycle_name)
        return {
            "job_id": job_id,
            "status": "submitted",
            "employee": employee_id,
            "submission_id": submission_id,
            "cycle_name": cycle_name,
            "message": "Job submitted. Use report URLs once generation is complete.",
            "report_url": report_urls.get("report"),
            "report_urls": report_urls,
        }

    @router.get("/report/{employee_id}", summary="📦 Get stored report JSON")
    @router.get("/api/report/{employee_id}", include_in_schema=False)
    async def get_employee_all_reports(
        employee_id: str,
        submission_id: Optional[str] = None,
        cycle_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        return load_employee_json(employee_id, submission_id=submission_id, cycle_name=cycle_name)

    return router
