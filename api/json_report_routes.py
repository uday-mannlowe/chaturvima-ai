"""
api/json_report_routes.py
Fast JSON generation endpoint - /generate/json
"""
import asyncio
import time as _time
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Body, HTTPException, Request

from core.config import Config
from services.frappe_client import (
    fetch_frappe_swot_doc,
    frappe_headers,
    frappe_query_params,
    resolve_frappe_auth_token,
)
from generate_groq import (
    DEFAULT_REPORT_TYPE_BY_DIMENSION,
    MODEL_BY_REPORT_TYPE_DEDICATED,
    generate_multi_reports_json,
    map_frappe_to_nd,
    resolve_input_data,
)

router = APIRouter(tags=["Fast JSON"])


def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


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


def _apply_1d_swot_override(reports_payload: Any, swot_doc: Optional[Dict[str, Any]]) -> bool:
    from core.worker_pool import _apply_1d_swot_override as _apply

    return _apply(reports_payload, swot_doc)


@router.post("/generate/json", summary="Fastest JSON report endpoint (parallel LLMs)")
async def generate_json_reports(
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    """Fires one dedicated LLM per report type, all in parallel."""
    start = _time.time()
    try:
        if "employee" in payload and "dimension" not in payload:
            employee_id = str(payload["employee"]).strip()
            cycle_name = _normalize_optional_str(payload.get("cycle_name"))
            submission_id = _optional_payload_str(payload, "submission_id")
            runtime_frappe_auth = _resolve_runtime_frappe_auth(payload=payload, request=request)
            params = frappe_query_params(employee_id, cycle_name=cycle_name, submission_id=submission_id)

            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    resp = await client.get(
                        Config.FRAPPE_BASE_URL,
                        params=params,
                        headers=frappe_headers(request=request, explicit_auth=runtime_frappe_auth),
                    )
                    resp.raise_for_status()
                    frappe_data = resp.json()
                except httpx.HTTPStatusError as exc:
                    detail = (exc.response.text or "").strip().replace("\n", " ")[:300]
                    raise HTTPException(
                        status_code=502,
                        detail=f"Frappe returned {exc.response.status_code}: {detail}",
                    ) from exc

            nd_data = map_frappe_to_nd(employee_id, frappe_data)
            msg = frappe_data.get("message", frappe_data)
            questionnaires = msg.get("questionnaires_considered", [])
            single_questionnaire = len(questionnaires) == 1
            swot_doc: Optional[Dict[str, Any]] = None
            if nd_data.get("dimension") == "1D":
                dominant_sub_stage = _normalize_optional_str(msg.get("dominant_sub_stage"))
                if dominant_sub_stage:
                    swot_doc = await fetch_frappe_swot_doc(dominant_sub_stage, user_auth=runtime_frappe_auth or "")
        else:
            nd_data = resolve_input_data(payload)
            swot_doc = None
            single_questionnaire = False

        result = await asyncio.wait_for(
            asyncio.to_thread(generate_multi_reports_json, nd_data),
            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
        )

        if single_questionnaire:
            primary = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(nd_data.get("dimension"))
            reports_map = result.get("reports", {})
            if primary and isinstance(reports_map, dict) and primary in reports_map:
                result["reports"] = {primary: reports_map[primary]}

        if nd_data.get("dimension") == "1D" and swot_doc:
            _apply_1d_swot_override(result.get("reports", {}), swot_doc)

        result["elapsed_seconds"] = round(_time.time() - start, 2)
        result["model_map"] = MODEL_BY_REPORT_TYPE_DEDICATED
        return result

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Report generation timed out.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
