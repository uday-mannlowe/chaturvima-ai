"""
api/json_report_routes.py
Fast JSON generation endpoint - /generate/json
"""
import asyncio
import time as _time
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Body, HTTPException, Request

from core.config import Config
from services.frappe_client import (
    extract_full_swot_doc,
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
    if Config.FORCE_STATIC_FRAPPE_AUTH:
        return None

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

    return resolve_frappe_auth_token(request=request, payload=payload)


def _apply_1d_swot_override(reports_payload: Any, swot_doc: Optional[Dict[str, Any]]) -> bool:
    from core.worker_pool import _apply_1d_swot_override as _apply

    return _apply(reports_payload, swot_doc)


def _inject_actionable_into_action_section(sections: List[Dict[str, Any]], swot_payload: Dict[str, Any]) -> bool:
    if not isinstance(swot_payload, dict) or not isinstance(sections, list):
        return False

    def _row_text(row: Any) -> str:
        if isinstance(row, dict):
            return (
                row.get("description")
                or row.get("desription")
                or row.get("recommendations_description")
                or row.get("value")
                or row.get("title")
                or ""
            ).strip()
        return str(row or "").strip()

    raw_steps = swot_payload.get("actionable_steps", [])
    if not isinstance(raw_steps, list):
        return False
    step_texts = [_row_text(r) for r in raw_steps if _row_text(r)]
    if not step_texts:
        return False

    priority_ids = ("action_plan", "next_steps", "joint_recommendations", "boss_recommendations", "recommendations")
    title_hints = ("action navigator", "action plan", "next steps", "development path", "recommendation", "intervention")

    target_idx: Optional[int] = None
    for preferred in priority_ids:
        for idx, sec in enumerate(sections):
            if not isinstance(sec, dict):
                continue
            if str(sec.get("id", "")).strip().lower() == preferred:
                target_idx = idx
                break
        if target_idx is not None:
            break

    if target_idx is None:
        for idx, sec in enumerate(sections):
            if not isinstance(sec, dict):
                continue
            title = str(sec.get("title", "")).strip().lower()
            if any(h in title for h in title_hints):
                target_idx = idx
                break

    if target_idx is None:
        return False

    target = sections[target_idx]
    target["actionable_steps"] = [{"description": t} for t in step_texts]

    action_line = "Actionable Steps: " + " ".join(f"{i}. {t}" for i, t in enumerate(step_texts, 1))
    paras = target.get("paragraphs", [])
    if not isinstance(paras, list):
        paras = [str(paras)] if paras else []

    replaced = False
    for i, para in enumerate(paras):
        if str(para).strip().lower().startswith("actionable steps:"):
            paras[i] = action_line
            replaced = True
            break
    if not replaced:
        paras.append(action_line)
    target["paragraphs"] = paras
    return True


def _inject_recommendations_section(sections: List[Dict[str, Any]], swot_payload: Dict[str, Any]) -> bool:
    if not isinstance(swot_payload, dict) or not isinstance(sections, list):
        return False

    def _row_text(row: Any) -> str:
        if isinstance(row, dict):
            return (
                row.get("description")
                or row.get("desription")
                or row.get("recommendations_description")
                or row.get("value")
                or row.get("title")
                or ""
            ).strip()
        return str(row or "").strip()

    rec_rows = swot_payload.get("recommendations", [])
    strategic = str(swot_payload.get("strategic_recommendations", "") or "").strip()

    paragraphs: List[str] = []
    normalized_recs: List[Dict[str, str]] = []
    if isinstance(rec_rows, list):
        idx = 1
        for row in rec_rows:
            if isinstance(row, dict):
                title = str(row.get("recommendations_title") or row.get("title") or "").strip()
                desc = _row_text(row)
                if not title and not desc:
                    continue
                paragraphs.append(f"{idx}. {title}: {desc}" if title and desc else f"{idx}. {title or desc}")
                normalized_recs.append({
                    "recommendations_title": title or f"Recommendation {idx}",
                    "recommendations_description": desc or title or "",
                })
                idx += 1
            else:
                text = _row_text(row)
                if text:
                    paragraphs.append(f"{idx}. {text}")
                    normalized_recs.append({
                        "recommendations_title": f"Recommendation {idx}",
                        "recommendations_description": text,
                    })
                    idx += 1
    if strategic:
        paragraphs.append(f"Strategic Recommendations: {strategic}")

    if not paragraphs:
        return False

    payload = {
        "id": "recommendations",
        "title": "Recommendations",
        "paragraphs": paragraphs,
        "recommendations": normalized_recs,
        "strategic_recommendations": strategic,
        "source": swot_payload.get("source", ""),
    }

    target_idx: Optional[int] = None
    swot_idx: Optional[int] = None
    for idx, sec in enumerate(sections):
        if not isinstance(sec, dict):
            continue
        sec_id = str(sec.get("id", "")).strip().lower()
        sec_title = str(sec.get("title", "")).strip().lower()
        if sec_id == "recommendations" or sec_title == "recommendations":
            target_idx = idx
            break
        if swot_idx is None and (sec_id == "swot" or "swot" in sec_title):
            swot_idx = idx

    if target_idx is not None:
        sections[target_idx] = payload
        return True

    if swot_idx is not None:
        sections.insert(swot_idx + 1, payload)
    else:
        sections.append(payload)
    return True


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

        # ── SWOT data: do NOT send to LLM — hold for post-inject ──
        # Pre-written Frappe content needs no LLM processing. Strip from nd_data
        # to keep the prompt lean; inject verbatim into output after LLM finishes.
        full_swot: Optional[Dict[str, Any]] = None
        if swot_doc:
            full_swot = extract_full_swot_doc(swot_doc)
            if nd_data.get("dimension") == "1D":
                def _row_text(row: Any) -> str:
                    if isinstance(row, dict):
                        return (
                            row.get("description")
                            or row.get("desription")
                            or row.get("recommendations_description")
                            or row.get("value")
                            or row.get("title")
                            or ""
                        ).strip()
                    return str(row or "").strip()

                def _has_rows(rows: Any) -> bool:
                    return isinstance(rows, list) and any(_row_text(r) for r in rows)

                missing_actionable = not _has_rows(full_swot.get("actionable_steps", []))
                missing_recommendations = not _has_rows(full_swot.get("recommendations", []))
                missing_strategic = not str(full_swot.get("strategic_recommendations", "") or "").strip()

                if missing_actionable or missing_recommendations or missing_strategic:
                    from core.worker_pool import _generate_swot_via_llm

                    llm_guidance = await asyncio.to_thread(
                        _generate_swot_via_llm,
                        nd_data.get("behavioral_stage", {}),
                        "employee",
                    )
                    if missing_recommendations:
                        full_swot["recommendations"] = llm_guidance.get("recommendations", [])
                    if missing_actionable:
                        full_swot["actionable_steps"] = llm_guidance.get("actionable_steps", [])
                    if missing_strategic:
                        full_swot["strategic_recommendations"] = llm_guidance.get("strategic_recommendations", "")

                    base_source = str(full_swot.get("source", "") or "").strip()
                    full_swot["source"] = f"{base_source}+llm_guidance_fill" if base_source else "llm_guidance_fill"

            nd_data.pop("individual_swot", None)
            nd_data.pop("recommendation_framework", None)

        result = await asyncio.wait_for(
            asyncio.to_thread(generate_multi_reports_json, nd_data),
            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
        )

        if single_questionnaire:
            primary = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(nd_data.get("dimension"))
            reports_map = result.get("reports", {})
            if primary and isinstance(reports_map, dict) and primary in reports_map:
                result["reports"] = {primary: reports_map[primary]}

        # ── 1D: inject Frappe SWOT verbatim into the employee report (post-LLM) ──
        if full_swot and isinstance(result.get("reports"), dict):
            emp = result["reports"].get("employee")
            if isinstance(emp, dict) and isinstance(emp.get("sections"), list):
                from services.report_renderer import is_swot_section
                injected = False
                for sec in emp["sections"]:
                    if is_swot_section(sec.get("id", ""), sec.get("title", "")):
                        sec["swot_lists"] = {
                            "strengths":     [r.get("description", "") for r in full_swot["strengths"]     if r.get("description")],
                            "weaknesses":    [r.get("description", "") for r in full_swot["weaknesses"]    if r.get("description")],
                            "opportunities": [r.get("description", "") for r in full_swot["opportunities"] if r.get("description")],
                            "threat":       [r.get("desription",   "") or r.get("description", "") for r in full_swot["threat"] if r.get("desription") or r.get("description")],
                        }
                        sec["recommendations"]  = full_swot["recommendations"]
                        sec["actionable_steps"] = full_swot["actionable_steps"]
                        sec["strategic_recommendations"] = full_swot["strategic_recommendations"]
                        sec["source"] = full_swot.get("source", "frappe_swot")
                        injected = True
                        break

                if injected:
                    _inject_actionable_into_action_section(emp["sections"], full_swot)
                    _inject_recommendations_section(emp["sections"], full_swot)

        result["elapsed_seconds"] = round(_time.time() - start, 2)
        result["model_map"] = MODEL_BY_REPORT_TYPE_DEDICATED
        return result

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Report generation timed out.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
