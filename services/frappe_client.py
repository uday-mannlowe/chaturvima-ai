"""
services/frappe_client.py
All Frappe API interactions: auth headers, SWOT fetching, query params, data mapping helpers.
"""
import base64
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx
from fastapi import HTTPException, Request

from core.config import Config


def frappe_headers(request: Optional[Request] = None) -> dict:
    """
    Build Frappe auth headers.

    Priority:
      1. Forward the Authorization header from the incoming user request
      2. Fall back to hardcoded .env admin key ONLY if no user token present
    """
    if request is not None:
        user_auth = request.headers.get("Authorization", "").strip()
        if user_auth.lower().startswith("token "):
            print("🔑 Frappe auth: forwarding user token from request")
            return {"Authorization": user_auth, "Content-Type": "application/json"}

    if Config.FRAPPE_API_KEY and Config.FRAPPE_API_SECRET:
        print("⚠️  Frappe auth: using fallback admin key from .env (no user token found)")
        return {
            "Authorization": f"token {Config.FRAPPE_API_KEY}:{Config.FRAPPE_API_SECRET}",
            "Content-Type": "application/json",
        }
    if Config.FRAPPE_USERNAME and Config.FRAPPE_PASSWORD:
        creds = base64.b64encode(
            f"{Config.FRAPPE_USERNAME}:{Config.FRAPPE_PASSWORD}".encode()
        ).decode()
        return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}

    print("⚠️  FRAPPE_AUTH: no credentials – requests to protected endpoints will 403")
    return {"Content-Type": "application/json"}


def frappe_query_params(
    employee_id: str,
    cycle_name: Optional[str] = None,
    submission_id: Optional[str] = None,
) -> Dict[str, str]:
    params: Dict[str, str] = {"employee": employee_id}
    if cycle_name:
        params["cycle_name"] = cycle_name
    if submission_id:
        params["submission_id"] = submission_id
    return params


def frappe_swot_doc_url(docname: str) -> str:
    encoded = quote(docname, safe="")
    return f"{Config.FRAPPE_RESOURCE_BASE_URL}/SWOT%20Analysis/{encoded}"


def collect_child_row_texts(rows: Any) -> List[str]:
    if not isinstance(rows, list):
        return []
    values: List[str] = []
    for row in rows:
        if isinstance(row, dict):
            text = (
                row.get("description")
                or row.get("desription")   # Frappe child-table typo
                or row.get("recommendations_description")
                or row.get("value")
                or row.get("title")
            )
            text = str(text).strip() if text else ""
        else:
            text = str(row).strip() if row else ""
        if text:
            values.append(text)
    return values


def extract_swot_lists(swot_doc: Dict[str, Any]) -> Dict[str, List[str]]:
    # NOTE: Frappe's Threat child table uses 'desription' (typo, missing 'c').
    # collect_child_row_texts already handles this via its 'desription' fallback.
    return {
        "strengths":     collect_child_row_texts(swot_doc.get("strength")     or swot_doc.get("strengths",     [])),
        "weaknesses":    collect_child_row_texts(swot_doc.get("weakness")     or swot_doc.get("weaknesses",    [])),
        "opportunities": collect_child_row_texts(swot_doc.get("opportunity")  or swot_doc.get("opportunities", [])),
        "threat":       collect_child_row_texts(swot_doc.get("threat")       or swot_doc.get("threat",       [])),
    }


# Frappe metadata keys to strip from child-table rows (internal Frappe fields, not content)
_FRAPPE_META_KEYS = frozenset({
    "idx", "doctype", "parent", "parenttype", "parentfield",
    "owner", "creation", "modified", "modified_by", "docstatus", "name",
    "amended_from", "_user_tags", "_comments", "_assign", "_liked_by",
})


def collect_child_row_dicts(rows: Any) -> List[Dict[str, Any]]:
    """
    Return each Frappe child-table row as a plain dict, preserving all
    content fields exactly as returned by the API.
    Strips only Frappe internal metadata keys (idx, parent, doctype, etc.).
    """
    if not isinstance(rows, list):
        return []
    result: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            clean = {k: v for k, v in row.items() if k not in _FRAPPE_META_KEYS}
            if clean:
                result.append(clean)
        elif row is not None:
            result.append({"value": str(row)})
    return result


def extract_full_swot_doc(swot_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all 6 required fields from the SWOT Frappe doc, preserving
    the original row data exactly as returned by the API.

    Fields:
        strengths        — child table 'strength'
        weaknesses       — child table 'weakness'
        opportunities    — child table 'opportunity'
        threat          — child table 'threat'
        recommendations  — child table 'reccomendation' (Frappe typo preserved)
        actionable_steps — child table 'actionable_steps'
        sub_stage        — string label
        strategic_recommendations — free-text field
    """
    return {
        "sub_stage": (swot_doc.get("sub_stage") or swot_doc.get("name") or "").strip(),
        "strengths":     collect_child_row_dicts(
            swot_doc.get("strength")     or swot_doc.get("strengths",     [])
        ),
        "weaknesses":    collect_child_row_dicts(
            swot_doc.get("weakness")     or swot_doc.get("weaknesses",    [])
        ),
        "opportunities": collect_child_row_dicts(
            swot_doc.get("opportunity")  or swot_doc.get("opportunities", [])
        ),
        "threat":       collect_child_row_dicts(
            swot_doc.get("threat")       or swot_doc.get("threat",       [])
        ),
        "recommendations": collect_child_row_dicts(
            swot_doc.get("reccomendation")   # Frappe typo kept as-is
            or swot_doc.get("recommendation")
            or swot_doc.get("recommendations")
            or []
        ),
        "actionable_steps": collect_child_row_dicts(
            swot_doc.get("actionable_steps")
            or swot_doc.get("actionable_step")
            or []
        ),
        "strategic_recommendations": (swot_doc.get("strategic_recommendations") or "").strip(),
    }


def build_swot_items(texts: List[str], category_label: str) -> List[Dict[str, str]]:
    return [
        {
            "area": f"{category_label} {idx}",
            "description": text,
            "context": "Fetched from SWOT Analysis doctype.",
            "impact": "",
        }
        for idx, text in enumerate(texts, start=1)
    ]


def map_frappe_swot_doc(swot_doc: Dict[str, Any]) -> Dict[str, Any]:
    swot_lists = extract_swot_lists(swot_doc)
    rec_rows = (
        swot_doc.get("reccomendation")
        or swot_doc.get("recommendation")
        or swot_doc.get("recommendations")
    )
    principles: List[str] = []
    recommended_actions: List[Dict[str, str]] = []

    strategic_intro = (swot_doc.get("strategic_recommendations") or "").strip() or None
    if strategic_intro:
        principles.append(strategic_intro)

    if isinstance(rec_rows, list):
        for idx, row in enumerate(rec_rows, start=1):
            if not isinstance(row, dict):
                continue
            title = (row.get("recommendations_title") or row.get("title") or "").strip() or None
            description = (
                row.get("recommendations_description") or row.get("description") or row.get("desription") or ""
            ).strip() or None
            if title:
                principles.append(title)
            if title or description:
                recommended_actions.append({
                    "focus_area": title or f"Action {idx}",
                    "recommendation": description or title or f"Action {idx}",
                    "priority": "Medium",
                    "time_horizon": "Short Term",
                })

    framework_name = (swot_doc.get("sub_stage") or "").strip() or "SWOT Strategic Recommendations"
    return {
        "individual_swot": {
            "strengths":     build_swot_items(swot_lists["strengths"],     "Strength"),
            "weaknesses":    build_swot_items(swot_lists["weaknesses"],    "Weakness"),
            "opportunities": build_swot_items(swot_lists["opportunities"], "Opportunity"),
            "threat":       build_swot_items(swot_lists["threat"],       "Threat"),
        },
        "recommendation_framework": {
            "framework_name": framework_name,
            "principles": principles,
            "recommended_actions": recommended_actions,
        },
    }


async def fetch_frappe_swot_doc(sub_stage: Optional[str], user_auth: str = "") -> Optional[Dict[str, Any]]:
    """
    Fetch SWOT doc from Frappe.
    user_auth: the logged-in user's 'token api_key:api_secret' string.
               If provided, it is used instead of the fallback .env admin key.
    """
    if not sub_stage:
        return None

    # Use the calling user's token if available, otherwise fall back to .env admin key
    if user_auth and user_auth.lower().startswith("token "):
        headers = {"Authorization": user_auth, "Content-Type": "application/json"}
        print(f"🔑 SWOT fetch: using user token for sub_stage='{sub_stage}'")
    else:
        headers = frappe_headers()  # fallback to .env admin key
        print(f"⚠️  SWOT fetch: no user token provided, using fallback admin key for sub_stage='{sub_stage}'")
    async with httpx.AsyncClient(timeout=30) as client:
        # Fast path: direct doc lookup
        direct_url = frappe_swot_doc_url(sub_stage)
        try:
            resp = await client.get(direct_url, headers=headers)
            if resp.status_code == 200:
                payload = resp.json()
                data = payload.get("data", payload)
                if isinstance(data, dict):
                    return data
            elif resp.status_code not in (404,):
                resp.raise_for_status()
        except Exception as exc:
            print(f"⚠️ SWOT direct lookup failed for '{sub_stage}': {exc}")

        # Fallback: filter search
        list_url = f"{Config.FRAPPE_RESOURCE_BASE_URL}/SWOT%20Analysis"
        for filters in (
            [["sub_stage", "=", sub_stage]],
            [["name", "=", sub_stage]],
        ):
            try:
                list_resp = await client.get(
                    list_url,
                    headers=headers,
                    params={
                        "filters": json.dumps(filters),
                        "fields": json.dumps(["name"]),
                        "limit_page_length": "1",
                    },
                )
                list_resp.raise_for_status()
                rows = list_resp.json().get("data", [])
                if not isinstance(rows, list) or not rows:
                    continue
                row0 = rows[0] if isinstance(rows[0], dict) else {}
                doc_name = (row0.get("name") or "").strip() or None
                if not doc_name:
                    continue
                doc_resp = await client.get(frappe_swot_doc_url(doc_name), headers=headers)
                doc_resp.raise_for_status()
                doc = doc_resp.json().get("data", doc_resp.json())
                if isinstance(doc, dict):
                    return doc
            except Exception as exc:
                print(f"⚠️ SWOT filter lookup failed for '{sub_stage}' with {filters}: {exc}")

    return None
