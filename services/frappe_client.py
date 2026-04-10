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
from fastapi import Request

from core.config import Config


def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _token_from_key_secret(api_key: Any, api_secret: Any) -> Optional[str]:
    key = _normalize_optional_str(api_key)
    secret = _normalize_optional_str(api_secret)
    if key and secret:
        return f"token {key}:{secret}"
    return None


def _token_from_request_headers(request: Optional[Request]) -> Optional[str]:
    if request is None:
        return None

    auth_header = _normalize_optional_str(request.headers.get("Authorization"))
    if auth_header and auth_header.lower().startswith("token "):
        return auth_header

    frappe_auth_header = _normalize_optional_str(request.headers.get("X-Frappe-Authorization"))
    if frappe_auth_header and frappe_auth_header.lower().startswith("token "):
        return frappe_auth_header

    return _token_from_key_secret(
        request.headers.get("X-Frappe-Api-Key"),
        request.headers.get("X-Frappe-Api-Secret"),
    )


def _token_from_payload(payload: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    # Check pre-built token strings first
    for key in ("frappe_auth_token", "_frappe_auth", "frappe_token", "frappe_authorization"):
        token = _normalize_optional_str(payload.get(key))
        if token and token.lower().startswith("token "):
            return token

    # Build token from key+secret pair
    token = _token_from_key_secret(payload.get("frappe_api_key"), payload.get("frappe_api_secret"))
    if token:
        return token

    return _token_from_key_secret(payload.get("frappe_key"), payload.get("frappe_secret"))


def resolve_frappe_auth_token(
    request: Optional[Request] = None,
    payload: Optional[Dict[str, Any]] = None,
    explicit_auth: Optional[str] = None,
) -> Optional[str]:
    # 1. Explicit auth takes highest priority
    token = _normalize_optional_str(explicit_auth)
    if token and token.lower().startswith("token "):
        return token

    # 2. Payload body (frappe_auth_token, frappe_api_key+secret, etc.)
    token = _token_from_payload(payload)
    if token:
        return token

    # 3. Request headers — ONLY used when no explicit/payload auth is present.
    #    This prevents Frappe session cookies/headers from overriding API tokens.
    token = _token_from_request_headers(request)
    if token:
        return token

    return None


def frappe_headers(
    request: Optional[Request] = None,
    payload: Optional[Dict[str, Any]] = None,
    explicit_auth: Optional[str] = None,
) -> dict:
    """
    Build Frappe auth headers.

    Priority:
      1. explicit_auth (pre-built token string)
      2. payload body  (frappe_auth_token / frappe_api_key+secret)
      3. request headers — only when explicit_auth AND payload both have nothing
      4. .env fallback  (FRAPPE_API_KEY + FRAPPE_API_SECRET)
      5. .env username/password

    IMPORTANT: When explicit_auth is provided, request is intentionally ignored
    so that incoming Frappe session headers do not override the API token.
    """
    # If explicit_auth is set, resolve ONLY from explicit+payload — skip request headers
    if explicit_auth:
        token = _normalize_optional_str(explicit_auth)
        if token and token.lower().startswith("token "):
            print("[FRAPPE_AUTH] using explicit_auth token")
            return {"Authorization": token, "Content-Type": "application/json"}

    # Full resolution including request headers
    runtime_auth = resolve_frappe_auth_token(request=request, payload=payload, explicit_auth=explicit_auth)
    if runtime_auth:
        print("[FRAPPE_AUTH] using runtime token from request/payload")
        return {"Authorization": runtime_auth, "Content-Type": "application/json"}

    # Fallback to .env credentials
    if Config.FRAPPE_API_KEY and Config.FRAPPE_API_SECRET:
        print("[FRAPPE_AUTH] using fallback admin key from .env")
        return {
            "Authorization": f"token {Config.FRAPPE_API_KEY}:{Config.FRAPPE_API_SECRET}",
            "Content-Type": "application/json",
        }

    if Config.FRAPPE_USERNAME and Config.FRAPPE_PASSWORD:
        creds = base64.b64encode(
            f"{Config.FRAPPE_USERNAME}:{Config.FRAPPE_PASSWORD}".encode()
        ).decode()
        print("[FRAPPE_AUTH] using fallback username/password from .env")
        return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}

    print("[FRAPPE_AUTH] no credentials found; protected endpoints may return 403")
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


def _normalize_sub_stage_value(value: str) -> str:
    text = str(value or "").strip()
    text = text.replace("—", "-").replace("–", "-")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*-\s*", " - ", text)
    return text.strip()


def _strip_stage_suffix(value: str) -> str:
    return re.sub(r"\s*-\s*[\w\s]+$", "", value).strip()


def _sub_stage_candidates(sub_stage: str) -> List[str]:
    base = _normalize_sub_stage_value(sub_stage)
    if not base:
        return []

    candidates: List[str] = []

    def _add(v: str) -> None:
        cleaned = _normalize_sub_stage_value(v)
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)

    _add(base)
    _add(base.lower())
    _add(base.title())

    stripped = _strip_stage_suffix(base)
    _add(stripped)
    _add(stripped.lower())
    _add(stripped.title())

    _add(base.replace(" - ", "-"))
    _add(base.replace(" - ", " "))

    return [c for c in candidates if c]


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
    return {
        "strengths":     collect_child_row_texts(swot_doc.get("strength")     or swot_doc.get("strengths",     [])),
        "weaknesses":    collect_child_row_texts(swot_doc.get("weakness")     or swot_doc.get("weaknesses",    [])),
        "opportunities": collect_child_row_texts(swot_doc.get("opportunity")  or swot_doc.get("opportunities", [])),
        "threat":        collect_child_row_texts(swot_doc.get("threat")       or swot_doc.get("threat",        [])),
    }


_FRAPPE_META_KEYS = frozenset({
    "idx", "doctype", "parent", "parenttype", "parentfield",
    "owner", "creation", "modified", "modified_by", "docstatus", "name",
    "amended_from", "_user_tags", "_comments", "_assign", "_liked_by",
})


def collect_child_row_dicts(rows: Any) -> List[Dict[str, Any]]:
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
    return {
        "source": "frappe_swot",
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
        "threat":        collect_child_row_dicts(
            swot_doc.get("threat")       or swot_doc.get("threat",        [])
        ),
        "recommendations": collect_child_row_dicts(
            swot_doc.get("reccomendation")
            or swot_doc.get("recommendation")
            or swot_doc.get("recommendations")
            or []
        ),
        "actionable_steps": collect_child_row_dicts(
            swot_doc.get("actionable_steps")
            or swot_doc.get("actionable_step")
            or swot_doc.get("actionable_items")
            or swot_doc.get("action_items")
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
                row.get("recommendations_description")
                or row.get("description")
                or row.get("desription")
                or ""
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
            "threat":        build_swot_items(swot_lists["threat"],        "Threat"),
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
    user_auth: pre-built token string like 'token api_key:api_secret'.
    """
    if not sub_stage:
        return None

    runtime_auth = resolve_frappe_auth_token(explicit_auth=user_auth)
    if runtime_auth:
        headers = {"Authorization": runtime_auth, "Content-Type": "application/json"}
        print(f"[FRAPPE_SWOT] using runtime token for sub_stage='{sub_stage}'")
    else:
        headers = frappe_headers()
        print(f"[FRAPPE_SWOT] runtime token missing for sub_stage='{sub_stage}', using fallback creds")

    lookup_candidates = _sub_stage_candidates(sub_stage)
    if not lookup_candidates:
        lookup_candidates = [sub_stage]

    async with httpx.AsyncClient(timeout=30) as client:
        for candidate in lookup_candidates:
            direct_url = frappe_swot_doc_url(candidate)
            try:
                resp = await client.get(direct_url, headers=headers)
                if resp.status_code == 200:
                    payload = resp.json()
                    data = payload.get("data", payload)
                    if isinstance(data, dict):
                        print(f"[FRAPPE_SWOT] direct lookup matched candidate='{candidate}'")
                        return data
                elif resp.status_code not in (404,):
                    resp.raise_for_status()
            except Exception as exc:
                print(f"[FRAPPE_SWOT] direct lookup failed for '{candidate}': {exc}")

        list_url = f"{Config.FRAPPE_RESOURCE_BASE_URL}/SWOT%20Analysis"
        seen_filters = set()
        filter_specs: List[List[List[str]]] = []
        for candidate in lookup_candidates:
            filter_specs.extend([
                [["sub_stage", "=", candidate]],
                [["name", "=", candidate]],
                [["sub_stage", "like", f"%{candidate}%"]],
                [["name", "like", f"%{candidate}%"]],
            ])

        for filters in filter_specs:
            key = json.dumps(filters, sort_keys=True)
            if key in seen_filters:
                continue
            seen_filters.add(key)

            try:
                list_resp = await client.get(
                    list_url,
                    headers=headers,
                    params={
                        "filters": json.dumps(filters),
                        "fields": json.dumps(["name", "sub_stage"]),
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
                doc_payload = doc_resp.json()
                doc = doc_payload.get("data", doc_payload)
                if isinstance(doc, dict):
                    print(f"[FRAPPE_SWOT] filter lookup matched name='{doc_name}' for filters={filters}")
                    return doc
            except Exception as exc:
                print(f"[FRAPPE_SWOT] filter lookup failed for '{sub_stage}' with {filters}: {exc}")

    return None
