"""
services/report_renderer.py
HTML + PDF rendering helpers for all report dimensions.
"""
import base64
import html as html_lib
import os
import re
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from jinja2 import Environment, FileSystemLoader, select_autoescape

from core.config import Config

try:
    from weasyprint import HTML as WeasyprintHTML
    WEASYPRINT_IMPORT_ERROR: Optional[str] = None
except Exception as exc:
    WeasyprintHTML = None
    WEASYPRINT_IMPORT_ERROR = str(exc)


# ─── Small text utilities ──────────────────────────────────────────────────────

def text_to_paragraphs(text: str) -> List[str]:
    if not text:
        return []
    parts = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    if len(parts) <= 1:
        parts = [p.strip() for p in text.splitlines() if p.strip()]
    return parts


def split_numbered_items(text: str) -> List[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    items: List[str] = []
    current = ""
    for line in lines:
        if re.match(r"^\d+[\.\)]\s+", line):
            if current:
                items.append(current)
            current = re.sub(r"^\d+[\.\)]\s+", "", line)
        else:
            current = f"{current} {line}".strip() if current else line
    if current:
        items.append(current)
    return items or [text]


def is_swot_section(section_id: Any, section_title: Any) -> bool:
    sec_id = str(section_id or "").strip().lower()
    sec_title = str(section_title or "").strip().lower()
    return sec_id == "swot" or "swot" in sec_title


def build_swot_lists_from_section_paragraphs(paragraphs: List[str]) -> Dict[str, List[str]]:
    """Parse free-text SWOT paragraphs into structured S/W/O/T lists."""
    swot: Dict[str, List[str]] = {
        "strengths": [], "weaknesses": [], "opportunities": [], "threat": []
    }
    keyword_map = {
        "strength": "strengths", "weakness": "weaknesses",
        "opportunit": "opportunities", "threat": "threat",
    }
    current_key = "strengths"
    for para in paragraphs:
        lower = para.lower()
        for kw, key in keyword_map.items():
            if kw in lower:
                current_key = key
                break
        items = split_numbered_items(para)
        swot[current_key].extend(items)
    return swot


_SWOT_KEYS = ("strengths", "weaknesses", "opportunities", "threat")
_SWOT_LABELS = {
    "strengths": "Strengths",
    "weaknesses": "Weaknesses",
    "opportunities": "Opportunities",
    "threat": "threat",
}
_SWOT_SECTION_TITLES = {
    "employee": "Individual SWOT Analysis",
    "boss": "Dyadic SWOT Analysis",
    "team": "Collective SWOT Analysis",
    "organization": "Cumulative SWOT Overlay",
}


def _normalize_swot_lists(raw_swot: Any) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {key: [] for key in _SWOT_KEYS}
    if not isinstance(raw_swot, dict):
        return normalized

    for key in _SWOT_KEYS:
        values = raw_swot.get(key, [])
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, list):
            continue
        cleaned: List[str] = []
        for value in values:
            text = str(value or "").strip()
            if text:
                cleaned.append(text)
        normalized[key] = cleaned
    return normalized


def _fill_missing_swot_lists(swot_lists: Dict[str, List[str]]) -> None:
    for key in _SWOT_KEYS:
        if not swot_lists.get(key):
            label = _SWOT_LABELS[key]
            swot_lists[key] = [f"{label} not explicitly available in this report output."]


def _swot_lists_to_paragraphs(swot_lists: Dict[str, List[str]]) -> List[str]:
    paragraphs: List[str] = []
    for key in _SWOT_KEYS:
        items = swot_lists.get(key, [])
        line = " ".join(f"{idx}. {item}" for idx, item in enumerate(items, 1))
        paragraphs.append(line or f"{_SWOT_LABELS[key]} not available.")
    return paragraphs


def _ensure_swot_sections_for_render(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    safe_reports: List[Dict[str, Any]] = []
    for report in reports:
        if not isinstance(report, dict):
            continue

        report_copy = dict(report)
        report_type = str(report_copy.get("report_type") or "").strip().lower()
        sections = list(report_copy.get("sections", []) or [])

        swot_index: Optional[int] = None
        for idx, section in enumerate(sections):
            if not isinstance(section, dict):
                continue
            if is_swot_section(section.get("id", ""), section.get("title", "")):
                swot_index = idx
                break

        if swot_index is None:
            swot_lists = {key: [] for key in _SWOT_KEYS}
            _fill_missing_swot_lists(swot_lists)
            sections.append({
                "id": "swot",
                "title": _SWOT_SECTION_TITLES.get(report_type, "SWOT Analysis"),
                "paragraphs": _swot_lists_to_paragraphs(swot_lists),
                "swot_lists": swot_lists,
            })
        else:
            swot_section = dict(sections[swot_index])
            existing_swot = swot_section.get("swot_lists")
            if isinstance(existing_swot, dict):
                swot_lists = _normalize_swot_lists(existing_swot)
            else:
                swot_lists = build_swot_lists_from_section_paragraphs(swot_section.get("paragraphs") or [])
                swot_lists = _normalize_swot_lists(swot_lists)
            _fill_missing_swot_lists(swot_lists)
            swot_section["id"] = "swot"
            swot_section["title"] = swot_section.get("title") or _SWOT_SECTION_TITLES.get(report_type, "SWOT Analysis")
            swot_section["swot_lists"] = swot_lists
            swot_section["paragraphs"] = _swot_lists_to_paragraphs(swot_lists)
            sections[swot_index] = swot_section

        report_copy["sections"] = sections
        safe_reports.append(report_copy)
    return safe_reports


# ─── Report normalization ──────────────────────────────────────────────────────

def normalize_single_report(
    report: Any, report_type: str, data: dict, report_title_map: Dict[str, str]
) -> Dict[str, Any]:
    if isinstance(report, dict) and "sections" in report:
        sections = []
        for section in report.get("sections", []):
            paragraphs = section.get("paragraphs") or text_to_paragraphs(section.get("text", ""))
            sections.append({
                "id": section.get("id", ""),
                "title": section.get("title", "Section"),
                "paragraphs": paragraphs,
            })
        return {
            "title": report.get("title") or report_title_map.get(report_type, "Report"),
            "report_type": report_type,
            "sections": sections,
        }
    text = report if isinstance(report, str) else str(report)
    return {
        "title": report_title_map.get(report_type, "Report"),
        "report_type": report_type,
        "sections": [{"id": "", "title": "Report", "paragraphs": text_to_paragraphs(text)}],
    }


def normalize_reports(
    report: Union[str, Dict[str, Any]],
    data: dict,
    report_title_map: Dict[str, str],
    default_report_type_by_dimension: Dict[str, str],
) -> List[Dict[str, Any]]:
    if isinstance(report, dict):
        if "sections" in report:
            rtype = report.get("report_type") or default_report_type_by_dimension.get(data.get("dimension"), "employee")
            return [normalize_single_report(report, rtype, data, report_title_map)]
        return [normalize_single_report(v, k, data, report_title_map) for k, v in report.items()]
    rtype = default_report_type_by_dimension.get(data.get("dimension"), "employee")
    return [normalize_single_report(report, rtype, data, report_title_map)]


# ─── HTML / PDF rendering ──────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_logo_data_uri() -> Optional[str]:
    logo_path = os.path.join(Config.TEMPLATE_DIR, "chatur logo.png")
    if not os.path.exists(logo_path):
        return None
    try:
        with open(logo_path, "rb") as handle:
            encoded = base64.b64encode(handle.read()).decode("ascii")
    except OSError:
        return None
    return f"data:image/png;base64,{encoded}"

def render_html_report(json_payload: Dict[str, Any]) -> str:
    """
    Render stored JSON payload → HTML using the unified Jinja2 template.
    Works for all dimensions (1D/2D/3D/4D).
    """
    try:
        env = Environment(
            loader=FileSystemLoader(Config.TEMPLATE_DIR),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template(Config.REPORT_TEMPLATE_NAME)
        header = dict(json_payload.get("header", {}))
        if not str(header.get("logo_data_uri") or "").strip():
            logo_data_uri = _load_logo_data_uri()
            if logo_data_uri:
                header["logo_data_uri"] = logo_data_uri
        reports = _ensure_swot_sections_for_render(
            list(json_payload.get("reports", []) or [])
        )
        return template.render(
            header=header,
            reports=reports,
        )
    except Exception as exc:
        raise RuntimeError(f"Template rendering failed: {exc}") from exc


def render_pdf_from_html(html_doc: str) -> bytes:
    if WeasyprintHTML is None:
        detail = "PDF rendering unavailable. Install weasyprint."
        if WEASYPRINT_IMPORT_ERROR:
            detail = f"{detail} Import error: {WEASYPRINT_IMPORT_ERROR}"
        raise RuntimeError(detail)
    return WeasyprintHTML(string=html_doc).write_pdf()
