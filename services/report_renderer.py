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
        "strengths": [], "weaknesses": [], "opportunities": [], "threats": []
    }
    keyword_map = {
        "strength": "strengths", "weakness": "weaknesses",
        "opportunit": "opportunities", "threat": "threats",
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
        return template.render(
            header=header,
            reports=json_payload.get("reports", []),
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
