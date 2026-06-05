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

def _clean_bracket_artifacts(text: str) -> str:
    """
    Remove stray JSON brackets, quotes, and escape artifacts that
    sometimes appear when LLM output is stored as raw JSON strings.
    e.g.  '["The employee..."]'  →  'The employee...'
          '"Some text"'         →  'Some text'
          'Some text\\n'        →  'Some text'

    NOTE: If the text is a multi-element JSON array like
    '["para one", "para two"]', only the FIRST element is returned.
    Use _clean_paragraphs() which splits multi-element arrays properly.
    """
    if not text:
        return text
    text = text.strip()

    # If text looks like a JSON array, try to parse it and take the first element
    if text.startswith('[') and text.endswith(']'):
        import json as _json
        try:
            parsed = _json.loads(text)
            if isinstance(parsed, list) and parsed:
                # Return first non-empty string element
                for item in parsed:
                    s = str(item or "").strip()
                    if s:
                        text = s
                        break
                else:
                    return ""
        except (_json.JSONDecodeError, ValueError):
            # Not valid JSON — try simple unwrap below
            inner = text[1:-1].strip()
            if (inner.startswith('"') and inner.endswith('"')) or \
               (inner.startswith("'") and inner.endswith("'")):
                text = inner[1:-1].strip()
            elif inner and not inner.startswith('['):
                text = inner

    # Strip wrapping double or single quotes
    if (text.startswith('"') and text.endswith('"')) or \
       (text.startswith("'") and text.endswith("'")):
        text = text[1:-1].strip()

    # Remove escaped newlines and backslash artifacts
    text = text.replace('\\n', ' ').replace('\\t', ' ').replace('\\"', '"')

    # Collapse multiple spaces
    text = re.sub(r' {2,}', ' ', text).strip()

    return text


def _expand_bracket_array(text: str) -> List[str]:
    """
    If text is a JSON array string like '["para one", "para two"]',
    return all elements as a list of clean strings.
    Otherwise return [text] with bracket artifacts removed.
    """
    if not text:
        return []
    text = text.strip()
    if text.startswith('[') and text.endswith(']'):
        import json as _json
        try:
            parsed = _json.loads(text)
            if isinstance(parsed, list):
                results = []
                for item in parsed:
                    s = _clean_bracket_artifacts(str(item or "").strip())
                    if s:
                        results.append(s)
                return results
        except (_json.JSONDecodeError, ValueError):
            pass
    return [_clean_bracket_artifacts(text)]


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


# Maps keyword fragments to SWOT quadrant keys.
# Order matters: more specific patterns first.
_QUAD_KEYWORD_MAP = [
    (re.compile(r'\bopportunit', re.IGNORECASE), "opportunities"),
    (re.compile(r'\bthreat',     re.IGNORECASE), "threats"),
    (re.compile(r'\bweakness|\bblind.?spot', re.IGNORECASE), "weaknesses"),
    (re.compile(r'\bstrength',   re.IGNORECASE), "strengths"),
]

# Sentence starters that signal the LLM is introducing a NEW quadrant.
# e.g. "There are several opportunities for growth"
#      "One threat is that..."
#      "Weaknesses include..."
_QUAD_INTRO_PATTERNS = re.compile(
    r'^(there are|one|another|additionally|furthermore|the|these)?\s*'
    r'(strengths?|weaknesses?|blind.?spots?|opportunities?|threats?)'
    r'\s*(of|for|to|include|in|is|are|:)',
    re.IGNORECASE
)


def _detect_quadrant(text: str) -> Optional[str]:
    """Return the SWOT quadrant this text most likely belongs to, or None."""
    # First check if the text STARTS with a quadrant intro sentence
    # e.g. "There are several opportunities for growth..."
    m = _QUAD_INTRO_PATTERNS.match(text.strip())
    if m:
        word = m.group(2).lower()
        if 'opportunit' in word: return 'opportunities'
        if 'threat'     in word: return 'threats'
        if 'weakness' in word or 'blind' in word: return 'weaknesses'
        if 'strength'   in word: return 'strengths'
    # Fall back: first keyword match anywhere in text
    for pattern, key in _QUAD_KEYWORD_MAP:
        if pattern.search(text):
            return key
    return None


def build_swot_lists_from_section_paragraphs(paragraphs: List[str]) -> Dict[str, List[str]]:
    """
    Parse free-text SWOT paragraphs into structured S/W/O/T lists.

    Strategy:
    1. Split each paragraph into individual points first.
    2. For EACH point, detect which quadrant it belongs to by scanning
       for intro sentences and keyword signals.
    3. Route accordingly — this handles cases where the LLM mixes
       Opportunities content into the Strengths paragraph.
    """
    swot: Dict[str, List[str]] = {
        "strengths": [], "weaknesses": [], "opportunities": [], "threats": []
    }
    current_key = "strengths"

    for para in paragraphs:
        # Detect quadrant from the paragraph-level header first
        detected = _detect_quadrant(para)
        if detected:
            current_key = detected

        # Split paragraph into individual points
        points = _split_swot_para_into_points(para)

        for point in points:
            # Re-detect quadrant at the point level — catches misplaced content
            point_quad = _detect_quadrant(point)
            target_key = point_quad if point_quad else current_key
            swot[target_key].append(point)

    return swot


def _split_swot_para_into_points(text: str) -> List[str]:
    """
    Split a SWOT paragraph that may contain multiple numbered/bulleted points
    into individual clean sentences.

    Handles:
      - "1. Point one. 2. Point two."     (inline numbered)
      - "1) Point one 2) Point two"        (parens style)
      - "• Point one • Point two"          (bullets)
      - plain prose paragraphs             (return as-is)
    """
    text = text.strip()
    if not text:
        return []

    # Remove wrapping list brackets that sometimes come from JSON parsing
    # e.g.  '["The team..."]'  →  'The team...'
    if text.startswith('[') and text.endswith(']'):
        inner = text[1:-1].strip()
        # Strip outer quotes if present
        if (inner.startswith('"') and inner.endswith('"')) or \
           (inner.startswith("'") and inner.endswith("'")):
            inner = inner[1:-1].strip()
        text = inner

    # ── Step 2: Truncate at second quadrant header if blob contains multiple ──
    # e.g. "...strengths. Threats include: 1. Risk..." → keep only pre-Threats part
    quad_pat = re.compile(
        r'(strengths?|weaknesses?|opportunities?|threats?)\s*[:\-]',
        re.IGNORECASE
    )
    quad_matches = list(quad_pat.finditer(text))
    if len(quad_matches) >= 2:
        text = text[:quad_matches[1].start()].strip()

    # ── Step 3: Strip leading quadrant label ──
    text = re.sub(
        r'^(strengths?|weaknesses?|opportunities?|threats?)\s*[:\-]\s*',
        '', text, flags=re.IGNORECASE
    ).strip()

    if not text:
        return []

    # ── Step 4: Split on numbered list markers ──
    # CRITICAL: do NOT split on decimal numbers like 2.67, 3.33
    # A valid list marker: <space><digits><period><space><non-digit>
    # An invalid split:    <digits><period><digits>  (decimal number)
    numbered = re.split(r'(?<=\S)\s+(?=\d+\.\s+[^\d])', text)
    if len(numbered) < 2:
        numbered = re.split(r'(?<=\S)\s+(?=\d+\)\s)', text)  # paren style "1) "

    if len(numbered) >= 2:
        clean: List[str] = []
        for part in numbered:
            part = re.sub(r'^\s*\d+[.)\]]\s*', '', part).strip()
            if part and len(part) > 15:  # skip decimal fragments like "67, indicates"
                clean.append(part)
        if len(clean) >= 2:
            return clean

    # ── Step 5: Split on bullet characters ──
    if any(ch in text for ch in ('\u2022', '\u25e6', '\u25b8', '\u00b7')):
        parts = re.split(r'\s*[\u2022\u25e6\u25b8\u00b7]\s*', text)
        clean = [p.strip() for p in parts if p.strip() and len(p.strip()) > 15]
        if len(clean) >= 2:
            return clean

    # ── Step 6: Plain prose — return as single item ──
    return [text] if text else []


_SWOT_KEYS = ("strengths", "weaknesses", "opportunities", "threats")
_SWOT_LABELS = {
    "strengths": "Strengths",
    "weaknesses": "Weaknesses",
    "opportunities": "Opportunities",
    "threats": "Threats",
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

    # Backward compatibility: older payloads may use "threat" (singular).
    if "threats" not in raw_swot and "threat" in raw_swot:
        raw_swot = dict(raw_swot)
        raw_swot["threats"] = raw_swot.get("threat", [])

    for key in _SWOT_KEYS:
        values = raw_swot.get(key, [])
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, list):
            continue
        cleaned: List[str] = []
        for value in values:
            text = str(value or "").strip()
            # Strip wrapping list brackets that appear when the LLM returns
            # a single-element JSON array as a string, e.g. '["The team..."]'
            if text.startswith('[') and text.endswith(']'):
                inner = text[1:-1].strip()
                if (inner.startswith('"') and inner.endswith('"')) or \
                   (inner.startswith("'") and inner.endswith("'")):
                    inner = inner[1:-1].strip()
                text = inner
            if not text:
                continue
            # If the item still looks like a multi-point blob, split it
            sub_points = _split_swot_para_into_points(text)
            cleaned.extend(sub_points)
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

def _clean_paragraphs(paragraphs: List[str]) -> List[str]:
    """
    Apply bracket artifact removal to every paragraph and drop blanks.
    Handles both single-element and multi-element JSON array strings.
    Ensures no raw JSON brackets or stray quotes appear in rendered output.
    """
    cleaned = []
    for p in paragraphs:
        # _expand_bracket_array handles '["a", "b"]' → ['a', 'b']
        # and plain strings → [clean_string]
        cleaned.extend(_expand_bracket_array(str(p or "")))
    return [p for p in cleaned if p]


def normalize_single_report(
    report: Any, report_type: str, data: dict, report_title_map: Dict[str, str]
) -> Dict[str, Any]:
    if isinstance(report, dict) and "sections" in report:
        sections = []
        for section in report.get("sections", []):
            raw_paragraphs = section.get("paragraphs") or text_to_paragraphs(section.get("text", ""))
            # ✅ Strip any stray JSON brackets / quotes from every paragraph
            paragraphs = _clean_paragraphs(raw_paragraphs)
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
        "sections": [{"id": "", "title": "Report", "paragraphs": _clean_paragraphs(text_to_paragraphs(text))}],
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


@lru_cache(maxsize=8)
def _load_stage_image_data_uri(stage_name: str) -> Optional[str]:
    """Load a stage-specific illustration as a base64 data URI.

    Image file naming convention (place in the html/ template dir):
      sunshine_stage.png   — for the Sunshine / Honeymoon stage
      self_introspection_stage.png
      soul_searching_stage.png
      steady_state_stage.png
    """
    safe_name = re.sub(r"[\s\-]+", "_", stage_name.lower().strip())
    candidates = [
        f"{safe_name}_stage",
        safe_name,
    ]
    for stem in candidates:
        for ext in ("png", "jpg", "jpeg", "webp"):
            image_path = os.path.join(Config.TEMPLATE_DIR, f"{stem}.{ext}")
            if os.path.exists(image_path):
                try:
                    with open(image_path, "rb") as handle:
                        encoded = base64.b64encode(handle.read()).decode("ascii")
                    mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
                    return f"data:{mime};base64,{encoded}"
                except OSError:
                    pass
    return None


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
        if not str(header.get("stage_image_data_uri") or "").strip():
            dominant_stage = str(header.get("dominant_stage") or "").strip()
            if dominant_stage:
                stage_img_uri = _load_stage_image_data_uri(dominant_stage)
                if stage_img_uri:
                    header["stage_image_data_uri"] = stage_img_uri
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
