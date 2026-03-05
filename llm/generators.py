"""
llm/generators.py
Top-level report generation functions (the public API of the LLM layer).

Extracted from: generate_groq.py  (lines ~1042–end)

Contains:
  - Data loading helpers:
      normalize_dimension(), validate_input_data(),
      load_input_json_from_path(), load_input_json_from_dimension(),
      resolve_input_data()
  - Frappe mapping:
      map_frappe_to_nd(), detect_dimension()
  - Section generation internals:
      _generate_section_text(), _expand_section_text(), _generate_one_section()
  - Top-level generators (PUBLIC API):
      generate_structured_report()
      generate_structured_report_by_dimension()
      generate_multi_reports_structured()
      generate_report_as_json()
      generate_primary_report_json()
      generate_multi_reports_json()
      generate_single_report()
      generate_multi_reports()
      generate_text_report()
"""

# TODO: Move the generator functions here.
#
# Steps:
#   1. Copy lines ~1042–end from generate_groq.py into this file
#   2. Update imports in this file to use the sub-modules:
#         from llm.groq_client import _call_groq_with_model_fallback, ...
#         from llm.rag import rag_system, retrieve_rag_context
#         from llm.prompts import SECTION_SPECS_EMPLOYEE, SYSTEM_PROMPT, ...
#   3. In generate_groq.py replace with re-exports:
#         from llm.generators import (
#             generate_text_report, generate_multi_reports, ...
#         )
#   4. Update main.py / worker_pool imports if needed.

raise NotImplementedError(
    "llm/generators.py is a migration stub. "
    "Move generator functions from generate_groq.py here."
)
