"""
llm/groq_client.py
Groq API client initialisation and model configuration.

Extracted from: generate_groq.py  (lines 1–238)

Contains:
  - Environment-driven model name constants
  - MODEL_BY_DIMENSION, MODEL_BY_REPORT_TYPE, MODEL_BY_REPORT_TYPE_DEDICATED
  - create_groq_client()
  - _dedupe_models(), _resolve_model_candidates()
  - _is_rate_limited_error(), _extract_retry_wait_seconds()
  - _call_groq_with_model_fallback()   ← core retry+fallback logic
"""

# TODO: Move the relevant lines out of generate_groq.py into this file.
#
# Steps:
#   1. Copy lines 1–238 from generate_groq.py into this file
#   2. Replace `from generate_groq import ...` with `from llm.groq_client import ...`
#      wherever those symbols are used (worker_pool.py, json_report_routes.py, etc.)
#   3. In generate_groq.py, replace the pasted block with:
#         from llm.groq_client import (
#             MODEL_BY_REPORT_TYPE_DEDICATED, create_groq_client,
#             _call_groq_with_model_fallback, ...
#         )

raise NotImplementedError(
    "llm/groq_client.py is a migration stub. "
    "Move the Groq client code from generate_groq.py here."
)
