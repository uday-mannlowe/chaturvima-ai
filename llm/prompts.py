"""
llm/prompts.py
All LLM prompt strings and section specifications.

Extracted from: generate_groq.py  (lines ~344–1042 approx.)

Contains:
  - TONE_GUIDELINES
  - SYSTEM_PROMPT
  - DEV_PROMPT_1D, DEV_PROMPT_2D, DEV_PROMPT_3D, DEV_PROMPT_4D
  - REPORT_STYLE dict
  - SectionSpec dataclass
  - SECTION_SPECS_EMPLOYEE, SECTION_SPECS_BOSS, SECTION_SPECS_TEAM, SECTION_SPECS_ORGANIZATION
  - REPORT_TITLE_MAP
  - DEFAULT_REPORT_TYPE_BY_DIMENSION
"""

# TODO: Move all prompt strings and SectionSpec definitions here.
#
# Steps:
#   1. Copy the block from generate_groq.py into this file
#   2. In generate_groq.py replace with:
#         from llm.prompts import (
#             SYSTEM_PROMPT, TONE_GUIDELINES, DEV_PROMPT_1D, ...,
#             SectionSpec, SECTION_SPECS_EMPLOYEE, ...,
#             REPORT_TITLE_MAP, DEFAULT_REPORT_TYPE_BY_DIMENSION,
#         )

raise NotImplementedError(
    "llm/prompts.py is a migration stub. "
    "Move prompts and SectionSpec from generate_groq.py here."
)
