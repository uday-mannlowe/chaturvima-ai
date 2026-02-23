import os
import re
import json
import argparse
import threading
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq
from typing import List, Dict, Any, Tuple

# RAG imports
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS


# LOAD ENV

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    raise ValueError("❌ GROQ_API_KEY not found in .env")

# INIT GROQ CLIENT
def create_groq_client() -> Groq:
    return Groq(api_key=GROQ_API_KEY)

# Choose your Groq model
# Options: "llama-3.3-70b-versatile", "llama-3.1-70b-versatile", "mixtral-8x7b-32768"
MODEL_NAME = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")

# RAG SYSTEM CLASS

class BehavioralStagesRAG:
    """
    RAG system for retrieving behavioral stage definitions and context.
    """
    
    def __init__(self, vector_db_path: str = "vector_db/behavioral_stages_semantic"):
        print("🔄 Loading RAG system...")
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        
        try:
            self.vectorstore = FAISS.load_local(
                vector_db_path,
                self.embeddings,
                allow_dangerous_deserialization=True
            )
            print(f"✅ RAG system loaded from {vector_db_path}")
        except Exception as e:
            print(f"⚠️ Warning: Could not load vector store: {e}")
            print("   RAG will be disabled for this session.")
            self.vectorstore = None
    
    def retrieve_stage_context(self, stage_name: str, k: int = 2) -> str:
        """
        Retrieve context for a specific behavioral stage.
        
        Args:
            stage_name: Name of the stage (e.g., "Honeymoon Stage")
            k: Number of chunks to retrieve
            
        Returns:
            Formatted context string
        """
        if self.vectorstore is None:
            return ""
        
        query = f"{stage_name} characteristics and behaviors"
        docs = self.vectorstore.similarity_search(query, k=k)
        
        context_parts = []
        for doc in docs:
            context_parts.append(
                f"[{doc.metadata.get('section_name', 'Unknown')}]\n{doc.page_content}"
            )
        
        return "\n\n".join(context_parts)
    
    def retrieve_multi_stage_context(self, stages: List[str], k_per_stage: int = 2) -> str:
        """
        Retrieve context for multiple stages.
        
        Args:
            stages: List of stage names
            k_per_stage: Number of chunks per stage
            
        Returns:
            Combined context string
        """
        if self.vectorstore is None:
            return ""
        
        all_context = []
        for stage in stages:
            context = self.retrieve_stage_context(stage, k=k_per_stage)
            if context:
                all_context.append(f"=== {stage.upper()} ===\n{context}")
        
        return "\n\n".join(all_context)
    
    def retrieve_behavioral_indicators(self, behavior_description: str, k: int = 3) -> str:
        """
        Retrieve relevant behavioral indicators based on description.
        
        Args:
            behavior_description: Description of observed behavior
            k: Number of chunks to retrieve
            
        Returns:
            Formatted context string
        """
        if self.vectorstore is None:
            return ""
        
        docs = self.vectorstore.similarity_search(behavior_description, k=k)
        
        context_parts = []
        for doc in docs:
            stage = doc.metadata.get('stage', 'Unknown Stage')
            section = doc.metadata.get('section_name', 'Unknown Section')
            context_parts.append(
                f"[{stage} - {section}]\n{doc.page_content}"
            )
        
        return "\n\n".join(context_parts)


# INITIALIZE RAG SYSTEM (GLOBAL)

rag_system = BehavioralStagesRAG()
rag_lock = threading.Lock()


# TONE GUIDELINES (BASED ON STAGE/SUBSTAGE)

TONE_GUIDELINES = """
TONE ADAPTATION BASED ON STAGE & SUBSTAGE:

**Honeymoon Stage:**
- Excitement & Optimism: Inspirational & Affirming (positive, energizing, future-oriented, validating confidence)
- Confidence & Over-Reliance on Past Success: Respectful yet Gently Grounding (appreciative, subtly present new context, non-confrontational)
- Initial Reality Check: Reassuring & Calibrating (supportive, calming, normalize challenges)
- Sustained Confidence with Subtle Complacency: Encouraging with Wake-Up Signals (balanced praise, mild urgency)

**Self-Reflection Stage:**
- Acknowledgment of Problems: Neutral & Observational (fact-based, non-judgmental, descriptive)
- Analyzing Cause: Analytical & Curious (diagnostic, structured, process-focused, avoids blame)
- Partial Acceptance of Responsibility: Constructive & Empowering (validating insight, reinforce accountability)
- Exploration of Solutions: Solution-Oriented & Supportive (forward-looking, collaborative, practical)

**Soul Searching Stage:**
- Deep Frustration: Highly Empathetic & Stabilizing (compassionate, validating struggle, emotionally safe)
- Questioning Fundamentals: Reflective & Thought-Provoking (calm, philosophical, invite deep insight)
- Openness to Change: Encouraging & Reassurance-Based (hopeful, confidence-building, normalize uncertainty)
- Actionable Transformation: Motivational & Directive (clear, decisive, action-focused, optimistic but grounded)

**Steady State Stage:**
- Stability & Alignment: Affirmative & Reinforcing (calm, confident, acknowledge effectiveness)
- Operational Predictability: Assuring & Confidence-Building (steady, matter-of-fact, reliability-focused)
- Emerging Challenges: Proactively Alerting (Non-Alarming) (observant, anticipatory, encourage readiness)
- Dynamic Balance: Strategic & Forward-Looking (vision-oriented, mature, continuous-improvement mindset)

APPLY THE APPROPRIATE TONE THROUGHOUT THE REPORT BASED ON THE IDENTIFIED STAGE AND SUBSTAGE.
"""


# MASTER SYSTEM PROMPT (GLOBAL)

SYSTEM_PROMPT = """
You are a senior behavioral diagnostics and organizational assessment expert
working within the ChaturVima framework.

You generate long-form, consulting-grade diagnostic reports
for individuals, relationships, teams, and organizations.

Give report in simple language that is easy to understand.

use your own reasoning to generate the report based on the input data and reference material.

GLOBAL RULES:
- Use ONLY the provided input data and reference material
- Do NOT invent scores, facts, stages, or causes
- Do NOT diagnose mental health or label individuals
- Do NOT assign blame or intent
- Interpret patterns, not personalities
- Maintain a professional, neutral, and developmental tone
- Write in clear, structured language suitable for HR and leadership review
- Keep the content of the report in detail and in layman terms so that it is easy to understand for everyone
- Use the behavioral stage definitions provided in the reference material to explain stages accurately
- Adapt your tone based on the stage and substage identified (see tone guidelines)
"""

# DEVELOPER PROMPTS (DIMENSION-SPECIFIC)

DEV_PROMPT_1D = """
Generate a FULL, IN-DEPTH 1D Individual Employee Diagnostic Report.

Report Title: "Employee Personal Insights"

Audience:
- Employee

MANDATORY SECTIONS (MULTI-PARAGRAPH EACH):

1. Purpose of the Assessment
   - Explain why this assessment was conducted
   - Set context for the individual dimension

2. Dimension Overview & Scope
   - Describe what the 1D dimension covers
   - Clarify the scope and limitations

3. Inputs & Assessment Instruments
   - List the data sources and methods used
   - Explain the assessment approach

   
4. Employee Profile Summary
   - Summarize key employee information
   - Include role, department, and relevant context

5. Emotional Stage Interpretation (Stage and Sub stage)
   5.1. Definition of Dominant Stage and Pre Dominant Sub stage
        - Use reference material to define the stages accurately
        - Explain what each stage means in clear terms
   5.2. Interpretation
        - Interpret the employee's current emotional stage
        - Connect it to observed behaviors and patterns
   5.3. Behavioural Indicators
        - List specific behavioral indicators from reference material
        - Show how these manifest in the employee's case
    5.4. Stage Level Score Summary Scoring (Table) and Interpretation
        - Provide a table summarizing stage level scores
        - Interpret what these scores indicate about the employee

6. Psychological Profile & Tendencies
   - Describe psychological patterns and tendencies
   - Connect to emotional stage characteristics

7. Internal Drivers & Stressors
   - Identify what motivates the employee
   - Highlight sources of stress and tension

8. Individual SWOT Analysis
   - Strengths: What the employee does well
   - Weaknesses (frame as blind spots): Areas for development
   - Opportunities: Growth potential areas
   - Threats: Risks to performance and wellbeing

9. Action Navigator – Personal Improvement Plan
   - Provide phase-wise action plan
   - Make it practical, actionable, and developmental
   - Focus on sustainable growth and self-awareness

10. Value Contribution to Higher Dimensions
    - Explain how individual growth impacts team and organization
    - Connect personal development to broader organizational value

TONE:
- Inspirational, developmental, and encouraging
- Use appropriate tone based on stage/substage (refer to tone guidelines)
- Frame weaknesses as blind spots, not failures
- Focus on self-awareness and sustainable growth

DEPTH:
- 8-10 pages equivalent
- Each section should be detailed and comprehensive
- Use layman terms for easy understanding

CRITICAL:
- Use stage definitions from reference material
- Do not invent stages or characteristics
"""

DEV_PROMPT_2D = """
Generate a FULL, IN-DEPTH 2D Employee-Boss Relationship Diagnostic Report.

Report Title: "Employee-Boss Relationship Assessment"

Audience:
- Boss/Manager
- HR/Leadership (if applicable)

MANDATORY SECTIONS (MULTI-PARAGRAPH EACH):

1. Purpose of the Assessment
   - Explain the 2D relational assessment
   - Set context for boss-employee dynamics

2. Dimension Overview & Scope
   - Describe the 2D relational dimension
   - Clarify assessment boundaries

3. Inputs & Assessment Instruments
   - List data sources from both parties
   - Explain relationship assessment methods

4. Relationship Profile Summary
   - Summarize the working relationship
   - Include history and context

5. Employee Perspective on the Relationship
   - Document employee's view of the relationship
   - Identify stated challenges and concerns

6. Boss Perspective on the Relationship  
   - Document boss's view of the relationship
   - Identify management observations

7. Relationship Stage Diagnosis
   - Identify current relational stage and substage
   - Explain using reference material
   - Provide evidence from input data

8. Communication & Interaction Patterns
   - Analyze communication dynamics
   - Identify interaction patterns

9. Trust & Alignment Assessment
   - Evaluate trust levels
   - Assess goal and value alignment

10. Conflict & Tension Points
    - Identify areas of friction
    - Analyze sources of conflict

11. Root Cause Assessment
    - Explore underlying relational factors
    - Connect patterns to root causes

12. Implications & Impact
    - Discuss current impact on performance
    - Project future implications

13. Recommendations for the Boss
    - Provide boss-focused interventions
    - Suggest management adjustments

14. Recommendations for Joint Action
    - Suggest collaborative improvements
    - Outline relationship-building steps

15. Next Steps & Development Path
    - Outline immediate actions
    - Suggest long-term relationship development

16. Closing Notes
    - Summarize relationship assessment
    - Provide constructive outlook

Write in full, detailed paragraphs with concrete examples from the data.
Do NOT use bullet points. Use flowing narrative structure.
Focus on the RELATIONSHIP dynamics, not individual psychology.
"""

DEV_PROMPT_3D = """
Generate a FULL, IN-DEPTH 3D Team Diagnostic Report.

Report Title: "Team Assessment Report"

Audience:
- Team Leader
- Team Members
- HR/Leadership

MANDATORY SECTIONS (MULTI-PARAGRAPH EACH):

1. Purpose of the Assessment
2. Dimension Overview & Scope
3. Inputs & Assessment Instruments
4. Team Profile Summary
5. Team Stage Diagnosis
6. Team Dynamics & Collaboration Patterns
7. Communication & Interaction Analysis
8. Trust & Psychological Safety
9. Performance & Productivity Patterns
10. Conflict & Tension Points
11. Root Cause Assessment
12. Implications & Impact
13. Recommendations for Team Development
14. Next Steps & Development Path
15. Closing Notes

Write in full, detailed paragraphs with concrete examples from the data.
Do NOT use bullet points. Use flowing narrative structure.
"""

DEV_PROMPT_4D = """
Generate a FULL, IN-DEPTH 4D Organizational Diagnostic Report.

Report Title: "Organizational Assessment Report"

Audience:
- Executive Leadership
- Board
- HR Leadership

MANDATORY SECTIONS (MULTI-PARAGRAPH EACH):

1. Purpose of the Assessment
2. Dimension Overview & Scope
3. Inputs & Assessment Instruments
4. Organizational Profile Summary
5. Organizational Stage Diagnosis
6. Culture & Values Analysis
7. Leadership & Governance Patterns
8. Communication & Information Flow
9. Performance & Strategic Alignment
10. Systemic Issues & Challenges
11. Root Cause Assessment
12. Implications & Impact
13. Strategic Recommendations
14. Implementation Roadmap
15. Closing Notes

Write in full, detailed paragraphs with concrete examples from the data.
Do NOT use bullet points. Use flowing narrative structure.
"""

# ===================================================
# STRUCTURED REPORT SPECS (SECTION-WISE GENERATION)
# ===================================================

@dataclass(frozen=True)
class SectionSpec:
    id: str
    title: str
    min_words: int
    max_words: int
    guidance: str
    data_keys: Tuple[str, ...] = ()
    needs_rag: bool = False


REPORT_STYLE = {
    "employee": (
        "Audience: Employee.\n"
        "Tone: inspirational, developmental, and encouraging.\n"
        "Use clear, simple language and avoid jargon.\n"
        "Write in full paragraphs and avoid bullet points.\n"
        "Do not invent facts or scores."
    ),
    "boss": (
        "Audience: Manager/Boss (and HR/Leadership).\n"
        "Tone: direct, actionable, professional.\n"
        "Focus on relationship dynamics, not individual psychology.\n"
        "Write in full paragraphs and do not use bullet points.\n"
        "Do not invent facts or scores."
    ),
    "team": (
        "Audience: Team Leader, Team Members, HR/Leadership.\n"
        "Tone: professional, diagnostic, constructive.\n"
        "Write in full paragraphs and avoid bullet points."
    ),
    "organization": (
        "Audience: Executive Leadership, Board, HR Leadership.\n"
        "Tone: strategic, analytical, constructive.\n"
        "Write in full paragraphs and avoid bullet points."
    ),
}


SECTION_SPECS_EMPLOYEE: List[SectionSpec] = [
    SectionSpec(
        id="purpose",
        title="Purpose of the Assessment",
        min_words=220,
        max_words=320,
        guidance=(
            "Explain why this assessment was conducted and what it is meant to help the employee "
            "understand about their current experience and development."
        ),
        data_keys=("employee_context", "context", "employee"),
    ),
    SectionSpec(
        id="overview",
        title="Dimension Overview & Scope",
        min_words=220,
        max_words=320,
        guidance=(
            "Describe what the individual dimension covers and clarify the scope and limitations of this "
            "employee-focused assessment."
        ),
        data_keys=("dimension", "employee_context", "employee"),
    ),
    SectionSpec(
        id="inputs",
        title="Inputs & Assessment Instruments",
        min_words=220,
        max_words=320,
        guidance=(
            "Describe the data sources and assessment methods used (e.g., questionnaires, weights, "
            "context inputs). Keep it concrete and accessible."
        ),
        data_keys=("employee_questionnaire", "revised_employee_model_weights", "employee", "context"),
    ),
    SectionSpec(
        id="profile",
        title="Employee Profile Summary",
        min_words=260,
        max_words=360,
        guidance=(
            "Summarize key employee information such as role, department, tenure, and relevant "
            "context. Use only the provided data."
        ),
        data_keys=("employee_context", "employee"),
    ),
    SectionSpec(
        id="stage",
        title="Emotional Stage Interpretation",
        min_words=700,
        max_words=900,
        guidance=(
            "Cover these subparts in order using clear paragraph openings: "
            "(1) Definition of Dominant Stage and Pre-Dominant Substage, "
            "(2) Interpretation of the employee's current stage, "
            "(3) Behavioral indicators tied to the reference material, "
            "(4) Stage level score summary and interpretation (describe as a narrative if no table data exists)."
        ),
        data_keys=("behavioral_stage", "employee", "employee_questionnaire", "revised_employee_model_weights"),
        needs_rag=True,
    ),
    SectionSpec(
        id="psych_profile",
        title="Psychological Profile & Tendencies",
        min_words=300,
        max_words=420,
        guidance=(
            "Describe psychological patterns and tendencies that are evident from the data. "
            "Connect these to the identified stage characteristics without labeling or diagnosing."
        ),
        data_keys=("behavioral_stage", "employee", "employee_questionnaire"),
    ),
    SectionSpec(
        id="drivers",
        title="Internal Drivers & Stressors",
        min_words=280,
        max_words=380,
        guidance=(
            "Identify what motivates the employee and what sources of stress or tension are visible "
            "in the data. Tie back to observed patterns."
        ),
        data_keys=("employee", "employee_questionnaire", "recommendation_framework"),
    ),
    SectionSpec(
        id="swot",
        title="Individual SWOT Analysis",
        min_words=300,
        max_words=420,
        guidance=(
            "Discuss strengths, blind spots (weaknesses), opportunities, and threats as narrative "
            "paragraphs grounded in the provided SWOT data."
        ),
        data_keys=("individual_swot",),
    ),
    SectionSpec(
        id="action_plan",
        title="Action Navigator – Personal Improvement Plan",
        min_words=480,
        max_words=650,
        guidance=(
            "Provide a phase-wise action plan that is practical and developmental. "
            "Include immediate, short-term, and mid-term steps based on the recommendations."
        ),
        data_keys=("recommendation_framework", "employee", "employee_questionnaire"),
    ),
    SectionSpec(
        id="value_contribution",
        title="Value Contribution to Higher Dimensions",
        min_words=220,
        max_words=320,
        guidance=(
            "Explain how individual growth connects to team and organizational value. "
            "Make the link between personal development and broader impact."
        ),
        data_keys=("employee_context", "employee"),
    ),
]


SECTION_SPECS_BOSS: List[SectionSpec] = [
    SectionSpec(
        id="purpose",
        title="Purpose of the Assessment",
        min_words=200,
        max_words=300,
        guidance=(
            "Explain the 2D relational assessment and set context for boss-employee dynamics."
        ),
        data_keys=("context", "employee", "boss"),
    ),
    SectionSpec(
        id="overview",
        title="Dimension Overview & Scope",
        min_words=200,
        max_words=300,
        guidance=(
            "Describe the 2D relational dimension and clarify assessment boundaries."
        ),
        data_keys=("context",),
    ),
    SectionSpec(
        id="inputs",
        title="Inputs & Assessment Instruments",
        min_words=200,
        max_words=300,
        guidance=(
            "List data sources from both parties and explain the relationship assessment methods."
        ),
        data_keys=("employee", "boss", "relationship_variables"),
    ),
    SectionSpec(
        id="profile",
        title="Relationship Profile Summary",
        min_words=240,
        max_words=340,
        guidance=(
            "Summarize the working relationship with relevant history and context."
        ),
        data_keys=("context", "relationship_variables", "superior_subordinate_dynamics"),
    ),
    SectionSpec(
        id="employee_perspective",
        title="Employee Perspective on the Relationship",
        min_words=240,
        max_words=340,
        guidance=(
            "Document the employee's view of the relationship and identify stated challenges."
        ),
        data_keys=("employee",),
    ),
    SectionSpec(
        id="boss_perspective",
        title="Boss Perspective on the Relationship",
        min_words=240,
        max_words=340,
        guidance=(
            "Document the boss's view of the relationship and identify management observations."
        ),
        data_keys=("boss",),
    ),
    SectionSpec(
        id="stage_diagnosis",
        title="Relationship Stage Diagnosis",
        min_words=300,
        max_words=420,
        guidance=(
            "Identify the current relational stage and substage, explain using reference material, "
            "and provide evidence from input data."
        ),
        data_keys=("employee", "boss", "relationship_variables", "superior_subordinate_dynamics"),
        needs_rag=True,
    ),
    SectionSpec(
        id="communication_patterns",
        title="Communication & Interaction Patterns",
        min_words=260,
        max_words=360,
        guidance=(
            "Analyze communication dynamics and interaction patterns using concrete examples."
        ),
        data_keys=("employee", "boss", "relationship_variables"),
    ),
    SectionSpec(
        id="trust_alignment",
        title="Trust & Alignment Assessment",
        min_words=240,
        max_words=340,
        guidance=(
            "Evaluate trust levels and assess goal and value alignment."
        ),
        data_keys=("relationship_variables",),
    ),
    SectionSpec(
        id="conflict",
        title="Conflict & Tension Points",
        min_words=240,
        max_words=340,
        guidance=(
            "Identify areas of friction and analyze sources of conflict."
        ),
        data_keys=("relationship_variables", "superior_subordinate_dynamics"),
    ),
    SectionSpec(
        id="root_cause",
        title="Root Cause Assessment",
        min_words=240,
        max_words=340,
        guidance=(
            "Explore underlying relational factors and connect patterns to root causes."
        ),
        data_keys=("relationship_variables", "superior_subordinate_dynamics"),
    ),
    SectionSpec(
        id="implications",
        title="Implications & Impact",
        min_words=240,
        max_words=340,
        guidance=(
            "Discuss current impact on performance and project future implications."
        ),
        data_keys=("relationship_variables", "superior_subordinate_dynamics"),
    ),
    SectionSpec(
        id="boss_recommendations",
        title="Recommendations for the Boss",
        min_words=300,
        max_words=420,
        guidance=(
            "Provide boss-focused interventions and suggest management adjustments."
        ),
        data_keys=("superior_subordinate_dynamics", "relationship_variables"),
    ),
    SectionSpec(
        id="joint_recommendations",
        title="Recommendations for Joint Action",
        min_words=260,
        max_words=360,
        guidance=(
            "Suggest collaborative improvements and outline relationship-building steps."
        ),
        data_keys=("superior_subordinate_dynamics", "relationship_variables"),
    ),
    SectionSpec(
        id="next_steps",
        title="Next Steps & Development Path",
        min_words=220,
        max_words=320,
        guidance=(
            "Outline immediate actions and suggest a long-term relationship development path."
        ),
        data_keys=("superior_subordinate_dynamics", "relationship_variables"),
    ),
    SectionSpec(
        id="closing",
        title="Closing Notes",
        min_words=200,
        max_words=300,
        guidance=(
            "Summarize the relationship assessment and provide a constructive outlook."
        ),
        data_keys=("context",),
    ),
]


REPORT_SPECS: Dict[str, List[SectionSpec]] = {
    "employee": SECTION_SPECS_EMPLOYEE,
    "boss": SECTION_SPECS_BOSS,
}

# ✅ NEW: Map of report types for each dimension
REPORT_TYPE_MAP = {
    "1D": ["employee"],
    "2D": ["employee", "boss"],
    "3D": ["employee", "boss", "team"],
    "4D": ["employee", "boss", "team", "organization"]
}

# Default report type when generating a single report per dimension
DEFAULT_REPORT_TYPE_BY_DIMENSION = {
    "1D": "employee",
    "2D": "boss",
    "3D": "team",
    "4D": "organization"
}

# ✅ NEW: Map of prompts for each report type
PROMPT_MAP = {
    "employee": DEV_PROMPT_1D,
    "boss": DEV_PROMPT_2D,
    "team": DEV_PROMPT_3D,
    "organization": DEV_PROMPT_4D
}

# ✅ NEW: Report titles for each type
REPORT_TITLE_MAP = {
    "employee": "Individual Self-Assessment Report",
    "boss": "Employee-Boss Relationship Assessment",
    "team": "Team Assessment Report",
    "organization": "Organizational Assessment Report"
}

# Keep the old map for backward compatibility with single report generation
DEV_PROMPT_MAP = {
    "1D": DEV_PROMPT_1D,
    "2D": DEV_PROMPT_2D,
    "3D": DEV_PROMPT_3D,
    "4D": DEV_PROMPT_4D
}


# DATA VALIDATION & NORMALIZATION

VALID_DIMENSIONS = ["1D", "2D", "3D", "4D"]

def normalize_dimension(dim_input: Any) -> str:
    if isinstance(dim_input, int):
        if 1 <= dim_input <= 4:
            return f"{dim_input}D"
        else:
            raise ValueError(f"Dimension must be 1-4, got {dim_input}")
    
    if isinstance(dim_input, str):
        dim_upper = dim_input.upper().strip()
        if dim_upper in VALID_DIMENSIONS:
            return dim_upper
        elif dim_upper in ["1", "2", "3", "4"]:
            return f"{dim_upper}D"
        else:
            raise ValueError(f"Invalid dimension string: {dim_input}")
    
    raise ValueError(f"Dimension must be int (1-4) or str ('1D'-'4D'), got {type(dim_input)}")


def validate_input_data(data: dict) -> dict:
    if "dimension" not in data:
        raise ValueError("Input data must include 'dimension' field")
    
    data["dimension"] = normalize_dimension(data["dimension"])
    return data


def load_input_json_from_path(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return validate_input_data(raw)


def load_input_json_from_dimension(dimension: str) -> dict:
    normalized = normalize_dimension(dimension)
    default_path = os.path.join("data", f"{normalized}.json")
    
    if not os.path.exists(default_path):
        raise FileNotFoundError(f"Default data file not found: {default_path}")
    
    return load_input_json_from_path(default_path)


def _safe_data_path(data_dir: str, file_name: str) -> str:
    base = os.path.abspath(data_dir)
    target = os.path.abspath(os.path.join(base, file_name))
    if not target.startswith(base + os.sep):
        raise ValueError("Invalid data_file path")
    return target


def resolve_input_data(payload: Dict[str, Any], data_dir: str = "data") -> Dict[str, Any]:
    """
    ✅ FIXED: Enhanced with dimension preservation throughout all code paths
    """
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object")

    # ✅ CRITICAL FIX: Capture requested dimension FIRST
    requested_dimension = None
    if "dimension" in payload:
        try:
            requested_dimension = normalize_dimension(payload["dimension"])
            print(f"🎯 Requested dimension: {requested_dimension}")
        except ValueError as e:
            raise ValueError(f"Invalid dimension in payload: {e}")

    # Load data based on payload structure
    if isinstance(payload.get("data"), dict):
        print("📦 Using embedded data from payload")
        data = payload["data"].copy()  # ✅ FIX: Use the nested data directly
        # ✅ FIX: Don't validate yet, we'll add dimension first
    elif "data_file" in payload:
        print(f"📂 Loading data from file: {payload['data_file']}")
        data_path = _safe_data_path(data_dir, str(payload["data_file"]))
        data = load_input_json_from_path(data_path)
    elif "dimension" in payload and len(payload.keys()) == 1:
        print(f"📚 Loading default data for dimension: {payload['dimension']}")
        data = load_input_json_from_dimension(payload["dimension"])
    else:
        print("📋 Using payload directly as data")
        data = payload.copy()  # ✅ FIX: Use copy to avoid modifying original
    
    # ✅ CRITICAL FIX: Force dimension to match original request
    # This ensures API payload dimension always wins
    if requested_dimension:
        if data.get("dimension") != requested_dimension:
            print(f"🔧 Forcing dimension: {data.get('dimension')} → {requested_dimension}")
        data["dimension"] = requested_dimension
    
    # ✅ FIX: Validate AFTER adding dimension
    data = validate_input_data(data)
    
    return data


def extract_stages_from_data(data: dict) -> List[str]:
    """
    Extract all mentioned stages from the input data.
    
    Args:
        data: Input JSON data
        
    Returns:
        List of unique stage names
    """
    stages = set()
    
    # Helper function to extract stage from nested structures
    def find_stages(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if 'stage' in key.lower() and isinstance(value, str):
                    # Clean up stage name
                    stage_name = value.replace('_', ' ').title()
                    if 'stage' not in stage_name.lower():
                        stage_name = f"{stage_name} Stage"
                    stages.add(stage_name)
                find_stages(value)
        elif isinstance(obj, list):
            for item in obj:
                find_stages(item)
    
    find_stages(data)
    return list(stages)


def retrieve_rag_context(data: dict) -> str:
    """
    Retrieve relevant behavioral stage context using RAG.
    
    Args:
        data: Input JSON data containing stage information
        
    Returns:
        Formatted context string with stage definitions
    """
    if rag_system.vectorstore is None:
        return "⚠️ RAG system not available. Stage definitions will be based on LLM knowledge only."
    
    print("🔍 Extracting stages from input data...")
    stages = extract_stages_from_data(data)
    
    if not stages:
        print("⚠️ No stages found in input data. Using general retrieval.")
        # Fallback: retrieve based on common stage names
        stages = ["Honeymoon Stage", "Self-Reflection Stage", 
                  "Soul Searching Stage", "Steady State Stage"]
    
    print(f"📚 Retrieving context for stages: {', '.join(stages)}")
    
    # Retrieve context for all identified stages
    context = rag_system.retrieve_multi_stage_context(stages, k_per_stage=2)
    
    if context:
        header = """
========================================
BEHAVIORAL STAGES REFERENCE MATERIAL
========================================
The following definitions and characteristics are from the authoritative 
ChaturVima behavioral stages framework. Use these to accurately describe 
and interpret the stages mentioned in the input data.

"""
        return header + context
    else:
        return "⚠️ No relevant context retrieved from RAG system."


# ===================================================
# STRUCTURED REPORT GENERATION HELPERS
# ===================================================

def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text or ""))


def _split_paragraphs(text: str) -> List[str]:
    if not text:
        return []
    parts = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    if len(parts) <= 1:
        parts = [p.strip() for p in text.splitlines() if p.strip()]
    return parts


def _select_section_data(data: dict, data_keys: Tuple[str, ...]) -> dict:
    selected = {"dimension": data.get("dimension")}
    for key in data_keys:
        if key in data:
            selected[key] = data[key]
    if len(selected.keys()) <= 1:
        return data
    return selected


def _max_tokens_for_section(spec: SectionSpec) -> int:
    return max(600, min(int(spec.max_words * 1.4), 2000))


def _build_section_prompt(
    report_type: str,
    spec: SectionSpec,
    section_data: dict,
    rag_context: str,
    prior_sections: List[str],
    index: int,
    total: int
) -> str:
    prior_block = ", ".join(prior_sections) if prior_sections else "None"
    rag_block = rag_context if spec.needs_rag else ""

    prompt_parts = [
        f"SECTION {index}/{total}: {spec.title}",
        f"SECTION GOAL: {spec.guidance}",
        f"WORD COUNT: {spec.min_words}-{spec.max_words} words.",
        "FORMAT: Write in full paragraphs only. Do not include bullet points or headings.",
        f"PRIOR SECTIONS ALREADY WRITTEN: {prior_block}",
        "Only use the data provided below. Do not invent facts or scores.",
        "DATA (JSON):",
        json.dumps(section_data, indent=2),
    ]

    if rag_block:
        prompt_parts.insert(
            5,
            "REFERENCE MATERIAL (AUTHORITATIVE):\n" + rag_block
        )

    return "\n\n".join(prompt_parts)


def _build_expansion_prompt(
    spec: SectionSpec,
    section_data: dict,
    rag_context: str,
    existing_text: str,
    additional_words: int
) -> str:
    rag_block = rag_context if spec.needs_rag else ""
    prompt_parts = [
        f"Expand the section '{spec.title}' by at least {additional_words} words.",
        "Add new paragraphs with deeper analysis and concrete examples grounded in the data.",
        "Do not repeat sentences or ideas already written. Do not add headings or bullet points.",
        "CURRENT SECTION:",
        existing_text.strip(),
        "DATA (JSON):",
        json.dumps(section_data, indent=2),
    ]
    if rag_block:
        prompt_parts.insert(
            4,
            "REFERENCE MATERIAL (AUTHORITATIVE):\n" + rag_block
        )
    return "\n\n".join(prompt_parts)


def _generate_section_text(
    data: dict,
    report_type: str,
    spec: SectionSpec,
    rag_context: str,
    prior_sections: List[str],
    index: int,
    total: int
) -> str:
    section_data = _select_section_data(data, spec.data_keys)
    user_prompt = _build_section_prompt(
        report_type=report_type,
        spec=spec,
        section_data=section_data,
        rag_context=rag_context,
        prior_sections=prior_sections,
        index=index,
        total=total,
    )

    system_prompt = "\n\n".join([
        SYSTEM_PROMPT,
        TONE_GUIDELINES,
        REPORT_STYLE.get(report_type, ""),
    ]).strip()

    client = create_groq_client()
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2,
        max_tokens=_max_tokens_for_section(spec),
    )

    return response.choices[0].message.content.strip()


def _expand_section_text(
    data: dict,
    report_type: str,
    spec: SectionSpec,
    rag_context: str,
    existing_text: str,
    additional_words: int
) -> str:
    section_data = _select_section_data(data, spec.data_keys)
    user_prompt = _build_expansion_prompt(
        spec=spec,
        section_data=section_data,
        rag_context=rag_context,
        existing_text=existing_text,
        additional_words=additional_words,
    )

    system_prompt = "\n\n".join([
        SYSTEM_PROMPT,
        TONE_GUIDELINES,
        REPORT_STYLE.get(report_type, ""),
    ]).strip()

    client = create_groq_client()
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2,
        max_tokens=min(1200, _max_tokens_for_section(spec)),
    )
    expanded = response.choices[0].message.content.strip()
    return f"{existing_text.strip()}\n\n{expanded}".strip()


def generate_structured_report(data: dict, report_type: str, rag_context: str) -> Dict[str, Any]:
    """
    Generate a structured report with section-wise content.
    """
    if report_type not in PROMPT_MAP:
        raise ValueError(f"Invalid report type: {report_type}. Valid types: {list(PROMPT_MAP.keys())}")

    specs = REPORT_SPECS.get(report_type)
    if not specs:
        text_report = generate_single_report(data, report_type, rag_context)
        return {
            "title": REPORT_TITLE_MAP.get(report_type, report_type),
            "report_type": report_type,
            "dimension": data.get("dimension"),
            "sections": [
                {
                    "id": "report",
                    "title": "Report",
                    "text": text_report,
                    "paragraphs": _split_paragraphs(text_report),
                    "word_count": _word_count(text_report),
                }
            ],
            "word_count": _word_count(text_report),
            "generated_at": datetime.now().isoformat(),
        }

    sections = []
    prior_titles: List[str] = []
    total_specs = len(specs)

    for idx, spec in enumerate(specs, start=1):
        section_text = _generate_section_text(
            data=data,
            report_type=report_type,
            spec=spec,
            rag_context=rag_context,
            prior_sections=prior_titles,
            index=idx,
            total=total_specs,
        )

        words = _word_count(section_text)
        attempts = 0
        while words < spec.min_words and attempts < 2:
            shortfall = max(120, spec.min_words - words)
            section_text = _expand_section_text(
                data=data,
                report_type=report_type,
                spec=spec,
                rag_context=rag_context,
                existing_text=section_text,
                additional_words=shortfall,
            )
            words = _word_count(section_text)
            attempts += 1

        sections.append({
            "id": spec.id,
            "title": spec.title,
            "text": section_text,
            "paragraphs": _split_paragraphs(section_text),
            "word_count": words,
        })
        prior_titles.append(spec.title)

    total_words = sum(section["word_count"] for section in sections)
    return {
        "title": REPORT_TITLE_MAP.get(report_type, report_type),
        "report_type": report_type,
        "dimension": data.get("dimension"),
        "sections": sections,
        "word_count": total_words,
        "generated_at": datetime.now().isoformat(),
    }


def generate_structured_report_by_dimension(data: dict) -> Dict[str, Any]:
    """
    Generate a single structured report based on the dimension default.
    """
    data = validate_input_data(data)
    dimension = data["dimension"]

    report_type = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(dimension)
    if not report_type:
        raise ValueError(f"Unsupported dimension: {dimension}. Available: {list(DEFAULT_REPORT_TYPE_BY_DIMENSION.keys())}")

    with rag_lock:
        rag_context = retrieve_rag_context(data)

    return generate_structured_report(data, report_type, rag_context)


def generate_multi_reports_structured(data: dict) -> Dict[str, Dict[str, Any]]:
    """
    Generate multiple structured reports based on dimension.
    """
    data = validate_input_data(data)
    dimension = data["dimension"]

    if dimension not in REPORT_TYPE_MAP:
        raise ValueError(f"âŒ Unsupported dimension: {dimension}. Available: {list(REPORT_TYPE_MAP.keys())}")

    with rag_lock:
        rag_context = retrieve_rag_context(data)

    reports: Dict[str, Dict[str, Any]] = {}
    for report_type in REPORT_TYPE_MAP[dimension]:
        reports[report_type] = generate_structured_report(data, report_type, rag_context)

    return reports


# ✅ NEW: MULTI-REPORT GENERATION FUNCTIONS
# ===================================================

def generate_single_report(data: dict, report_type: str, rag_context: str) -> str:
    """
    Generate a single report of specified type.
    
    Args:
        data: Input data
        report_type: Type of report ("employee", "boss", "team", "organization")
        rag_context: RAG context to use
        
    Returns:
        Generated report text
    """
    if report_type not in PROMPT_MAP:
        raise ValueError(f"Invalid report type: {report_type}. Valid types: {list(PROMPT_MAP.keys())}")
    
    developer_prompt = PROMPT_MAP[report_type]
    report_title = REPORT_TITLE_MAP[report_type]
    
    print(f"🚀 Generating {report_title} with Groq ({MODEL_NAME})")
    
    user_prompt = f"""
REFERENCE MATERIAL (AUTHORITATIVE):
{rag_context}

INPUT DATA:
{json.dumps(data, indent=2)}

IMPORTANT: 
- Use the stage definitions from the reference material above to accurately explain what each behavioral stage means
- Do not make up stage definitions
- Apply the appropriate tone based on the identified stage and substage
- Follow the exact section structure provided in the developer prompt
"""

    print(f"💭 Sending request to Groq for {report_type} report...")
    client = create_groq_client()
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT + "\n\n" + TONE_GUIDELINES + "\n\n" + developer_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2,
        max_tokens=8000,
    )

    print(f"✅ {report_title} generated successfully!\n")
    return response.choices[0].message.content.strip()


def generate_multi_reports(data: dict) -> Dict[str, str]:
    """
    Generate multiple reports based on dimension.
    
    Args:
        data: Validated input data with dimension field
        
    Returns:
        Dictionary mapping report type to generated report text
        Example: {"employee": "report text...", "boss": "report text..."}
    """
    data = validate_input_data(data)
    dimension = data["dimension"]
    
    print(f"\n{'='*60}")
    print(f"🔍 MULTI-REPORT GENERATION")
    print(f"{'='*60}")
    print(f"Dimension: {dimension}")
    print(f"Report types to generate: {REPORT_TYPE_MAP.get(dimension, [])}")
    print(f"{'='*60}\n")
    
    if dimension not in REPORT_TYPE_MAP:
        raise ValueError(f"❌ Unsupported dimension: {dimension}. Available: {list(REPORT_TYPE_MAP.keys())}")
    
    # Get RAG context once (shared across all reports)
    with rag_lock:
        rag_context = retrieve_rag_context(data)
    
    # Generate reports for each type
    reports = {}
    report_types = REPORT_TYPE_MAP[dimension]
    
    for report_type in report_types:
        try:
            report = generate_single_report(data, report_type, rag_context)
            reports[report_type] = report
        except Exception as e:
            print(f"❌ Error generating {report_type} report: {e}")
            reports[report_type] = f"Error generating report: {str(e)}"
    
    return reports


# ===================================================
# LEGACY: SINGLE REPORT GENERATION (BACKWARD COMPATIBLE)
# ===================================================
def generate_text_report(data: dict) -> str:
    """
    ✅ LEGACY FUNCTION: Generate single report based on dimension
    Kept for backward compatibility
    """
    data = validate_input_data(data)
    dimension = data["dimension"]
    
    # ✅ CRITICAL DEBUG LOGGING
    print(f"\n{'='*60}")
    print(f"🔍 DIMENSION TRACKING DEBUG")
    print(f"{'='*60}")
    print(f"Dimension extracted: {dimension}")
    print(f"Dimension type: {type(dimension)}")
    print(f"Available prompts: {list(DEV_PROMPT_MAP.keys())}")
    print(f"Dimension in map: {dimension in DEV_PROMPT_MAP}")
    print(f"{'='*60}\n")

    if dimension not in DEV_PROMPT_MAP:
        raise ValueError(f"❌ Unsupported dimension: {dimension}. Available: {list(DEV_PROMPT_MAP.keys())}")

    developer_prompt = DEV_PROMPT_MAP[dimension]
    
    print(f"\n{'='*60}")
    print(f"🚀 Generating {dimension} Report with Groq ({MODEL_NAME})")
    print(f"{'='*60}\n")
    
    with rag_lock:
        rag_context = retrieve_rag_context(data)

    user_prompt = f"""
REFERENCE MATERIAL (AUTHORITATIVE):
{rag_context}

INPUT DATA:
{json.dumps(data, indent=2)}

IMPORTANT: 
- Use the stage definitions from the reference material above to accurately explain what each behavioral stage means
- Do not make up stage definitions
- Apply the appropriate tone based on the identified stage and substage
- Follow the exact section structure provided in the developer prompt
"""

    print("💭 Sending request to Groq...")
    client = create_groq_client()
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT + "\n\n" + TONE_GUIDELINES + "\n\n" + developer_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2,
        max_tokens=8000,  # Groq has different limits per model
    )

    print(f"✅ {dimension} Report generated successfully!\n")
    return response.choices[0].message.content.strip()


# MAIN

def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate ChaturVima reports")
    parser.add_argument("--input", help="Path to input JSON file")
    parser.add_argument("--dimension", help="1D, 2D, 3D, or 4D to load from data/")
    parser.add_argument("--multi", action="store_true", help="Generate multiple reports based on dimension")
    return parser


def _main() -> None:
    args = _build_arg_parser().parse_args()

    if args.input:
        input_data = load_input_json_from_path(args.input)
    elif args.dimension:
        input_data = load_input_json_from_dimension(args.dimension)
    else:
        raise SystemExit("Provide --input or --dimension")

    if args.multi:
        # Generate multiple reports
        reports = generate_multi_reports(input_data)
        
        print("\n" + "="*60)
        print("GENERATED REPORTS")
        print("="*60 + "\n")
        
        for report_type, report_text in reports.items():
            print(f"\n{'='*60}")
            print(f"{REPORT_TITLE_MAP[report_type].upper()}")
            print(f"{'='*60}\n")
            print(report_text)
            
            # Save each report
            output_filename = (
                f"output/report_{input_data['dimension']}_{report_type}_{input_data.get('employee_name', 'unknown')}.txt"
            )
            os.makedirs("output", exist_ok=True)
            with open(output_filename, "w", encoding="utf-8") as f:
                f.write(f"{REPORT_TITLE_MAP[report_type]}\n")
                f.write("="*60 + "\n\n")
                f.write(report_text)
            print(f"\n{report_type} report saved to: {output_filename}")
    else:
        # Legacy: Generate single report
        report_text = generate_text_report(input_data)

        print("\n" + "="*60)
        print("GENERATED REPORT")
        print("="*60 + "\n")
        print(report_text)

        output_filename = (
            f"output/report_{input_data['dimension']}_{input_data.get('employee_name', 'unknown')}.txt"
        )
        os.makedirs("output", exist_ok=True)
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"\nReport saved to: {output_filename}")


if __name__ == "__main__":
    _main()
