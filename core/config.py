"""
core/config.py
Centralized configuration loaded from environment variables.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Centralized configuration with environment variable support"""

    MAX_CONCURRENT_GENERATIONS = int(os.getenv("MAX_CONCURRENT_GENERATIONS", "5"))
    MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "100"))

    GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
    GROQ_RATE_LIMIT_PER_MINUTE = int(os.getenv("GROQ_RATE_LIMIT_PER_MINUTE", "30"))
    GROQ_TIMEOUT_SECONDS = int(os.getenv("GROQ_TIMEOUT_SECONDS", "120"))

    TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "html")
    DEFAULT_TEMPLATE_NAME = os.getenv("REPORT_TEMPLATE", "report_wrapper.html")

    REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "300"))
    ENABLE_METRICS = os.getenv("ENABLE_METRICS", "true").lower() == "true"

    FRAPPE_BASE_URL = os.getenv(
        "FRAPPE_BASE_URL",
        "https://cvdev.m.frappe.cloud/api/method/"
        "chaturvima_api.api.dashboard.get_employee_weighted_assessment_summary",
    )
    FRAPPE_RESOURCE_BASE_URL = os.getenv(
        "FRAPPE_RESOURCE_BASE_URL",
        "https://cvdev.m.frappe.cloud/api/resource",
    )
    DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
    HTML_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "html_data")

    FRAPPE_API_KEY    = os.getenv("FRAPPE_API_KEY", "")
    FRAPPE_API_SECRET = os.getenv("FRAPPE_API_SECRET", "")
    FRAPPE_USERNAME   = os.getenv("FRAPPE_USERNAME", "")
    FRAPPE_PASSWORD   = os.getenv("FRAPPE_PASSWORD", "")
    # Temporary safety switch: keep Frappe auth static (configured creds) until
    # runtime/dynamic auth flow is stabilized.
    FORCE_STATIC_FRAPPE_AUTH = os.getenv("FORCE_STATIC_FRAPPE_AUTH", "true").lower() == "true"

    CORS_ALLOWED_ORIGINS = [
        origin.strip()
        for origin in os.getenv(
            "CORS_ALLOWED_ORIGINS",
            "*",
        ).split(",")
        if origin.strip()
    ]
    CORS_ALLOWED_ORIGIN_REGEX = (
        os.getenv(
            "CORS_ALLOWED_ORIGIN_REGEX",
            "",
        ).strip()
        or None
    )
    CORS_ALLOW_CREDENTIALS = os.getenv("CORS_ALLOW_CREDENTIALS", "false").lower() == "true"
    CORS_ALLOW_METHODS = ["*"]
    CORS_ALLOW_HEADERS = ["*"]

    # Browsers reject credentialed CORS responses when allow-origin is wildcard.
    if CORS_ALLOW_CREDENTIALS and "*" in CORS_ALLOWED_ORIGINS:
        CORS_ALLOWED_ORIGINS = [o for o in CORS_ALLOWED_ORIGINS if o != "*"]
        if not CORS_ALLOWED_ORIGIN_REGEX:
            CORS_ALLOWED_ORIGIN_REGEX = r"^https?://.*$"

    # Single Jinja2 template used for ALL dimensions
    REPORT_TEMPLATE_NAME = "report_template.html"
