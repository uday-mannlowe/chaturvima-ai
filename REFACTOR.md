# ChaturVima — Refactored Project Structure

## New Folder Layout

```
implementation/
│
├── main.py                         ← Entry point (~60 lines, just wiring)
├── generate_groq.py                ← LEGACY: still works; migrate to llm/ gradually
│
├── core/                           ← Infrastructure — no business logic
│   ├── __init__.py
│   ├── config.py                   ← All env-driven config (was Config class in main.py)
│   ├── rate_limiter.py             ← Token-bucket RateLimiter
│   └── worker_pool.py              ← Async WorkerPool (drains ReportQueue)
│
├── models/                         ← Pure data structures — no I/O, no HTTP
│   ├── __init__.py
│   └── schemas.py                  ← JobStatus enum, ReportJob dataclass, ReportQueue
│
├── services/                       ← Stateless utility services
│   ├── __init__.py
│   ├── frappe_client.py            ← All Frappe API calls + SWOT helpers
│   ├── report_renderer.py          ← Jinja2 HTML rendering + WeasyPrint PDF
│   └── report_storage.py          ← File-system read/write + path builders
│
├── api/                            ← FastAPI routers — thin HTTP layer only
│   ├── __init__.py
│   ├── html_report_routes.py       ← GET /html-report/{id}  +  /html-report/{id}/pdf
│   ├── employee_routes.py          ← POST /generate-employee-report, GET /report/{id}, GET /debug-frappe/{id}
│   └── json_report_routes.py       ← POST /generate/json
│
└── llm/                            ← LLM / AI layer (generated from generate_groq.py)
    ├── __init__.py
    ├── groq_client.py              ← [STUB] Groq client init + model config + retry logic
    ├── rag.py                      ← [STUB] BehavioralStagesRAG class
    ├── prompts.py                  ← [STUB] All prompt strings + SectionSpec definitions
    └── generators.py               ← [STUB] Top-level generate_* functions
```

## What Was Done

| Before | After |
|---|---|
| `main.py` ~1831 lines | `main.py` ~80 lines (pure wiring) |
| `generate_groq.py` ~2620 lines | Stays as-is for now; `llm/` stubs show split plan |
| `Config` class inline in `main.py` | `core/config.py` |
| `RateLimiter` inline in `main.py` | `core/rate_limiter.py` |
| `WorkerPool` inline in `main.py` | `core/worker_pool.py` |
| `ReportJob`, `ReportQueue` inline | `models/schemas.py` |
| Frappe helpers scattered in `main.py` | `services/frappe_client.py` |
| HTML/PDF rendering helpers in `main.py` | `services/report_renderer.py` |
| File I/O helpers in `main.py` | `services/report_storage.py` |
| All routes in `main.py` | `api/html_report_routes.py`, `api/employee_routes.py`, `api/json_report_routes.py` |

## Migration Checklist for `generate_groq.py`

The `llm/` stub files each contain a `TODO` comment explaining exactly what to move and how.
Work through them in order:

- [ ] `llm/groq_client.py` — Groq client + model config + retry logic (~238 lines)
- [ ] `llm/rag.py`          — `BehavioralStagesRAG` class + helpers (~100 lines)
- [ ] `llm/prompts.py`      — Prompts, `SectionSpec`, section spec lists (~700 lines)
- [ ] `llm/generators.py`   — All `generate_*` functions + data helpers (~1580 lines)

Once each file is populated, update `generate_groq.py` to simply re-export everything
(backward-compatible shim for 3rd-party code that imports from it directly):

```python
# generate_groq.py  — backward-compat shim after full migration
from llm.groq_client import *
from llm.rag import *
from llm.prompts import *
from llm.generators import *
```

## Running the App

No change:
```bash
uvicorn main:app --host 0.0.0.0 --port 5000
# or
python main.py
```

## Design Principles Applied

1. **Single Responsibility** — each file has one clear job.
2. **Thin HTTP layer** — routers only parse requests and return responses; logic lives in services.
3. **No circular imports** — dependency flow is strictly: `api → services/core → models`.
4. **Gradual migration** — `generate_groq.py` still works unchanged; `llm/` stubs guide the next step.
