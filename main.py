# site_backend/main.py
from __future__ import annotations

# --- .env loading (from this directory) ---------------------------------------
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=dotenv_path)

# Diagnostic (safe to keep in dev; remove in prod if noisy)
print("--- .env diagnostics ---")
print(f"Loading .env from: {dotenv_path}")
print(f"NEO4J_URI loaded: {os.getenv('NEO4J_URI')}")
print(f"JWT_SECRET length: {len(os.getenv('JWT_SECRET', ''))}")
print("------------------------")

# --- std/3rd party imports ----------------------------------------------------
import hashlib
import re
import traceback
from typing import Dict, List
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY
from pydantic import ValidationError

from neo4j import Driver
from neo4j.exceptions import Neo4jError
from site_backend.core.paths import UPLOAD_ROOT

# --- internal imports ---------------------------------------------------------
from site_backend.core.admin_guard import require_admin, JWT_SECRET, JWT_ALGO
from site_backend.core.neo_driver import build_driver, ensure_constraints
from site_backend.core.admin_cookie import router as admin_cookie_router

from site_backend.api import auth, profile, stats, launchpad, gamification, account
from site_backend.api.eco_home import home_routes
from site_backend.api.sidequests import router as sidequest_router
from site_backend.api.leaderboards import leaderboards
from site_backend.api.account_delete import router as account_delete_router
from site_backend.api.notifications import router as notif_router
from site_backend.api.privacy import router as privacy_router
from site_backend.api.export import router as export_router
from site_backend.api.export_worker import router as worker_router
from site_backend.api.social.router_public import router as social_router
from site_backend.api.teams.router_public import router as teams_router
from site_backend.api.tournaments.router_public import router as tournaments_router
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from site_backend.api.account.service import gc_avatar_blobs  # adjust path if your GC lives elsewhere

# --- config -------------------------------------------------------------------
API_PORT = int(os.getenv("API_PORT", "8000"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
    if o.strip()
]
ERROR_DETAIL = os.getenv("ERROR_DETAIL", "verbose").lower()  # "verbose" | "minimal"
# Upload root: .env wins; otherwise default to <repo>/site_backend/data/uploads
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../EcodiaOS/site_backend
DEFAULT_UPLOAD_ROOT = os.path.normpath(os.path.join(BASE_DIR, "data", "uploads"))
UPLOAD_ROOT = os.path.abspath(os.getenv("UPLOAD_ROOT", DEFAULT_UPLOAD_ROOT))
# --- debug router -------------------------------------------------------------
debug_router = APIRouter()

@debug_router.get("/debug/secret-hash")
def secret_hash():
    return {
        "len": len(JWT_SECRET),
        "sha256": hashlib.sha256(JWT_SECRET.encode("utf-8")).hexdigest(),
        "algo": JWT_ALGO,
    }

@debug_router.get("/debug/admin-ping")
async def admin_ping(email: str = Depends(require_admin)):
    return {"ok": True, "email": email}

# --- neo4j error humanizer ----------------------------------------------------
def _neo4j_humanize(code: str | None, message: str | None) -> Dict[str, List[str] | str | None]:
    """
    Turn Neo4j's cryptic codes/messages into short, actionable hints.
    Extract the missing label/property/rel name from the server message if present.
    """
    c = code or ""
    msg = (message or "").strip()

    meaning = "A database error occurred."
    fixes: List[str] = []

    def extract(pattern: str) -> str | None:
        m = re.search(pattern, msg, flags=re.IGNORECASE)
        return m.group(1).strip() if m else None

    if "UnknownLabel" in c:
        label = extract(r"missing label name is:\s*([^)]+)\)")
        meaning = f"Neo4j does not know the label '{label or '?'}'."
        fixes = [
            "Create nodes with that label, or correct the label used in MATCH.",
            "Example: CREATE (:_YourLabel {id:'x'}); or change MATCH (:YourLabel) to an existing label.",
        ]

    elif "UnknownPropertyKey" in c:
        prop = extract(r"missing property name is:\s*([^)]+)\)")
        meaning = f"The property '{prop or '?'}' does not exist on matched nodes."
        fixes = [
            "Initialize the property when creating nodes (e.g., SET u.banned=false on :User).",
            "Guard reads with COALESCE, e.g., coalesce(u.banned, false).",
        ]

    elif "UnknownRelationshipType" in c:
        rel = extract(r"missing relationship type is:\s*([^)]+)\)")
        meaning = f"The relationship type '{rel or '?'}' does not exist."
        fixes = [
            "Create data with that relationship type, or correct the type in your pattern.",
            "Example: CREATE (a)-[:REFERRED]->(b); or use an existing relationship name.",
        ]

    elif "TypeError" in c:
        if "Property values can only be of primitive types" in msg:
            meaning = "Attempted to write a map/object into a single property."
            fixes = [
                "Store maps as a JSON string (apoc.convert.toJson) or flatten into scalar properties.",
                "Check MERGE/SET payloads for nested dicts and flatten them.",
            ]

    elif "SyntaxError" in c or "Statement" in c:
        meaning = "Cypher syntax or statement error."
        fixes = [
            "Verify labels, property names, and relationship types.",
            "Run the exact query in Neo4j Browser to see the highlighted token.",
        ]

    if not fixes:
        fixes = [
            "Verify the labels, properties, and relationship types referenced in your query.",
            "Try the exact statement in Neo4j Browser to pinpoint the issue.",
        ]

    return {
        "code": c,
        "meaning": meaning,
        "hints": fixes,
        "raw_message": msg if ERROR_DETAIL == "verbose" else None,
    }

# --- lifespan: connect to Neo4j and ensure constraints -----------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    driver: Driver = build_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    ensure_constraints(driver)
    app.state.driver = driver
    print("[lifespan] Neo4j connected & constraints ensured")

    # ── Avatar GC scheduler ─────────────────────────────────────────────
    scheduler = AsyncIOScheduler(timezone="Australia/Brisbane")  # or "Australia/Brisbane" if you prefer local time
    def run_avatar_gc():
        try:
            with driver.session() as s:
                purged = gc_avatar_blobs(s)
                if purged:
                    print(f"[avatar-gc] purged {purged} blobs")
        except Exception as e:
            # Keep errors from crashing the scheduler
            print(f"[avatar-gc] error: {e}")

    # Every 6 hours at minute 7 (staggered a bit after the hour)
    scheduler.add_job(
        run_avatar_gc,
        trigger="cron",
        hour="*/6",
        minute=7,
        id="gc_avatars",
        replace_existing=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    app.state.scheduler = scheduler
    # ───────────────────────────────────────────────────────────────────

    try:
        yield
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        driver.close()
        print("[lifespan] driver closed")

 

# --- app factory --------------------------------------------------------------
def create_app() -> FastAPI:
    app = FastAPI(title="Ecodia Site Backend", version="0.1.0", lifespan=lifespan)

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*", "Authorization", "X-Owner-Token", "X-Auth-Token", "X-User-Email"],
        expose_headers=["X-Owner-Token", "X-Auth-Token"],
    )

    app.mount("/uploads", StaticFiles(directory=str(UPLOAD_ROOT)), name="uploads")
    print(f"[uploads] serving from: {UPLOAD_ROOT}")  # dev visibility
    # Optional request logger for owner token (dev-only)
    if os.getenv("LOG_OWNER_TOKEN", "0") == "1":
        @app.middleware("http")
        async def log_owner_token(request: Request, call_next):
            if request.url.path.startswith("/launchpad/proposals/") and request.method == "GET":
                tok = request.headers.get("x-owner-token")
                pid = request.url.path.rsplit("/", 1)[-1]
                print(f"[owner GET] id={pid} X-Owner-Token={'<present>' if tok else '<missing>'}")
            return await call_next(request)
   
    app.include_router(debug_router)
    app.include_router(auth.router, prefix="/auth", tags=["auth"])
    app.include_router(profile.router, prefix="/youth", tags=["youth"])
    app.include_router(sidequest_router)
    app.include_router(home_routes.router)
    app.include_router(stats.router)
    app.include_router(account.router)
    app.include_router(account_delete_router)
    app.include_router(admin_cookie_router)
    app.include_router(launchpad.router)
    app.include_router(leaderboards.router)
    app.include_router(gamification.router)
    app.include_router(notif_router)
    app.include_router(privacy_router)
    app.include_router(social_router)
    app.include_router(teams_router)
    app.include_router(tournaments_router)
    app.include_router(export_router)
    app.include_router(worker_router)

    # ---------------- Friendlier error messages ----------------
    @app.exception_handler(RequestValidationError)
    async def friendly_422(request: Request, exc: RequestValidationError):
        errors = []
        for e in exc.errors():
            loc = e.get("loc", [])
            field = ".".join(str(x) for x in loc if isinstance(x, (str, int)))
            msg = e.get("msg", "Invalid value")
            if field.startswith("body."):
                field = field[5:]
            errors.append({"field": field, "message": msg})

        summary = ", ".join(f"{e['field']}: {e['message']}" for e in errors if e["field"])
        return JSONResponse(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "ok": False,
                "error": "Validation failed",
                "path": str(request.url),
                "method": request.method,
                "details": errors,
                "message": summary or "Invalid input",
            },
        )

    @app.exception_handler(ValidationError)
    async def friendly_pydantic_validation(request: Request, exc: ValidationError):
        return JSONResponse(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "ok": False,
                "error": "Validation failed",
                "path": str(request.url),
                "method": request.method,
                "message": str(exc) if ERROR_DETAIL == "verbose" else "Invalid payload for this endpoint.",
            },
        )

    @app.exception_handler(Neo4jError)
    async def friendly_neo4j_error(request: Request, exc: Neo4jError):
        human = _neo4j_humanize(getattr(exc, "code", None), str(exc))

        # concise server-side log
        print("=== Neo4jError =====================================")
        print(f"PATH: {request.method} {request.url.path}")
        print(f"CODE: {getattr(exc, 'code', None)}")
        print(f"MSG : {str(exc)}")
        print("TRACE:")
        traceback.print_exc()
        print("=====================================================")

        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": "Database error",
                "path": str(request.url),
                "method": request.method,
                "neo4j": human,
            },
        )

    @app.exception_handler(Exception)
    async def friendly_generic_error(request: Request, exc: Exception):
        print("=== Unhandled Exception =============================")
        print(f"PATH: {request.method} {request.url.path}")
        traceback.print_exc()
        print("=====================================================")

        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": "Server error",
                "path": str(request.url),
                "method": request.method,
                "message": "Something went wrong. Please try again.",
            },
        )

    @app.get("/health")
    def health():
        return {"ok": True}

    return app

# --- ASGI app -----------------------------------------------------------------
app = create_app()

# --- dev entry ----------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=API_PORT, reload=True)
