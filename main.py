from __future__ import annotations
import os # <-- Import os first
from dotenv import load_dotenv

# --- THIS IS THE FIX ---
#
# Try loading the .env file from the SAME directory as main.py
# __file__ is D:\EcodiaOS\site_backend\main.py
# os.path.dirname(__file__) is D:\EcodiaOS\site_backend
# This will look for D:\EcodiaOS\site_backend\.env
#
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=dotenv_path)

# --- DIAGNOSTIC PRINT ---
# This will run immediately. Check your console for this output.
print("--- .env diagnostics ---")
print(f"Loading .env from: {dotenv_path}")
print(f"NEO4J_URI loaded: {os.getenv('NEO4J_URI')}")
print(f"JWT_SECRET length: {len(os.getenv('JWT_SECRET', ''))}")
print("------------------------")
# --------------------------


from contextlib import asynccontextmanager
import hashlib
from fastapi import APIRouter, Depends
from site_backend.core.admin_guard import require_admin, JWT_SECRET, JWT_ALGO

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY
from pydantic import ValidationError
from neo4j import Driver
from neo4j.exceptions import Neo4jError

from site_backend.core.neo_driver import build_driver, ensure_constraints
from site_backend.core import admin_cookie
from site_backend.api import auth, profile, stats
from site_backend.api.eco_home import home_routes
from site_backend.api.eyba import router as eyba_router
from site_backend.api.missions import missions
from site_backend.api.leaderboards import leaderboards
from site_backend.api import launchpad
from site_backend.api import gamification
from site_backend.api import auth

API_PORT = int(os.getenv("API_PORT", "8000"))
NEO4J_URI = os.getenv("NEO4J_URI") # This will now be correctly loaded
NEO4J_USER = os.getenv("NEO4J_USER") # This will now be correctly loaded
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD") # This will now be correctly loaded
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",") if o.strip()]
debug_router = APIRouter()

@debug_router.get("/debug/secret-hash")
def secret_hash():
        return {
            "len": len(JWT_SECRET), # This should now be 32
            "sha256": hashlib.sha256(JWT_SECRET.encode("utf-8")).hexdigest(),
            "algo": JWT_ALGO,
        }

@debug_router.get("/debug/admin-ping")
async def admin_ping(email: str = Depends(require_admin)):
        return {"ok": True, "email": email}

@asynccontextmanager
async def lifespan(app: FastAPI):
    driver: Driver = build_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) # This will no longer fail
    ensure_constraints(driver)
    
    app.state.driver = driver
    print("[lifespan] Neo4j connected & constraints ensured")
    try:
        yield
    finally:
        driver.close()
        print("[lifespan] driver closed")

def create_app() -> FastAPI:
    app = FastAPI(title="Ecodia Site Backend", version="0.1.0", lifespan=lifespan)

    # --- CORS: explicitly allow our custom headers ---
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*", "Authorization", "X-Owner-Token", "X-Auth-Token", "X-User-Email"],
        expose_headers=["X-Owner-Token", "X-Auth-Token"],
    )

    # --- Dev diag: log token presence for owner calls (optional, remove in prod) ---
    if os.getenv("LOG_OWNER_TOKEN", "0") == "1":
      @app.middleware("http")
      async def log_owner_token(request: Request, call_next):
          if request.url.path.startswith("/launchpad/proposals/") and request.method == "GET":
              tok = request.headers.get("x-owner-token")
              pid = request.url.path.rsplit("/", 1)[-1]
              print(f"[owner GET] id={pid} X-Owner-Token={'<present>' if tok else '<missing>'}")
          return await call_next(request)

    # Static
    base_dir = os.path.dirname(os.path.abspath(__file__))
    static_img_dir = os.path.join(base_dir, "static", "img")
    if not os.path.exists(static_img_dir):
        os.makedirs(static_img_dir, exist_ok=True)
    app.mount("/img", StaticFiles(directory=static_img_dir), name="img")
    # add in your FastAPI app (e.g., launchpad router file or main)
    from fastapi import APIRouter, Depends
    from site_backend.core.admin_guard import require_admin

    

    app.include_router(debug_router)

    # Routers
    app.include_router(eyba_router)
    app.include_router(auth.router, prefix="/auth", tags=["auth"])
    app.include_router(profile.router, prefix="/youth", tags=["youth"])
    app.include_router(missions.router)
    app.include_router(home_routes.router)
    app.include_router(stats.router)
    app.include_router(admin_cookie.router)
    app.include_router(launchpad.router)
    app.include_router(leaderboards.router)
    app.include_router(gamification.router)
    app.include_router(auth.router)
    

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
            content={"ok": False, "error": "Validation failed", "details": errors, "message": summary or "Invalid input"},
        )

    @app.exception_handler(ValidationError)
    async def friendly_pydantic_validation(request: Request, exc: ValidationError):
        return JSONResponse(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            content={"ok": False, "error": "Validation failed", "message": str(exc)},
        )

    @app.exception_handler(Neo4jError)
    async def friendly_neo4j_error(request: Request, exc: Neo4jError):
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Database error", "message": "Something went wrong saving to the database."},
        )

    @app.exception_handler(Exception)
    async def friendly_generic_error(request: Request, exc: Exception):
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Server error", "message": "Something went wrong. Please try again."},
        )

    

    @app.get("/health")
    def health():
        return {"ok": True}

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=API_PORT, reload=True)