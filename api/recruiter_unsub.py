# (server) your FastAPI router file shown above
from __future__ import annotations
import os, hmac, hashlib, time
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, HTMLResponse, Response
from typing import Optional

router = APIRouter(prefix="/eco-local/business", tags=["eco-local"])

_SECRET = os.getenv("UNSUB_SIGNING_SECRET") or os.getenv("UNSUB_HMAC_SECRET") or ""
_MAX_AGE_SECS = int(os.getenv("ECO_LOCAL_UNSUB_MAX_AGE", "2592000"))  # 30 days

def _norm_email(e: str) -> str:
    return (e or "").strip().lower()

def _bad_link_response(request: Request, message: str = "Invalid or expired link") -> Response:
    wants_json = "application/json" in (request.headers.get("accept") or "")
    if wants_json:
        return JSONResponse({"ok": False, "detail": message}, status_code=400)
    return HTMLResponse(
        f"""
<!doctype html><meta charset="utf-8"/>
<title>Invalid link</title>
<body style="font-family:system-ui;padding:32px">
  <h1>Invalid or expired link</h1>
  <p>If you’d like to unsubscribe, reply “unsubscribe” to any ECO Local email and we’ll sort it out.</p>
</body>
""",
        status_code=400,
    )

@router.get("/unsubscribe")
async def eco_local_unsubscribe(
    request: Request,
    e: str = Query(..., alias="e"),
    ts: str = Query(..., alias="ts"),
    sig: str = Query(..., alias="sig"),
    t: Optional[str] = Query(None, alias="t"),  # optional thread id
):
    if not _SECRET:
        return _bad_link_response(request, "Server not configured")

    try:
        ts_i = int(ts)
    except Exception:
        return _bad_link_response(request, "Bad timestamp")

    now = int(time.time())
    if abs(now - ts_i) > _MAX_AGE_SECS:
        return _bad_link_response(request, "Link expired")

    e_norm = _norm_email(e)

    # Primary (current) canonical form: "<email>|<ts>"
    payload1 = f"{e_norm}|{ts_i}".encode("utf-8")
    expected1 = hmac.new(_SECRET.encode("utf-8"), payload1, hashlib.sha256).hexdigest()

    # Backward-compat: if 't' is present, we also accept "<email>|<ts>|<t>"
    expected2 = None
    if t:
        payload2 = f"{e_norm}|{ts_i}|{t.strip()}".encode("utf-8")
        expected2 = hmac.new(_SECRET.encode("utf-8"), payload2, hashlib.sha256).hexdigest()

    got = (sig or "").strip().lower()
    ok = hmac.compare_digest(got, expected1.lower()) or (expected2 and hmac.compare_digest(got, expected2.lower()))

    if not ok:
        if os.getenv("ECO_LOCAL_UNSUB_DEBUG", "0") == "1":
            return JSONResponse(
                {
                    "ok": False,
                    "detail": "sig mismatch",
                    "expected": expected1,
                    "expected_alt": expected2,
                    "got": got,
                    "email_norm": e_norm,
                    "ts": ts_i,
                    "t": t,
                },
                status_code=400,
            )
        return _bad_link_response(request, "Invalid signature")

    # TODO: mark unsubscribed in your store here
    # store.mark_unsubscribed(e_norm)

    if "application/json" in (request.headers.get("accept") or ""):
        return JSONResponse({"ok": True, "email": e_norm})
    return HTMLResponse(
        f"""
<!doctype html><meta charset="utf-8"/>
<title>Unsubscribed</title>
<body style="font-family:system-ui;padding:32px">
  <h1>You’re all set.</h1>
  <p><strong>{e_norm}</strong> has been removed from ECO Local emails.</p>
  <p><a href="https://ecodia.au/eco-local">Back to ECO Local</a></p>
</body>
"""
    )

@router.get("/unsubscribe/debug")
async def eco_local_unsubscribe_debug(e: str, ts: int, t: Optional[str] = None, show: int = 0):
    if os.getenv("ECO_LOCAL_UNSUB_DEBUG", "0") != "1":
        return JSONResponse({"ok": False, "detail": "debug disabled"}, status_code=403)
    en = _norm_email(e)
    p1 = f"{en}|{ts}".encode("utf-8")
    exp1 = hmac.new((_SECRET or "").encode("utf-8"), p1, hashlib.sha256).hexdigest()
    exp2 = None
    if t:
        p2 = f"{en}|{ts}|{t.strip()}".encode("utf-8")
        exp2 = hmac.new((_SECRET or "").encode("utf-8"), p2, hashlib.sha256).hexdigest()
    out = {"ok": True, "email_norm": en, "ts": ts, "expected_sig": exp1, "expected_sig_alt": exp2}
    return JSONResponse(out if show else {"ok": True})
