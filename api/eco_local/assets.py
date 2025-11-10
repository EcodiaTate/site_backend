# api/eco-local/assets.py
from __future__ import annotations

import io
import os
import re
import time
import hmac
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import qrcode
from fastapi import APIRouter, HTTPException, Depends, Query, UploadFile, File, Body
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from neo4j import Session
from PIL import Image, UnidentifiedImageError

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

router = APIRouter(prefix="/eco-local/assets", tags=["eco-local-assets"])

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
PUBLIC_BASE = os.environ.get("ECODIA_PUBLIC_URL", "http://localhost:3001")
QR_SIGNING_SECRET = os.environ.get("QR_SIGNING_SECRET", "dev-please-change-me")

# Brand palette (hex)
BRAND_FOREST = "#396041"
BRAND_SUN    = "#f4d35e"
BRAND_MINT   = "#7fd069"
BRAND_CREAM  = "#faf3e0"
BRAND_BLACK  = "#000000"
BRAND_WHITE  = "#ffffff"

# Storage roots (ABSOLUTE)
# Try to infer repo root safely; fall back to CWD if path depth is shallow.
_this_file = Path(__file__).resolve()
_repo_parents = _this_file.parents
REPO_ROOT = Path(os.getenv("REPO_ROOT") or (_repo_parents[3] if len(_repo_parents) >= 4 else Path.cwd()))

DEFAULT_HERO_DIR = REPO_ROOT / "storage" / "eco-local" / "hero"
HERO_DIR = Path(os.getenv("HERO_DIR", str(DEFAULT_HERO_DIR))).resolve()
HERO_DIR.mkdir(parents=True, exist_ok=True)  # safe in dev

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def short_url_for_code(code: str) -> str:
    """Next.js QR landing lives at /q/[code]."""
    return f"{PUBLIC_BASE.rstrip('/')}/q/{code}"

def app_payload_for_code(code: str) -> str:
    # Simple scheme the modal can parse reliably.
    # Examples: "eco-local:qr_abc123"  or for non-qr prefixed codes: "eco-local:biz_,783j28d3"
    return f"eco-local:{code}"

def _sign_qr_path(code: str, exp_ts: int) -> str:
    # payload: "code|exp"
    msg = f"{code}|{exp_ts}".encode("utf-8")
    return hmac.new(QR_SIGNING_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def _verify_qr_signature(code: str, exp_ts: int, sig: str) -> bool:
    if exp_ts < int(time.time()):
        return False
    expected = _sign_qr_path(code, exp_ts)
    return hmac.compare_digest(expected, sig or "")

def _assert_png_filename(fname: str) -> str:
    """
    Only allow simple filenames like abc123.png (no slashes).
    """
    if not re.fullmatch(r"[A-Za-z0-9_\-]+\.png", fname or ""):
        raise HTTPException(status_code=404, detail="Not found")
    return fname

def _abs_hero_path(fname: str) -> Path:
    fname = _assert_png_filename(fname)
    p = (HERO_DIR / fname).resolve()
    # Prevent path traversal: must stay inside HERO_DIR
    if HERO_DIR not in p.parents and p != HERO_DIR:
        raise HTTPException(status_code=404, detail="Not found")
    return p

@dataclass
class QRMeta:
    business_id: str
    business_name: str
    location_name: Optional[str]
    code: str

def _get_owned_qr_meta(s: Session, *, user_id: str, code: str) -> QRMeta:
    """
    Ensure the QR code belongs to a business owned/managed by the current user.
    """
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile)
        WHERE type(r) IN ['OWNS','MANAGES']
        MATCH (q:QR {code:$code})-[:OF]->(b)
        RETURN b.id AS bid, coalesce(b.name,'ECO Local Partner') AS bname,
               coalesce(b.area, b.location) AS loc, q.code AS code
        LIMIT 1
        """,
        uid=user_id,
        code=code,
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="QR not found")
    return QRMeta(
        business_id=rec["bid"],
        business_name=rec["bname"],
        location_name=rec["loc"],
        code=rec["code"],
    )

def _get_owned_business_id(s: Session, *, user_id: str) -> Optional[str]:
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile)
        WHERE type(r) IN ['OWNS','MANAGES']
        RETURN b.id AS id
        ORDER BY id
        LIMIT 1
        """,
        uid=user_id,
    ).single()
    return rec["id"] if rec else None

# --------------------------------------------------------------------
# HERO: Upload & Serve
# --------------------------------------------------------------------
@router.post("/hero_upload", response_model=dict)
async def hero_upload(
    file: UploadFile = File(...),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Accept an image upload, validate it with Pillow, store as PNG,
    and return a *relative* URL like `/eco-local/assets/hero/<slug>.png`.
    """
    # Ownership scope
    bid = _get_owned_business_id(s, user_id=user_id)
    if not bid:
        raise HTTPException(status_code=403, detail="No business found for this account")

    data = await file.read()
    if not data or len(data) < 16:
        raise HTTPException(status_code=400, detail="Empty or invalid file")

    try:
        img = Image.open(io.BytesIO(data))
        img.verify()
    except UnidentifiedImageError:
        raise HTTPException(status_code=400, detail="The uploaded file is not a valid image")

    # Re-open after verify() to get a usable image object, normalize to RGB PNG
    img = Image.open(io.BytesIO(data)).convert("RGB")

    import secrets
    slug = secrets.token_hex(8)
    filename = f"{slug}.png"
    abs_path = os.path.join(HERO_DIR, filename)

    try:
        img.save(abs_path, format="PNG", optimize=True)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to store image")

    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.hero_url = $rel
        """,
        bid=bid,
        rel=f"/eco-local/assets/hero/{filename}",
    )
    return {"url": f"/eco-local/assets/hero/{filename}"}

@router.get("/hero/{filename}")
def serve_hero(filename: str):
    path = _abs_hero_path(filename)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    # Allow browser caching; assets change by filename
    return FileResponse(
        str(path),
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=2592000, immutable"}  # 30d
    )

@router.get("/_debug/hero_exists")
def hero_exists_debug(filename: str):
    try:
        path = _abs_hero_path(filename)
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail, "hero_dir": str(HERO_DIR)}, status_code=404)
    return {
        "ok": path.is_file(),
        "filename": filename,
        "abs_path": str(path),
        "hero_dir": str(HERO_DIR),
        "dir_exists": HERO_DIR.is_dir(),
        "dir_contents_sample": sorted([p.name for p in HERO_DIR.glob("*.png")])[:10],
    }

# --------------------------------------------------------------------
# QR: PNG + Poster (signed public)
# --------------------------------------------------------------------
@router.get("/qr/{code}.png")
def qr_png(
    code: str,
    size: int = Query(1024, ge=128, le=4096),
    exp: int = Query(..., description="unix expiry"),
    sig: str = Query(..., description="hmac signature"),
    s: Session = Depends(session_dep),
):
    if not _verify_qr_signature(code, exp, sig):
        raise HTTPException(status_code=401, detail="Invalid or expired signature")

    rec = s.run("MATCH (q:QR {code:$code}) RETURN q.code AS code LIMIT 1", code=code).single()
    if not rec:
        raise HTTPException(status_code=404, detail="QR not found")

    link = short_url_for_code(code)
    qr = qrcode.QRCode(version=None, error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=10, border=3)
    qr.add_data(link)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").resize((size, size))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="image/png",
        headers={
            "Cache-Control": "private, no-store",
            "Content-Disposition": f'inline; filename="eco-local-qr-{code}.png"'
        },
    )

@router.get("/qr/{code}/poster.pdf")
def qr_poster_pdf(
    code: str,
    exp: int = Query(..., description="unix expiry"),
    sig: str = Query(..., description="hmac signature"),
    s: Session = Depends(session_dep),
):
    if not _verify_qr_signature(code, exp, sig):
        raise HTTPException(status_code=401, detail="Invalid or expired signature")

    rec = s.run(
        """
        MATCH (q:QR {code:$code})<-[:OF]-(b:BusinessProfile)
        RETURN coalesce(b.name, 'ECO Local Partner') AS bname,
               coalesce(b.area, b.location) AS loc
        LIMIT 1
        """,
        code=code,
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="QR not found")

    business_name = rec["bname"]
    location_name = rec["loc"]
    link = short_url_for_code(code)

    qr = qrcode.QRCode(version=None, error_correction=qrcode.constants.ERROR_CORRECT_Q, box_size=10, border=2)
    qr.add_data(link)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white")
    png_bytes = io.BytesIO()
    qr_img.save(png_bytes, format="PNG")
    png_bytes.seek(0)

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4
    margin = 18 * mm
    inner_w = W - 2 * margin
    inner_h = H - 2 * margin

    # Cream sheet with forest header band
    c.setFillColor(BRAND_CREAM)
    c.roundRect(margin, margin, inner_w, inner_h, 12, fill=1, stroke=0)
    c.setFillColor(BRAND_FOREST)
    c.roundRect(margin, H - margin - 28 * mm, inner_w, 28 * mm, 10, fill=1, stroke=0)

    # Header text
    c.setFillColor(BRAND_WHITE)
    c.setFont("Helvetica-Bold", 26)
    c.drawString(margin + 12 * mm, H - margin - 12 * mm, "Earn ECO here")

    c.setFont("Helvetica", 13)
    line2 = f"{business_name}" + (f" · {location_name}" if location_name else "")
    c.drawString(margin + 12 * mm, H - margin - 19 * mm, line2)

    # QR centered
    qr_size = 90 * mm
    qr_x = margin + (inner_w - qr_size) / 2
    qr_y = margin + (inner_h - qr_size) / 2 - 8 * mm
    c.drawImage(png_bytes, qr_x, qr_y, qr_size, qr_size, mask="auto")

    # Caption pill under QR
    c.setFillColor(BRAND_WHITE)
    c.roundRect(qr_x - 8 * mm, qr_y - 22 * mm, qr_size + 16 * mm, 18 * mm, 8, fill=1, stroke=0)
    c.setFillColor(BRAND_BLACK)
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(qr_x + qr_size / 2, qr_y - 10 * mm, "Scan with your phone")

    # Footer copy + short URL
    c.setFont("Helvetica", 10)
    c.setFillColor(BRAND_BLACK)
    c.drawCentredString(W / 2, margin + 10 * mm, "Young people earn ECO for visiting, learning, and acting.")
    c.setFont("Helvetica", 9)
    c.setFillColor(BRAND_FOREST)
    c.drawCentredString(W / 2, margin + 6 * mm, link)

    c.showPage()
    c.save()
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={
            "Cache-Control": "private, no-store",
            "Content-Disposition": f'inline; filename="eco-local-qr-poster-{code}.pdf"'
        },
    )

@router.post("/qr_signed_url")
def qr_signed_url(
    code: str = Body(..., embed=True),
    minutes_valid: int = Body(30, embed=True),  # default 30 minutes
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Generate signed, time-limited URLs for QR PNG and Poster.
    Ensures the caller owns the QR/business.
    """
    # ensure caller owns this QR before minting a signed URL
    _get_owned_qr_meta(s, user_id=user_id, code=code)

    exp = int(time.time()) + (minutes_valid * 60)
    sig = _sign_qr_path(code, exp)
    return {
        "png": f"/eco-local/assets/qr/{code}.png?exp={exp}&sig={sig}",
        "pdf": f"/eco-local/assets/qr/{code}/poster.pdf?exp={exp}&sig={sig}",
        "exp": exp,
    }