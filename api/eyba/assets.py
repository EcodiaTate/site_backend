# api/eyba/assets.py
from __future__ import annotations

import io
import re
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import qrcode
from fastapi import APIRouter, HTTPException, Depends, Query, UploadFile, File
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from neo4j import Session
from PIL import Image, UnidentifiedImageError

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

router = APIRouter(prefix="/eyba/assets", tags=["eyba"])

PUBLIC_BASE = os.environ.get("PUBLIC_BASE_URL", "http://localhost:3000")

# Brand palette (hex)
BRAND_FOREST = "#396041"
BRAND_SUN = "#f4d35e"
BRAND_MINT = "#7fd069"
BRAND_CREAM = "#faf3e0"
BRAND_BLACK = "#000000"
BRAND_WHITE = "#ffffff"

router = APIRouter(prefix="/eyba/assets", tags=["eyba"])

PUBLIC_BASE = os.environ.get("PUBLIC_BASE_URL", "http://localhost:3001")
import hmac, hashlib, time
from fastapi import Request

QR_SIGNING_SECRET = os.environ.get("QR_SIGNING_SECRET", "dev-please-change-me")

def _sign_qr_path(code: str, exp_ts: int) -> str:
    # payload: "code|exp"
    msg = f"{code}|{exp_ts}".encode("utf-8")
    return hmac.new(QR_SIGNING_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def _verify_qr_signature(code: str, exp_ts: int, sig: str) -> bool:
    if exp_ts < int(time.time()):
        return False
    expected = _sign_qr_path(code, exp_ts)
    return hmac.compare_digest(expected, sig or "")

# --- storage roots (ABSOLUTE) ---
# Prefer an env var if you have one, else default to repo-root-relative ./storage/eyba/hero
# On your machine this should resolve to: D:\EcodiaOS\storage\eyba\hero
REPO_ROOT = Path(os.getenv("REPO_ROOT") or Path(__file__).resolve().parents[3] if len(Path(__file__).resolve().parents) >= 4 else Path.cwd())
DEFAULT_HERO_DIR = REPO_ROOT / "storage" / "eyba" / "hero"
HERO_DIR = Path(os.getenv("HERO_DIR", str(DEFAULT_HERO_DIR))).resolve()

HERO_DIR.mkdir(parents=True, exist_ok=True)  # safe in dev
def short_url_for_code(code: str) -> str:
    # Next.js QR landing lives at /q/[code]
    return f"{PUBLIC_BASE.rstrip('/')}/q/{code}"

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
    Uses WHERE type(r) IN [...] to avoid deprecation warnings.
    """
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile)<-[:OF]-(q:QR {code:$code})
        WHERE type(r) IN ['OWNS','MANAGES']
        RETURN b.id AS bid,
               coalesce(b.name, 'EYBA Partner') AS bname,
               coalesce(b.area, b.location) AS loc,
               q.code AS code
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

# ---------------------------
# HERO: Upload & Serve
# ---------------------------

@router.post("/hero_upload", response_model=dict)
async def hero_upload(
    file: UploadFile = File(...),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    """
    Accept an image upload, validate it with Pillow, store as PNG,
    and return a *relative* URL like `/eyba/assets/hero/<slug>.png`.
    """
    # Make sure the user actually owns a business (basic scoping)
    bid = _get_owned_business_id(s, user_id=user_id)
    if not bid:
        raise HTTPException(status_code=403, detail="No business found for this account")

    # Read bytes
    data = await file.read()
    if not data or len(data) < 16:
        raise HTTPException(status_code=400, detail="Empty or invalid file")

    # Validate image with Pillow & convert to PNG
    try:
        img = Image.open(io.BytesIO(data))
        img.verify()  # quick structural check
    except UnidentifiedImageError:
        raise HTTPException(status_code=400, detail="The uploaded file is not a valid image")

    # Reopen to actually save (verify() leaves file in an unusable state)
    img = Image.open(io.BytesIO(data)).convert("RGB")

    # Generate name and path
    import secrets
    slug = secrets.token_hex(8)
    filename = f"{slug}.png"
    abs_path = os.path.join(HERO_DIR, filename)

    # Write PNG
    try:
        img.save(abs_path, format="PNG", optimize=True)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to store image")

    # Store relative path (no domain) on business profile (optional convenience)
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.hero_url = $rel
        """,
        bid=bid,
        rel=f"/eyba/assets/hero/{filename}",
    )

    # Respond with relative URL only (Next/Image safe; your UI strips domain anyway)
    return {"url": f"/eyba/assets/hero/{filename}"}
@router.get("/hero/{filename}")
def serve_hero(filename: str):
    """
    Serve stored hero PNG by filename (we always store as .png).
    """
    path = _abs_hero_path(filename)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    # Return as image/png; let FileResponse set content-length/etag
    return FileResponse(str(path), media_type="image/png")

@router.get("/_debug/hero_exists")
def hero_exists_debug(filename: str):
    """
    Quick dev aid: /eyba/assets/_debug/hero_exists?filename=xxxx.png
    Tells you the exact resolved directory and whether the file exists.
    """
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

@router.get("/qr/{code}.png")
def qr_png(
    code: str,
    size: int = Query(1024, ge=128, le=4096),
    exp: int = Query(..., description="unix expiry"),
    sig: str = Query(..., description="hmac signature"),
    s: Session = Depends(session_dep),
):
    # verify signature (public view, no login needed)
    if not _verify_qr_signature(code, exp, sig):
        raise HTTPException(status_code=401, detail="Invalid or expired signature")

    # (optional) ensure the QR actually exists in DB, but don't tie to current user
    rec = s.run(
        "MATCH (q:QR {code:$code}) RETURN q.code AS code LIMIT 1",
        code=code,
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="QR not found")

    link = short_url_for_code(code)

    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=3,
    )
    qr.add_data(link)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").resize((size, size))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")


@router.get("/qr/{code}/poster.pdf")
def qr_poster_pdf(
    code: str,
    exp: int = Query(..., description="unix expiry"),
    sig: str = Query(..., description="hmac signature"),
    s: Session = Depends(session_dep),
):
    if not _verify_qr_signature(code, exp, sig):
        raise HTTPException(status_code=401, detail="Invalid or expired signature")

    # get business/meta for nicer poster text (no owner check now)
    rec = s.run(
        """
        MATCH (q:QR {code:$code})<-[:OF]-(b:BusinessProfile)
        RETURN coalesce(b.name, 'EYBA Partner') AS bname,
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

    # ... (keep your existing ReportLab PDF building exactly the same, but use business_name/location_name)
    # 1) build QR PNG in-memory
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_Q,
        box_size=10,
        border=2,
    )
    qr.add_data(link)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white")
    png_bytes = io.BytesIO()
    qr_img.save(png_bytes, format="PNG")
    png_bytes.seek(0)

    # 2) build A4 poster (use your existing code, swapping meta vars)
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4
    margin = 18 * mm
    inner_w = W - 2 * margin
    inner_h = H - 2 * margin

    c.setFillColor(BRAND_CREAM)
    c.roundRect(margin, margin, inner_w, inner_h, 12, fill=1, stroke=0)
    c.setFillColor(BRAND_FOREST)
    c.roundRect(margin, H - margin - 28 * mm, inner_w, 28 * mm, 10, fill=1, stroke=0)

    c.setFillColor(BRAND_WHITE)
    c.setFont("Helvetica-Bold", 26)
    c.drawString(margin + 12 * mm, H - margin - 12 * mm, "EYBA - Earn eco here")

    c.setFont("Helvetica", 13)
    line2 = f"{business_name}" + (f" · {location_name}" if location_name else "")
    c.drawString(margin + 12 * mm, H - margin - 19 * mm, line2)

    qr_size = 90 * mm
    qr_x = margin + (inner_w - qr_size) / 2
    qr_y = margin + (inner_h - qr_size) / 2 - 8 * mm
    c.drawImage(png_bytes, qr_x, qr_y, qr_size, qr_size, mask="auto")

    c.setFillColor(BRAND_WHITE)
    c.roundRect(qr_x - 8 * mm, qr_y - 22 * mm, qr_size + 16 * mm, 18 * mm, 8, fill=1, stroke=0)
    c.setFillColor(BRAND_BLACK)
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(qr_x + qr_size / 2, qr_y - 10 * mm, "Scan to claim eco")

    c.setFont("Helvetica", 10)
    c.setFillColor(BRAND_BLACK)
    c.drawCentredString(W / 2, margin + 10 * mm, "Young people earn eco for visiting, learning, and acting.")
    c.setFont("Helvetica", 9)
    c.setFillColor(BRAND_FOREST)
    c.drawCentredString(W / 2, margin + 6 * mm, link)

    c.showPage()
    c.save()
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf")
from fastapi import Body

@router.post("/qr_signed_url")
def qr_signed_url(
    code: str = Body(..., embed=True),
    minutes_valid: int = Body(30, embed=True),  # default 30 minutes
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    # make sure the caller actually owns this code before we mint a signed URL
    _get_owned_qr_meta(s, user_id=user_id, code=code)

    exp = int(time.time()) + (minutes_valid * 60)
    sig = _sign_qr_path(code, exp)
    return {
        "png": f"/eyba/assets/qr/{code}.png?exp={exp}&sig={sig}",
        "pdf": f"/eyba/assets/qr/{code}/poster.pdf?exp={exp}&sig={sig}",
        "exp": exp,
    }
