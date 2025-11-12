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
from typing import Optional, Tuple

import qrcode
from fastapi import APIRouter, HTTPException, Depends, Query, UploadFile, File, Body
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse, RedirectResponse
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from neo4j import Session
from PIL import Image, UnidentifiedImageError

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.core.paths import UPLOAD_ROOT  # <- canonical uploads root (…/site_backend/data/uploads)

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

# ---------- Storage locations ----------
# Avatars already live under UPLOAD_ROOT/avatars; heroes mirror that:
UPLOADS_DIR: Path = UPLOAD_ROOT
HERO_DIR: Path = (UPLOADS_DIR / "heroes").resolve()
HERO_DIR.mkdir(parents=True, exist_ok=True)

# Allowed hero file extensions we’ll *read* (we always store .webp)
_ALLOWED_EXT = (".png", ".webp", ".jpg", ".jpeg")

# Legacy read locations (compat)
LEGACY_HERO_DIRS = [
    (UPLOADS_DIR / "hero").resolve(),                      # singular
    (UPLOADS_DIR / "eco-local" / "hero").resolve(),        # very old
]

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def short_url_for_code(code: str) -> str:
    return f"{PUBLIC_BASE.rstrip('/')}/q/{code}"

def app_payload_for_code(code: str) -> str:
    return f"eco-local:{code}"

def _sign_qr_path(code: str, exp_ts: int) -> str:
    msg = f"{code}|{exp_ts}".encode("utf-8")
    return hmac.new(QR_SIGNING_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def _verify_qr_signature(code: str, exp_ts: int, sig: str) -> bool:
    if exp_ts < int(time.time()):
        return False
    expected = _sign_qr_path(code, exp_ts)
    return hmac.compare_digest(expected, sig or "")

def _assert_hero_filename(fname: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+\.(?:png|webp|jpg|jpeg)", fname or "", flags=re.I):
        raise HTTPException(status_code=404, detail="Not found")
    return fname

def _safe_stem(stem: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", stem or ""):
        raise HTTPException(status_code=404, detail="Not found")
    return stem

def _abs_hero_path(filename: str) -> Path:
    filename = _assert_hero_filename(filename)
    p = (HERO_DIR / filename).resolve()
    if HERO_DIR not in p.parents and p != HERO_DIR:
        raise HTTPException(status_code=404, detail="Not found")
    return p

def _try_paths_for(stem_or_filename: str) -> Optional[Tuple[Path, str]]:
    """
    Resolve an on-disk hero by checking /uploads/heroes first, then legacy dirs.
    Accepts either 'abc123.png' or bare 'abc123'.
    Returns (path, media_type) or None.
    """
    candidates: list[Path] = []

    if "." in stem_or_filename:
        try:
            fname = _assert_hero_filename(stem_or_filename)
        except HTTPException:
            return None
        candidates.append((HERO_DIR / fname).resolve())
        for d in LEGACY_HERO_DIRS:
            candidates.append((d / fname).resolve())
    else:
        stem = _safe_stem(stem_or_filename)
        for ext in _ALLOWED_EXT:
            candidates.append((HERO_DIR / f"{stem}{ext}").resolve())
        for d in LEGACY_HERO_DIRS:
            for ext in _ALLOWED_EXT:
                candidates.append((d / f"{stem}{ext}").resolve())

    for p in candidates:
        base_dirs = [HERO_DIR, *LEGACY_HERO_DIRS]
        if not any((bd == p or bd in p.parents) for bd in base_dirs):
            continue
        if p.is_file():
            media = f"image/{p.suffix.lstrip('.').lower()}"
            return p, media
    return None

@dataclass
class QRMeta:
    business_id: str
    business_name: str
    location_name: Optional[str]
    code: str

def _get_owned_qr_meta(s: Session, *, user_id: str, code: str) -> QRMeta:
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
# Hero storage (inline, no external service module)
# --------------------------------------------------------------------
def _to_webp_bytes(im: Image.Image) -> bytes:
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")
    buf = io.BytesIO()
    im.save(buf, "WEBP", quality=85, method=6)
    return buf.getvalue()

def _sharded_target(root: Path, sha: str, ext: str) -> Path:
    aa, bb = sha[:2], sha[2:4]
    p = root / aa / bb
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{sha}{ext}"

def set_business_hero_from_bytes(
    s: Session,
    business_id: str,
    raw: bytes,
    filename_hint: Optional[str] = None,
) -> str:
    # Content hash (stable name)
    sha = hashlib.sha256(raw).hexdigest()

    # Decode & convert to webp
    im = Image.open(io.BytesIO(raw))
    im.load()
    webp = _to_webp_bytes(im)

    # Write sharded file
    target = _sharded_target(HERO_DIR, sha, ".webp")
    target.write_bytes(webp)

    # Cache-busting rev
    rev = str(int(time.time()))
    aa, bb = sha[:2], sha[2:4]
    url = f"/uploads/heroes/{aa}/{bb}/{sha}.webp?v={rev}"

    # Persist on BusinessProfile
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.hero_sha = $sha,
            b.hero_rev = $rev,
            b.hero_url = $url
        """,
        bid=business_id, sha=sha, rev=rev, url=url,
    )
    return url
# replace your existing hero_upload with this

@router.post("/hero_upload", response_model=dict)
async def hero_upload(
    file: UploadFile = File(None),          # accept "file"
    upload: UploadFile = File(None),        # or legacy "upload"
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    uf = file or upload
    if uf is None:
        # make it clear what's wrong if someone still trips this
        raise HTTPException(status_code=400, detail="No file field provided. Use form field 'file'.")

    bid = _get_owned_business_id(s, user_id=user_id)
    if not bid:
        raise HTTPException(status_code=403, detail="No business found for this account")

    data = await uf.read()
    if not data or len(data) < 16:
        raise HTTPException(status_code=400, detail="Empty or invalid file")

    # sanity check (we convert internally to webp)
    try:
        Image.open(io.BytesIO(data)).verify()
    except Exception:
        raise HTTPException(status_code=400, detail="The uploaded file is not a valid image")

    url = set_business_hero_from_bytes(s, bid, data, uf.filename or None)
    # return both for callers that use `path` or `url`
    return {"url": url, "path": url}


@router.get("/hero/{key}")
def serve_hero(key: str):
    """
    Legacy-friendly hero serving:
    - If we find a file under /uploads/heroes → 307 to that canonical path.
    - Otherwise, serve directly from the legacy folder.
    """
    found = _try_paths_for(key)
    if not found:
        raise HTTPException(status_code=404, detail="Not found")
    path, media = found

    # If it's already in our canonical folder, redirect to its public path
    if HERO_DIR in path.parents:
        # detect shard if present; else flat filename
        rel = str(path.relative_to(UPLOADS_DIR)).replace("\\", "/")  # "heroes/aa/bb/sha.webp"
        u = f"/uploads/{rel}"
        resp = RedirectResponse(u, status_code=307)
        resp.headers["Cache-Control"] = "public, max-age=2592000, immutable"
        return resp

    # Legacy: serve the file as-is
    return FileResponse(
        str(path),
        media_type=media,
        headers={"Cache-Control": "public, max-age=2592000, immutable"}
    )

@router.head("/hero/{key}")
def serve_hero_head(key: str):
    found = _try_paths_for(key)
    if not found:
        raise HTTPException(status_code=404, detail="Not found")
    path, media = found
    return JSONResponse(
        content=None,
        headers={
            "Content-Length": str(path.stat().st_size),
            "Content-Type": media,
            "Cache-Control": "public, max-age=2592000, immutable",
        },
        status_code=200,
    )

@router.get("/_debug/hero_exists")
def hero_exists_debug(filename: str):
    f = _try_paths_for(filename)
    if not f:
        return JSONResponse(
            {
                "ok": False,
                "error": "Not found",
                "hero_dir": str(HERO_DIR),
                "legacy_dirs": [str(p) for p in LEGACY_HERO_DIRS],
                "dir_exists": HERO_DIR.is_dir(),
                "dir_contents_sample": sorted(
                    [str(p.relative_to(UPLOADS_DIR)) for p in HERO_DIR.rglob("*") if p.suffix.lower() in _ALLOWED_EXT]
                )[:10],
            },
            status_code=404,
        )
    path, media = f
    rel = f"/uploads/{path.relative_to(UPLOADS_DIR)}".replace("\\", "/") if HERO_DIR in path.parents else None
    return {
        "ok": True,
        "resolved_path": str(path),
        "media": media,
        "hero_dir": str(HERO_DIR),
        "legacy_dirs": [str(p) for p in LEGACY_HERO_DIRS],
        "public_canonical": rel,
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
    minutes_valid: int = Body(30, embed=True),
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
):
    _get_owned_qr_meta(s, user_id=user_id, code=code)
    exp = int(time.time()) + (minutes_valid * 60)
    sig = _sign_qr_path(code, exp)
    return {
        "png": f"/eco-local/assets/qr/{code}.png?exp={exp}&sig={sig}",
        "pdf": f"/eco-local/assets/qr/{code}/poster.pdf?exp={exp}&sig={sig}",
        "exp": exp,
    }
