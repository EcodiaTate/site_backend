# site_backend/api/eco_local/qr.py
from __future__ import annotations

import io
import re
import qrcode
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.eco_local.assets import short_url_for_code, app_payload_for_code

router = APIRouter(prefix="/eco-local/qr", tags=["eco_local-qr"])

# ─────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────

def _owned_qr_code(s: Session, *, user_id: str, business_id: str) -> str:
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile {id:$bid})
        WHERE type(r) IN ['OWNS','MANAGES']
        OPTIONAL MATCH (q:QR)-[:OF]->(b)
        RETURN q.code AS code
        """,
        uid=user_id, bid=business_id,
    ).single()
    if not rec or not rec["code"]:
        raise HTTPException(status_code=404, detail="QR not found for this business")
    return rec["code"]

def _strip_prefix(code: str) -> str:
    # Support payloads like "biz_ABC123..." → "ABC123..."
    m = re.match(r"^(?:biz_|qr_|code_)?(.+)$", code, re.IGNORECASE)
    return m.group(1) if m else code

def _normalize_offer(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize offer records so FE gets a consistent shape whether
    offers were created via owner.py (visible/type) or offers.py (status/eco_price),
    or older fields like redeem_eco / price_eco / eco.
    """
    o = dict(raw or {})

    # Prefer a business_id in the record; fall back to join-provided one
    business_id = o.get("business_id") or o.get("bid")

    # Unified ECO price resolution
    eco_price = (
        o.get("eco_price", None)
        if o.get("eco_price", None) not in ("", None)
        else o.get("redeem_eco", None)
    )
    if eco_price in ("", None):
        eco_price = o.get("price_eco", None)
    if eco_price in ("", None):
        # Some older data stored total/amount under 'eco' or 'amount'
        eco_price = o.get("eco", None)
    if eco_price in ("", None):
        eco_price = o.get("amount", None)

    eco_price_out: Optional[int]
    try:
        eco_price_out = None if eco_price in (None, "") else int(eco_price)
    except Exception:
        eco_price_out = None

    # Visible/status normalization
    status = o.get("status")
    if not status:
        status = "active" if bool(o.get("visible", True)) else "hidden"

    # tags may be list or string
    tags_val = o.get("tags")
    if isinstance(tags_val, str):
        tags = [t.strip() for t in tags_val.split("|") if t.strip()]
    else:
        tags = tags_val or []

    return {
        "id": o.get("id"),
        "business_id": business_id,
        "title": o.get("title"),
        "blurb": o.get("blurb"),
        "status": status,
        "visible": bool(o.get("visible", status == "active")),  # keep legacy flag too
        "eco_price": eco_price_out,
        "fiat_cost_cents": o.get("fiat_cost_cents"),
        "stock": o.get("stock"),
        "url": o.get("url"),
        "valid_until": o.get("valid_until") or o.get("validUntil"),
        "type": o.get("type") or "perk",
        "tags": tags,
        "createdAt": o.get("created_at") or o.get("createdAt"),
        "template_id": o.get("template_id"),
    }

def _is_visible(o: Dict[str, Any]) -> bool:
    # status or visible gate
    if (o.get("status") or "active") != "active" and not o.get("visible", False):
        return False
    # Optional stock/date checks if present
    eco_price = o.get("eco_price")
    if eco_price is not None:
        try:
            if int(eco_price) < 0:
                return False
        except Exception:
            return False
    stock = o.get("stock")
    if stock is not None:
        try:
            if int(stock) <= 0:
                return False
        except Exception:
            return False
    return True

# ─────────────────────────────────────────────────────────
# QR image for owners
# ─────────────────────────────────────────────────────────

@router.get("/business.png")
def business_qr_png(
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: str = Query(..., description="Your business id"),
    size: int = Query(1024, ge=128, le=4096),
    kind: str = Query("app", pattern="^(app|web)$"),
):
    code = _owned_qr_code(s, user_id=user_id, business_id=business_id)
    value = app_payload_for_code(code) if kind == "app" else short_url_for_code(code)

    qr = qrcode.QRCode(version=None, error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=10, border=3)
    qr.add_data(value)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").resize((size, size))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return Response(content=buf.getvalue(), media_type="image/png")

# ─────────────────────────────────────────────────────────
# PUBLIC: Offers by scanned QR
#   POST /eco-local/qr/{qr_code}/offers
#   Example: /eco-local/qr/biz_a7c4291582/offers
# ─────────────────────────────────────────────────────────
