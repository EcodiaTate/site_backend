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

@router.post("/{qr_code}/offers")
def offers_for_qr(
    qr_code: str,
    s: Session = Depends(session_dep),
    visible_only: bool = Query(True, description="Hide paused/hidden or out-of-stock/expired where applicable"),
) -> Dict[str, Any]:
    code = _strip_prefix(qr_code)

    # Find the business for this QR
    biz = s.run(
        """
        MATCH (q:QR {code:$code})-[:OF]->(b:BusinessProfile)
        RETURN b.id AS bid, b.name AS name
        """,
        code=code,
    ).single()
    if not biz:
        raise HTTPException(status_code=404, detail="Unknown QR code")

    bid = biz["bid"]
    bname = biz["name"]

    # Try multiple shapes and relationship names; UNION ALL then distinct on id.
    rows = s.run(
        """
        // Shape A: (b)-[:HAS_OFFER]->(o:Offer)
        MATCH (b:BusinessProfile {id:$bid})-[:HAS_OFFER]->(oA:Offer)
        WITH b, oA AS o
        RETURN o{ .*, business_id: coalesce(o.business_id, b.id) } AS offer
        UNION
        // Shape B: (o:Offer)-[:OF]->(b)
        MATCH (b:BusinessProfile {id:$bid})<-[:OF]-(oB:Offer)
        RETURN oB{ .*, business_id: coalesce(oB.business_id, b.id) } AS offer
        UNION
        // Shape C: other common rel names seen in data
        MATCH (b:BusinessProfile {id:$bid})<-[:AVAILABLE_AT|REDEEM_AT|REDEEMABLE_AT]-(oC:Offer)
        RETURN oC{ .*, business_id: coalesce(oC.business_id, b.id) } AS offer
        UNION
        // Shape D: offers carrying a raw business_id property only
        MATCH (b:BusinessProfile {id:$bid})
        MATCH (oD:Offer {business_id: $bid})
        RETURN oD{ .*, business_id: coalesce(oD.business_id, b.id) } AS offer
        """,
        bid=bid,
    )

    offers: List[Dict[str, Any]] = []
    for r in rows:
        norm = _normalize_offer(r["offer"])
        if visible_only and not _is_visible(norm):
            continue
        offers.append(norm)

    # Distinct by id (and drop empties)
    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for o in offers:
        oid = (o.get("id") or "").strip()
        if not oid or oid in seen:
            continue
        seen.add(oid)
        deduped.append(o)

    out: Dict[str, Any] = {
        "ok": True,
        "business_id": bid,
        "business_name": bname,
        "location_name": None,
        "offers": deduped,
        "count": len(deduped),
    }

    # If nothing matched, return diagnostics so you can see which branches have data.
    if not deduped:
        diag = s.run(
            """
            MATCH (b:BusinessProfile {id:$bid})
            OPTIONAL MATCH (b)-[:HAS_OFFER]->(o1:Offer)
            WITH b, count(o1) AS a
            OPTIONAL MATCH (o2:Offer)-[:OF]->(b)
            WITH b, a, count(o2) AS bcnt
            OPTIONAL MATCH (o3:Offer)-[:AVAILABLE_AT|REDEEM_AT|REDEEMABLE_AT]->(b)
            WITH b, a, bcnt, count(o3) AS ccnt
            OPTIONAL MATCH (o4:Offer {business_id:$bid})
            RETURN a AS has_offer_out,
                   bcnt AS of_in,
                   ccnt AS alt_rels,
                   count(o4) AS prop_business_id
            """,
            bid=bid,
        ).single()

        out["ok"] = False
        out["reason"] = "no_offers"
        out["diagnostics"] = {
            "has_offer_out": int(diag["has_offer_out"]) if diag else 0,
            "of_in": int(diag["of_in"]) if diag else 0,
            "alt_rels": int(diag["alt_rels"]) if diag else 0,
            "prop_business_id": int(diag["prop_business_id"]) if diag else 0,
        }

    return out
