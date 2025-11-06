# api/eco_local/qr.py
from __future__ import annotations

import io
import qrcode
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from neo4j import Session

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id
from site_backend.api.eco_local.assets import short_url_for_code, app_payload_for_code

router = APIRouter(prefix="/eco_local/qr", tags=["eco_local-qr"])

def _owned_qr_code(s: Session, *, user_id: str, business_id: str) -> str:
    """
    Ensure the caller OWNS/MANAGES the business, and fetch its QR code.
    """
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
@router.get("/business.png")
def business_qr_png(
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: str = Query(..., description="Your business id"),
    size: int = Query(1024, ge=128, le=4096),
    # Optional switch if you ever want a web URL instead
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