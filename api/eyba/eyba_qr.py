# api/routers/eyba_qr.py
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from neo4j import Session
import qrcode
import io

from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

router = APIRouter(prefix="/eyba/qr", tags=["qr"])

@router.get("/business.png")
def business_qr_png(
    s: Session = Depends(session_dep),
    user_id: str = Depends(current_user_id),
    business_id: str = Query(...),
):
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile {id:$bid})
        WHERE type(r) IN ['OWNS','MANAGES']
        OPTIONAL MATCH (q:QR)-[:OF]->(b)
        RETURN q.code AS code
        """,
        uid=user_id, bid=business_id
    ).single()
    if not rec or not rec["code"]:
        raise HTTPException(status_code=404, detail="QR not found")

    # Encode the QR code (value can be your deep-link, or just the code)
    value = rec["code"]
    img = qrcode.make(value)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return Response(content=buf.getvalue(), media_type="image/png")
