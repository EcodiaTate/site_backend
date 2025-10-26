from fastapi import APIRouter, Depends, HTTPException
from neo4j import Session
from datetime import datetime, timedelta, timezone
import io, json, zipfile, os
from site_backend.core.neo_driver import session_dep
from site_backend.api.storage_gcs import upload_bytes
from site_backend.api.email_stub import send_mail

router = APIRouter(prefix="/internal/workers", tags=["workers"])

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

@router.post("/export-user")
def run_export(jobId: str, s: Session = Depends(session_dep)):
    # load job + user
    rec = s.run("""
        MATCH (j:DataExportJob {id: $jid})<-[:REQUESTED]-(u:User)
        RETURN j, u
    """, jid=jobId).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Job not found")
    j, u = rec["j"], rec["u"]
    user_id = u["id"]
    email = u.get("email")

    # if not pending/processing, ignore
    if j.get("status") not in (None, "PENDING", "PROCESSING"):
        return {"ok": True, "status": j.get("status")}

    s.run("""
        MATCH (j:DataExportJob {id: $jid})
        SET j.status = 'PROCESSING', j.updatedAt = $now
    """, jid=jobId, now=_now_iso()).consume()

    try:
        # ===== collect datasets (extend as needed) =====
        rec = s.run("""
            MATCH (u:User {id: $uid})
            OPTIONAL MATCH (u)-[:HAS_NOTIFICATION_PREFS]->(np:NotificationPrefs)
            OPTIONAL MATCH (u)-[:HAS_PRIVACY_PREFS]->(pp:PrivacyPrefs)
            OPTIONAL MATCH (u)<-[:OWNED_BY]-(proj:Project)
            OPTIONAL MATCH (u)<-[:CREATED_BY]-(lp:LaunchpadItem)
            OPTIONAL MATCH (u)-[:HAS_PROFILE]->(yp:YouthProfile)
            OPTIONAL MATCH (u)-[:HAS_PROFILE]->(bp:BusinessProfile)
            WITH u, np, pp, collect(DISTINCT proj) AS projects, collect(DISTINCT lp) AS launchpad,
                 yp, bp
            RETURN u, np, pp, projects, launchpad, yp, bp
        """, uid=user_id).single()

        u_node = rec["u"]
        np, pp = rec["np"], rec["pp"]
        projects = rec["projects"] or []
        launchpad = rec["launchpad"] or []
        yp, bp = rec["yp"], rec["bp"]

        payload = {
            "generatedAt": _now_iso(),
            "user": dict(u_node),
            "notificationPrefs": dict(np) if np else None,
            "privacyPrefs": dict(pp) if pp else None,
            "youthProfile": dict(yp) if yp else None,
            "businessProfile": dict(bp) if bp else None,
            "projects": [dict(p) for p in projects],
            "launchpadItems": [dict(l) for l in launchpad],
            # TODO: add EYBA redemptions, comments, messages, etc.
        }

        # ===== write to ZIP in memory =====
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
            z.writestr("user.json", json.dumps(payload, indent=2, default=str))

        object_key = f"exports/{user_id}/{jobId}.zip"
        upload_bytes(object_key, buf.getvalue(), content_type="application/zip")

        ready = _now_iso()
        expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        s.run("""
            MATCH (j:DataExportJob {id: $jid})
            SET j.status = 'READY',
                j.objectKey = $key,
                j.bucket = $bucket,
                j.readyAt = $ready,
                j.expiresAt = $expires,
                j.updatedAt = $ready
        """, jid=jobId, key=object_key, bucket=os.environ["EXPORT_BUCKET"], ready=ready, expires=expires).consume()

        # notify user
        if email:
            origin = os.environ.get("APP_ORIGIN", "https://ecodia.au")
            send_mail(
                to=email,
                subject="Your Ecodia data export is ready",
                text=f"Your export is ready. Download it from {origin}/account/export",
            )

        return {"ok": True}
    except Exception as e:
        s.run("""
            MATCH (j:DataExportJob {id: $jid})
            SET j.status = 'FAILED', j.error = $err, j.updatedAt = $now
        """, jid=jobId, err=str(e), now=_now_iso()).consume()
        raise
