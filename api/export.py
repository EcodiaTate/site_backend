from fastapi import APIRouter, Depends, HTTPException, status
from neo4j import Session
from datetime import datetime, timedelta, timezone
from nanoid import generate
import os
from site_backend.core.user_guard import current_user_id
from site_backend.core.neo_driver import session_dep
from site_backend.api.models.prefs import DataExportJobOut
from site_backend.api.storage_gcs import signed_url

router = APIRouter(prefix="/data-export", tags=["data-export"])

def row_to_job(r) -> DataExportJobOut:
    j = r["j"]
    return DataExportJobOut(
        id=j["id"], user_id=j["user_id"], status=j.get("status","PENDING"),
        bucket=j.get("bucket"), objectKey=j.get("objectKey"),
        readyAt=j.get("readyAt"), expiresAt=j.get("expiresAt"),
        createdAt=j.get("createdAt"), updatedAt=j.get("updatedAt"),
    )

@router.post("", response_model=dict, status_code=status.HTTP_202_ACCEPTED)
def create_job(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    # throttle: if a recent pending/processing exists, return it
    rec = s.run("""
        MATCH (u:User {id: $uid})-[:REQUESTED]->(j:DataExportJob)
        WHERE j.status IN ['PENDING','PROCESSING']
        RETURN j ORDER BY j.createdAt DESC LIMIT 1
    """, uid=user_id).single()
    if rec:
        j = row_to_job(rec)
        return {"jobId": j.id, "status": j.status}

    now = datetime.now(timezone.utc).isoformat()
    job_id = generate(size=16)
    s.run("""
        MERGE (u:User {id: $uid})
        CREATE (j:DataExportJob {
            id: $jid, user_id: $uid, status: 'PENDING',
            createdAt: $now, updatedAt: $now
        })
        MERGE (u)-[:REQUESTED]->(j)
    """, uid=user_id, jid=job_id, now=now).consume()

    # Optional: ping worker
    worker = os.getenv("EXPORT_WORKER_URL")
    if worker:
        import requests
        try: requests.post(f"{worker}/internal/workers/export-user", params={"jobId": job_id}, timeout=2.0)
        except Exception: pass

    return {"jobId": job_id, "status": "PENDING"}

@router.get("", response_model=DataExportJobOut | None)
def latest_job(
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    rec = s.run("""
        MATCH (u:User {id: $uid})-[:REQUESTED]->(j:DataExportJob)
        RETURN j ORDER BY j.createdAt DESC LIMIT 1
    """, uid=user_id).single()
    if not rec:
        return None
    return row_to_job(rec)

@router.get("/{job_id}/download", response_model=dict)
def download_signed(
    job_id: str,
    user_id: str = Depends(current_user_id),
    s: Session = Depends(session_dep),
):
    rec = s.run("""
        MATCH (u:User {id: $uid})-[:REQUESTED]->(j:DataExportJob {id: $jid})
        RETURN j
    """, uid=user_id, jid=job_id).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Not found")

    j = rec["j"]
    if j.get("status") != "READY" or not j.get("objectKey"):
        raise HTTPException(status_code=409, detail="Not ready")

    url = signed_url(j["objectKey"], expires_seconds=15 * 60)
    return {"url": url}
