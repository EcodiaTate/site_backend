from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional, Dict, Any, Set
import os
import logging

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from neo4j import Session

from site_backend.core.neo_driver import session_dep

log = logging.getLogger(__name__)

router = APIRouter(prefix="/transparency", tags=["transparency"])


def _admin_emails_from_env() -> Set[str]:
    """
    Shared source of truth for 'who counts as admin' by email.

    - Prefer ADMIN_EMAILS (comma-separated list)
    - Fallback to legacy ADMIN_EMAIL (single email)
    """
    raw = os.getenv("ADMIN_EMAILS")
    if raw:
        parts = [p.strip().lower() for p in raw.split(",")]
        return {p for p in parts if p}
    legacy = os.getenv("ADMIN_EMAIL")
    if legacy:
        return {legacy.strip().lower()}
    return set()


class AdminEcoLogItem(BaseModel):
    id: str = Field(..., description="ID of the underlying ECO tx or sidequest submission")
    kind: Literal["eco_tx", "sidequest"] = Field(..., description="Record type")
    created_at: datetime
    eco_delta: int = Field(..., description="Net ECO change (+/-)")
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    user_name: Optional[str] = None

    # Optional extra context
    title: Optional[str] = None          # e.g. sidequest title or tx label
    description: Optional[str] = None    # short human-readable summary
    source: Optional[str] = None         # eg 'sidequest', 'manual_adjustment', etc.
    meta: Dict[str, Any] = Field(default_factory=dict)


class AdminEcoLogResponse(BaseModel):
    admins: List[str]
    items: List[AdminEcoLogItem]


@router.get("/admin-eco-log", response_model=AdminEcoLogResponse)
def get_admin_eco_log(
    limit: int = Query(200, ge=1, le=1000),
    session: Session = Depends(session_dep),
):
    """
    Public, read-only log of ECO transactions + sidequest rewards
    for *admin* accounts only.

    This is intentionally public so anyone can verify that:
    - ECO for admins is earned via the same rules as everyone else
    - Admins aren't secretly minting free ECO for themselves
    """
    admin_emails = _admin_emails_from_env()
    if not admin_emails:
        # Fail soft: nothing to show rather than erroring.
        log.warning("AdminEcoLog: no admin emails configured; returning empty log")
        return AdminEcoLogResponse(admins=[], items=[])

    # NOTE: Adjust labels/properties to match your actual graph schema.
    # This Cypher assumes:
    # - (:User {id, email, name})
    # - (:EcoTx {id, eco_delta, created_at, description, source})
    # - (:SidequestSubmission {id, created_at, eco_reward, title})
    # - Relationships:
    #     (u:User)-[:MADE_TX]->(tx:EcoTx)
    #     (u:User)-[:SUBMITTED]->(sub:SidequestSubmission)
    cypher = """
    CALL {
      MATCH (u:User)-[:MADE_TX]->(tx:EcoTx)
      WHERE toLower(u.email) IN $admin_emails
      RETURN
        tx.id            AS id,
        'eco_tx'         AS kind,
        tx.created_at    AS created_at,
        tx.eco_delta     AS eco_delta,
        u.id             AS user_id,
        u.email          AS user_email,
        u.name           AS user_name,
        tx.description   AS title,
        tx.description   AS description,
        tx.source        AS source,
        {}               AS meta

      UNION ALL

      MATCH (u:User)-[:SUBMITTED]->(sub:SidequestSubmission)
      WHERE toLower(u.email) IN $admin_emails
      RETURN
        sub.id           AS id,
        'sidequest'      AS kind,
        sub.created_at   AS created_at,
        sub.eco_reward   AS eco_delta,
        u.id             AS user_id,
        u.email          AS user_email,
        u.name           AS user_name,
        sub.title        AS title,
        'Sidequest completion' AS description,
        'sidequest'      AS source,
        {}               AS meta
    }
    RETURN *
    ORDER BY created_at DESC
    LIMIT $limit
    """

    records = session.run(
        cypher,
        admin_emails=[e.lower() for e in admin_emails],
        limit=limit,
    )

    items: List[AdminEcoLogItem] = []
    for rec in records:
        # Neo4j driver returns naive datetime objects if property is datetime.
        created_at = rec["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

        items.append(
            AdminEcoLogItem(
                id=str(rec["id"]),
                kind=rec["kind"],
                created_at=created_at,
                eco_delta=int(rec["eco_delta"] or 0),
                user_id=rec.get("user_id"),
                user_email=rec.get("user_email"),
                user_name=rec.get("user_name"),
                title=rec.get("title"),
                description=rec.get("description"),
                source=rec.get("source"),
                meta=rec.get("meta") or {},
            )
        )

    return AdminEcoLogResponse(
        admins=sorted(admin_emails),
        items=items,
    )
