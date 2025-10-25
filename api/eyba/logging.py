# api/services/graph_logging.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from neo4j import Session


@dataclass
class ClaimTx:
    user_id: str
    business_id: str
    business_name: Optional[str]
    location_id: Optional[str]
    location_name: Optional[str]
    qr_code: str
    eco: int
    lat: Optional[float] = None
    lng: Optional[float] = None
    device_hash: Optional[str] = None
    method: str = "qr"  # or "nfc"
    ts: datetime = datetime.now(timezone.utc)
    tx_id: str = ""

    def ensure_id(self):
        if not self.tx_id:
            self.tx_id = f"tx_{uuid4().hex[:16]}"
        return self

def log_eyba_claim(session: Session, claim: ClaimTx) -> str:
    claim.ensure_id()
    params = {
        "user_id": claim.user_id,
        "business_id": claim.business_id,
        "business_name": claim.business_name,
        "location_id": claim.location_id,
        "location_name": claim.location_name,
        "eco": int(claim.eco),
        "code": claim.qr_code,
        "lat": claim.lat,
        "lng": claim.lng,
        "device": claim.device_hash,
        "method": claim.method,
        "tx_id": claim.tx_id,
        "at": claim.ts.isoformat(),
    }

    cypher = """
    // Ensure user + youth profile
    MERGE (u:User {id: $user_id})
    MERGE (p:YouthProfile {user_id: $user_id})
    MERGE (u)-[:HAS_PROFILE]->(p)
    ON CREATE SET p.eyba_points = 0, p.actions_completed = 0

    // Ensure business
    MERGE (b:BusinessProfile {id: $business_id})
      ON CREATE SET b.name = coalesce($business_name, $business_id)
      ON MATCH  SET b.name = coalesce($business_name, b.name)

    // Optionally ensure/create a location and link it to the business
    FOREACH (_ IN CASE WHEN $location_id IS NOT NULL THEN [1] ELSE [] END |
      MERGE (l:BusinessLocation {id: $location_id})
        ON CREATE SET l.name = coalesce($location_name, $location_id),
                      l.lat = $lat, l.lng = $lng
        ON MATCH  SET l.name = coalesce($location_name, l.name)
      MERGE (l)-[:OF]->(b)
    )

    // Merge SUPPORT link (used by profile rollups)
    MERGE (p)-[:SUPPORTED]->(b)

    // Create immutable transaction node
    CREATE (t:EYBATransaction {
      id: $tx_id,
      eco: $eco,
      at: datetime($at),
      method: $method,
      code: $code,
      lat: $lat,
      lng: $lng,
      device: $device
    })

    // Relate transaction to profile and business
    MERGE (p)-[:CLAIMED]->(t)
    MERGE (t)-[:FROM]->(b)

    // ⬇️ REQUIRED: move from WRITE to READ with a WITH
    WITH t, p, b, $location_id AS location_id

    OPTIONAL MATCH (loc:BusinessLocation {id: location_id})
    FOREACH (_ IN CASE WHEN loc IS NULL THEN [] ELSE [1] END |
      MERGE (t)-[:AT]->(loc)
    )

    // ⬇️ Another WITH before a SET/RETURN is not strictly required,
    // but keeps scope explicit and avoids accidental variable loss.
    WITH t, p

    // Update counters used by /profile
    SET p.eyba_points = coalesce(p.eyba_points, 0) + $eco,
        p.actions_completed = coalesce(p.actions_completed, 0) + 1

    RETURN t.id AS tx_id
    """

    rec = session.run(cypher, **params).single()
    return rec["tx_id"] if rec else claim.tx_id
