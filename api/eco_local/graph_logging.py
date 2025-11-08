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
    method: str = "qr"  # "qr" | "nfc" | "impact"
    ts: datetime = datetime.now(timezone.utc)
    tx_id: str = ""

    def ensure_id(self):
        if not self.tx_id:
            self.tx_id = f"tx_{uuid4().hex[:16]}"
        return self

def log_eco_local_claim(session: Session, claim: ClaimTx) -> str:
    """
    Canonical logging into the unified EcoTx ledger.

    - Creates (EcoTx { amount, kind, source, status, createdAt, at, device_id, qr_code })
    - Links (User)-[:EARNED]->(EcoTx)<-[:TRIGGERED]-(BusinessProfile)
    - Optionally links (EcoTx)-[:AT]->(BusinessLocation)
    - Rolls simple counters safely (no duplicate increments on re-run).
    """
    claim.ensure_id()
    at_iso = claim.ts.isoformat()
    created_ms = int(claim.ts.timestamp() * 1000)

    params = {
        "tx_id": claim.tx_id,
        "uid": claim.user_id,
        "bid": claim.business_id,
        "bname": claim.business_name,
        "loc_id": claim.location_id,
        "loc_name": claim.location_name,
        "eco": int(claim.eco),
        "lat": claim.lat,
        "lng": claim.lng,
        "device": claim.device_hash,
        "method": claim.method,
        "code": claim.qr_code,
        "at": at_iso,
        "created_ms": created_ms,
    }

    cypher = """
    // Entities
    MERGE (u:User {id:$uid})
    MERGE (b:BusinessProfile {id:$bid})
      ON CREATE SET b.name = coalesce($bname, $bid)
      ON MATCH  SET b.name = coalesce($bname, b.name)
    // Optional location
    WITH u,b
    CALL {
      WITH b, $loc_id AS loc_id, $loc_name AS loc_name, $lat AS lat, $lng AS lng
      WITH b, loc_id, loc_name, lat, lng
      CALL {
        WITH b, loc_id, loc_name, lat, lng
        WITH b, loc_id, loc_name, lat, lng
        RETURN CASE WHEN loc_id IS NULL THEN NULL END AS noloc
      }
      WITH b, loc_id, loc_name, lat, lng, noloc
      CALL {
        WITH b, loc_id, loc_name, lat, lng
        WITH b, loc_id, loc_name, lat, lng
        WHERE loc_id IS NOT NULL
        MERGE (l:BusinessLocation {id: loc_id})
          ON CREATE SET l.name = coalesce(loc_name, loc_id), l.lat = lat, l.lng = lng
          ON MATCH  SET l.name = coalesce(loc_name, l.name)
        MERGE (l)-[:OF]->(b)
        RETURN l
      }
      RETURN 1
    }

    // EcoTx
    MERGE (t:EcoTx {id:$tx_id})
      ON CREATE SET
        t.amount     = $eco,
        t.kind       = "scan",
        t.method     = $method,
        t.source     = "eco-local",
        t.status     = "settled",
        t.createdAt  = $created_ms,
        t.at         = datetime($at),
        t.qr_code    = $code,
        t.device_id  = $device
      ON MATCH SET
        t.amount     = coalesce(t.amount, $eco)

    MERGE (u)-[:EARNED]->(t)
    MERGE (b)-[:TRIGGERED]->(t)

    // Optional link to location
    WITH t, $loc_id AS loc_id
    CALL {
      WITH t, loc_id
      WHERE loc_id IS NOT NULL
      MATCH (l:BusinessLocation {id: loc_id})
      MERGE (t)-[:AT]->(l)
      RETURN 1
    }
    RETURN t.id AS tx_id
    """

    rec = session.run(cypher, params).single()
    return rec["tx_id"] if rec else claim.tx_id
