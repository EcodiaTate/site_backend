# api/services/impact.py
from __future__ import annotations
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from neo4j import Session

def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"

# -------------------- Impact Event Catalog (server-defined) --------------------
IMPACT_EVENTS: Dict[str, Dict[str, Any]] = {
    "bring_your_cup": {
        "label": "Bring your cup",
        "base_eco": 8,
        "criteria": {"proof": "cashier_or_photo"},
        "youth_cooldown_hours": 6,
        "business_cooldown_seconds": 2,
    },
    "plant_based_meal": {
        "label": "Plant-based meal",
        "base_eco": 12,
        "criteria": {"min_spend_aud": 8, "proof": "pos_amount_or_cashier"},
        "youth_cooldown_hours": 12,
        "business_cooldown_seconds": 2,
    },
    "active_transport": {
        "label": "Active transport to store",
        "base_eco": 10,
        "criteria": {"proof": "attestation"},
        "youth_cooldown_hours": 24,
        "business_cooldown_seconds": 2,
    },
    "community_volunteer": {
        "label": "Community volunteer contribution",
        "base_eco": 20,
        "criteria": {"proof": "photo_or_org_code"},
        "youth_cooldown_hours": 24,
        "business_cooldown_seconds": 2,
    },
}

# -------------------- BIS: computation & storage --------------------
def upsert_impact_inputs(
    s: Session,
    *,
    business_id: str,
    practices: Dict[str, Any],
    social: Dict[str, Any],
    certifications: List[str],
    transparency: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Save normalized inputs on BusinessProfile; a separate method computes BIS from them.
    """
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.impact_practices = $pr,
            b.impact_social = $so,
            b.impact_certifications = $certs,
            b.impact_transparency = $tr,
            b.impact_updated_at = timestamp()
        """,
        bid=business_id, pr=practices, so=social, certs=certifications, tr=transparency
    )
    return {"ok": True}

def _score_practices(pr: Dict[str, Any]) -> int:
    # Expect normalized 0..5 answers, weight to 0..45 total
    def subscore(k): return max(0, min(5, int(pr.get(k, 0))))
    energy = subscore("energy") * 3   # 0..15
    waste  = subscore("waste") * 3    # 0..15
    source = subscore("sourcing") * 3 # 0..15
    return energy + waste + source    # 0..45

_CERT_POINTS = {
    "b_corp": 12,
    "climate_active": 8,
    "iso_14001": 6,
    "carbon_neutral_org": 8,
}
def _score_certs(certs: List[str]) -> int:
    pts = 0
    for c in certs or []:
        pts += _CERT_POINTS.get(c.lower(), 2)  # unknown gets 2
    return min(20, pts)

def _score_social(so: Dict[str, Any]) -> int:
    # Expect booleans/levels 0..5 → scale to 0..15
    inc = int(so.get("inclusive_hiring", 0))
    youth = int(so.get("youth_support", 0))
    comm = int(so.get("community_initiatives", 0))
    total5 = max(0, min(5, inc)) + max(0, min(5, youth)) + max(0, min(5, comm))
    return min(15, total5 * 1)  # 0..15

def _score_transparency(tr: Dict[str, Any]) -> int:
    pts = 0
    if tr.get("public_standards"): pts += 4
    if tr.get("impact_report"): pts += 4
    if tr.get("third_party_audit"): pts += 4
    return min(10, pts)

def compute_and_store_bis(s: Session, *, business_id: str) -> Dict[str, Any]:
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        RETURN b.impact_practices AS pr, b.impact_social AS so,
               b.impact_certifications AS certs, b.impact_transparency AS tr,
               b.impact_score AS old, b.impact_updated_at AS upd
        """,
        bid=business_id
    ).single()
    if not rec:
        raise ValueError("Business not found")
    pr = rec["pr"] or {}
    so = rec["so"] or {}
    certs = rec["certs"] or []
    tr = rec["tr"] or {}

    base = _score_practices(pr) + _score_social(so) + _score_certs(certs) + _score_transparency(tr) # 0..90
    # Freshness decay: if older than ~90 days, subtract up to 10
    decay = 0
    if rec["upd"]:
        age_days = max(0, (datetime.now(timezone.utc).timestamp()*1000 - rec["upd"]) / (1000*60*60*24))
        decay = min(10, int(age_days // 30))  # 1 point per ~month, cap 10
    bis = max(0, min(100, base - decay))

    s.run("MATCH (b:BusinessProfile {id:$bid}) SET b.impact_score=$bis", bid=business_id, bis=int(bis))
    # History point
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        MERGE (p:ImpactScorePoint {id:$pid})
          ON CREATE SET p.score=$bis, p.createdAt=timestamp()
        MERGE (b)-[:HAS_IMPACT_SCORE]->(p)
        """,
        bid=business_id, pid=new_id("bispt"), bis=int(bis)
    )
    return {"impact_score": int(bis)}

def bis_multiplier(score: int) -> float:
    score = max(0, min(100, int(score)))
    return 0.7 + 0.6 * (score / 100.0)  # 0.7 .. 1.3

# -------------------- Business enables events --------------------
def enable_events_for_business(s: Session, *, business_id: str, event_keys: List[str]) -> Dict[str, Any]:
    valid = [k for k in (event_keys or []) if k in IMPACT_EVENTS]
    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.enabled_event_keys = $keys
        """,
        bid=business_id, keys=valid
    )
    return {"ok": True, "enabled": valid}

# -------------------- Scan & Mint --------------------
def record_scan_mint(
    s: Session,
    *,
    business_id: str,
    youth_id: str,
    event_key: str,
    evidence: Optional[Dict[str, Any]] = None,
    client_ip: Optional[str] = None,
    device_id: Optional[str] = None,
) -> Dict[str, Any]:
    if event_key not in IMPACT_EVENTS:
        raise ValueError("Unknown event")
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        OPTIONAL MATCH (y:User {id:$yid})
        RETURN b.enabled_event_keys AS enabled, b.impact_score AS bis, y.id AS yid
        """, bid=business_id, yid=youth_id
    ).single()
    if not rec or not rec["yid"]:
        raise ValueError("Business or youth not found")
    enabled = rec["enabled"] or []
    if event_key not in enabled:
        raise PermissionError("Event not enabled for this business")

    # Cooldowns
    ev = IMPACT_EVENTS[event_key]
    youth_hours = int(ev.get("youth_cooldown_hours", 6))
    business_burst = int(ev.get("business_cooldown_seconds", 2))

    # Per-youth cooldown on this business+event
    rows = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})-[:TRIGGERED]->(t:EcoTx {kind:'scan'})<-[:EARNED]-(y:User {id:$yid})
        WHERE t.event_key=$ek AND t.createdAt >= $sinceYouth
        RETURN count(t) AS c
        """,
        bid=business_id, yid=youth_id, ek=event_key,
        sinceYouth=_now_ms() - youth_hours*60*60*1000
    ).single()
    if rows and int(rows["c"]) > 0:
        return {"ok": False, "reason": "youth_cooldown", "awarded_eco": 0}

    # Simple business burst control (avoid mass scans in a second)
    rows2 = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})-[:TRIGGERED]->(t:EcoTx {kind:'scan'})
        WHERE t.createdAt >= $sinceBiz
        RETURN count(t) AS c
        """,
        bid=business_id, sinceBiz=_now_ms() - business_burst*1000
    ).single()
    if rows2 and int(rows2["c"]) >= 3:
        return {"ok": False, "reason": "business_burst", "awarded_eco": 0}

    # Compute ECO
    bis = int(rec["bis"] or 50)
    mult = bis_multiplier(bis)
    base = int(IMPACT_EVENTS[event_key]["base_eco"])
    eco = max(1, int(round(base * mult)))

    txid = new_id("eco_tx")

    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        MATCH (y:User {id:$yid})
        MERGE (t:EcoTx {id:$txid})
          ON CREATE SET t.amount=$eco,
                        t.kind='scan',
                        t.event_key=$ek,
                        t.createdAt=$now,
                        t.evidence=$evidence,
                        t.client_ip=$cip,
                        t.device_id=$dev
        MERGE (b)-[:TRIGGERED]->(t)
        MERGE (y)-[:EARNED]->(t)
        SET b.eco_given_total = coalesce(b.eco_given_total,0) + $eco,
            b.minted_eco       = coalesce(b.minted_eco,0) - $eco
        """,
        bid=business_id, yid=youth_id, txid=txid, eco=eco, ek=event_key,
        now=_now_ms(), evidence=evidence or {}, cip=client_ip, dev=device_id
    )

    return {
        "ok": True,
        "tx_id": txid,
        "awarded_eco": eco,
        "bis": bis,
        "multiplier": mult,
        "base_eco": base,
        "event_key": event_key,
    }
