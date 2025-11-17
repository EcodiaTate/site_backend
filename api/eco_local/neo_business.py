# site_backend/api/eco_local/neo_business.py
from __future__ import annotations
from typing import Optional, List, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from neo4j import Session

# ---------- helpers ----------
def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"

def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

# ---------- standards ----------
def business_update_standards(
    s: Session,
    *,
    business_id: str,
    standards_eco: str,
    standards_sustainability: str,
    standards_social: str,
    certifications: Optional[List[str]] = None,
    links: Optional[List[str]] = None,
) -> Dict[str, Any]:
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        SET b.standards_eco            = $standards_eco,
            b.standards_sustainability = $standards_sustainability,
            b.standards_social         = $standards_social,
            b.certifications           = $certifications,
            b.links                    = $links
        RETURN {
          id: b.id,
          standards_eco: b.standards_eco,
          standards_sustainability: b.standards_sustainability,
          standards_social: b.standards_social,
          certifications: b.certifications,
          links: b.links
        } AS out
        """,
        bid=business_id,
        standards_eco=standards_eco.strip(),
        standards_sustainability=standards_sustainability.strip(),
        standards_social=standards_social.strip(),
        certifications=certifications or [],
        links=links or [],
    ).single()
    if not rec:
        raise ValueError("Business not found")
    return rec["out"]

# ---------- business setup / profile ----------
def business_init(
    s: Session,
    *,
    user_id: str,
    business_name: str,
    industry_group: str,
    size: str,
    area: str,
    pledge_tier: str,
) -> Dict[str, Any]:
    """
    Canonical rule: one BusinessProfile per user.
    Key by (user_id) and ensure an (id) on first create.
    Also ensure OWNS and a QR code.
    """
    qr_code = f"biz_{uuid4().hex[:10]}"

    rec = s.run(
        """
        // Ensure the user
        MERGE (u:User {id:$user_id})

        // Single business per user - key by user_id
        MERGE (b:BusinessProfile {user_id:$user_id})
          ON CREATE SET
            b.id             = randomUUID(),
            b.created_at     = datetime(),
            b.visible_on_map = true,
            // initialize common public fields to avoid warnings
            b.website='', b.tagline='', b.address='', b.hours='', b.description='',
            b.hero_url='', b.tags=[],
            b.eco_contributed_total=0, b.eco_given_total=0, b.minted_eco=0

        // Always keep latest provided props up to date
        SET b.name           = $name,
            b.industry_group = $industry_group,
            b.size           = $size,
            b.area           = $area,
            b.pledge_tier    = $pledge_tier,
            b.eco_mint_ratio = 1

        MERGE (u)-[:OWNS]->(b)

        WITH b
        MERGE (q:QR {code:$qr_code})
        MERGE (q)-[:OF]->(b)
        ON CREATE SET q.created_at = datetime()

        RETURN b.id AS business_id, q.code AS qr_code
        """,
        {
            "user_id": user_id,
            "name": business_name.strip(),
            "industry_group": industry_group.strip(),
            "size": size.strip(),
            "area": area.strip(),
            "pledge_tier": pledge_tier.strip(),
            "qr_code": qr_code,
        },
    ).single()

    return {"business_id": rec["business_id"], "qr_code": rec["qr_code"]}

def business_by_owner(s: Session, *, user_id: str) -> Optional[Dict[str, Any]]:
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[r]->(b:BusinessProfile)
        WHERE type(r) IN ['OWNS','MANAGES']
        OPTIONAL MATCH (q:QR)-[:OF]->(b)
        RETURN {
          id:b.id,
          name:b.name,
          industry_group:b.industry_group,
          size:b.size,
          area:b.area,
          pledge_tier:b.pledge_tier,
          eco_mint_ratio:b.eco_mint_ratio,
          website:b.website,
          tagline:b.tagline,
          address:b.address,
          hours:b.hours,
          description:b.description,
          hero_url:b.hero_url,
          lat:b.lat,
          lng:b.lng,
          visible_on_map: coalesce(b.visible_on_map, true),
          tags: coalesce(b.tags, []),
          qr_code:q.code
        } AS out
        """,
        uid=user_id,
    ).single()
    return rec["out"] if rec else None

def business_update_public_profile(
    s: Session,
    *,
    business_id: str,
    owner_user_id: str,
    fields: Dict[str, Any]
) -> Dict[str, Any]:
    allowed = {
        "name","tagline","website","address","hours","description","hero_url",
        "lat","lng","visible_on_map","tags"
    }
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        raise ValueError("No valid fields provided")

    sets = ", ".join([f"b.{k} = ${k}" for k in clean.keys()])

    rec = s.run(
        f"""
        MATCH (u:User {{id:$uid}})-[r]->(b:BusinessProfile {{id:$bid}})
        WHERE type(r) IN ['OWNS','MANAGES']
        SET {sets}
        RETURN {{
          id:b.id, name:b.name, tagline:b.tagline, website:b.website, address:b.address,
          hours:b.hours, description:b.description, hero_url:b.hero_url,
          lat:b.lat, lng:b.lng, visible_on_map:coalesce(b.visible_on_map,true),
          tags: coalesce(b.tags, [])
        }} AS out
        """,
        uid=owner_user_id, bid=business_id, **clean
    ).single()
    if not rec:
        raise PermissionError("Not owner/manager or business not found")
    return rec["out"]

# ---------- stripe contributions ----------
def stripe_record_contribution(
    s: Session,
    *,
    business_id: str,
    aud_cents: int,
) -> Dict[str, Any]:
    rec = s.run(
        "MATCH (b:BusinessProfile {id:$bid}) RETURN b.id AS bid",
        bid=business_id,
    ).single()
    if not rec:
        raise ValueError("Business not found")

    aud = int(aud_cents) / 100.0
    eco = int(round(aud * 1))  # 1:1
    tx_id = new_id("eco_tx")

    s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        MERGE (t:EcoTx {id:$tx_id})
          ON CREATE SET
            t.amount     = $eco,
            t.kind       = "contribution",
            t.source     = "stripe",
            t.status     = "settled",
            t.createdAt  = $now,
            t.at         = datetime({epochMillis: $now})
        MERGE (b)-[:TRIGGERED]->(t)
        SET b.eco_contributed_total = coalesce(b.eco_contributed_total,0) + $eco,
            b.minted_eco            = coalesce(b.minted_eco,0) + $eco
        """,
        bid=business_id, tx_id=tx_id, eco=eco, now=_now_ms(),
    )
    return {"ok": True, "tx_id": tx_id, "eco": eco, "business_id": business_id}

# ---------- metrics & activity ----------
def get_business_metrics(s: Session, *, business_id: str) -> Dict[str, Any]:
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        OPTIONAL MATCH (b)-[:TRIGGERED]->(t:EcoTx)
        WITH b, collect(t) AS txs
        WITH b, [x IN txs WHERE x IS NOT NULL |
            {
              amt: toInteger(coalesce(x.amount, x.eco, 0)),
              created: toInteger(coalesce(x.createdAt, timestamp()))
            }
        ] AS sane
        WITH b,
             coalesce(b.eco_contributed_total,0) AS contributed,
             coalesce(b.eco_given_total,0)       AS given,
             coalesce(b.minted_eco,0)            AS total,
             [p IN sane WHERE p.created >= $since_ms | p.amt] AS recent
        WITH b, contributed, given, total, reduce(s=0, x IN recent | s + x) AS last30
        RETURN {
          business_id: b.id,
          name: b.name,
          pledge_tier: b.pledge_tier,
          eco_mint_ratio: b.eco_mint_ratio,
          eco_contributed_total: contributed,
          eco_given_total: given,
          minted_eco: total,
          eco_velocity_30d: toFloat(last30) / 30.0
        } AS out
        """,
        bid=business_id,
        since_ms=int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000),
    ).single()
    if not rec:
        raise ValueError("Business not found")
    return rec["out"]

def get_business_activity(
    s: Session, *, business_id: str, limit: int = 50
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit or 50), 200))
    rows = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})-[:TRIGGERED]->(t:EcoTx)
        OPTIONAL MATCH (u:User)-[:EARNED]->(t)
        WITH t, u,
             toInteger(coalesce(t.createdAt, timestamp())) AS created_ms,
             toInteger(coalesce(t.amount, t.eco, 0))      AS amt,
             coalesce(t.kind, 'scan')                      AS knd,
             coalesce(t.source, 'eco_local')               AS src
        RETURN {
          id: t.id,
          kind: knd,
          source: src,
          amount: amt,
          createdAt: created_ms,
          user_id: u.id
        } AS row
        ORDER BY created_ms DESC
        LIMIT $lim
        """,
        bid=business_id, lim=limit
    ).data() or []
    return [r["row"] for r in rows]

# ---------- offers ----------
def create_offer(
    s: Session, *, business_id: str, title: str, blurb: str,
    offtype: str, visible: bool, redeem_eco: Optional[int],
    url: Optional[str], valid_until: Optional[str],
    tags: Optional[List[str]]
) -> Dict[str, Any]:
    oid = new_id("off")
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        CREATE (o:Offer {
          id:$oid, title:$title, blurb:$blurb, type:$type, visible:$visible,
          redeem_eco:$redeem_eco, url:$url, valid_until:$valid_until, tags:$tags,
          createdAt:timestamp()
        })
        MERGE (o)-[:OF]->(b)
        RETURN o
        """,
        bid=business_id, oid=oid, title=title.strip(), blurb=blurb.strip(),
        type=offtype, visible=bool(visible), redeem_eco=redeem_eco,
        url=url, valid_until=valid_until, tags=tags or [],
    ).single()
    if not rec:
        raise ValueError("Business not found")
    return rec["o"]

def list_offers(s: Session, *, business_id: str, visible_only: bool) -> List[Dict[str, Any]]:
    return [
        r["o"] for r in s.run(
            """
            MATCH (b:BusinessProfile {id:$bid})<-[:OF]-(o:Offer)
            WHERE $visible_only = false OR coalesce(o.visible,true) = true
            RETURN o
            ORDER BY coalesce(o.valid_until, date("2999-12-31")) ASC, o.createdAt ASC
            """,
            bid=business_id, visible_only=visible_only
        )
    ]

def patch_offer(s: Session, *, offer_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    if not fields:
        rec = s.run("MATCH (o:Offer {id:$oid}) RETURN o", oid=offer_id).single()
        if not rec: raise ValueError("Offer not found")
        return rec["o"]
    sets = ", ".join([f"o.{k} = ${k}" for k in fields.keys()])
    params = {"oid": offer_id, **fields}
    rec = s.run(f"MATCH (o:Offer {{id:$oid}}) SET {sets} RETURN o", **params).single()
    if not rec: raise ValueError("Offer not found")
    return rec["o"]

def delete_offer(s: Session, *, offer_id: str) -> None:
    s.run("MATCH (o:Offer {id:$oid}) DETACH DELETE o", oid=offer_id)
