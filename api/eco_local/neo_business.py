# api/services/neo_business.py
from __future__ import annotations
from typing import Optional, List, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from neo4j import Session

# ---------- IDs / Time ----------
def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"

def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ---------- Standards ----------
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
        SET b.standards_eco           = $standards_eco,
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


# ---------- Business Setup / Profile ----------
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
    biz_id = new_id("biz")
    qr_code = new_id("qr")
    rec = s.run(
        """
        MERGE (u:User {id:$uid})
        MERGE (b:BusinessProfile {id:$bid})
          ON CREATE SET b.name=$name,
                        b.industry_group=$industry_group,
                        b.size=$size,
                        b.area=$area,
                        b.pledge_tier=$pledge_tier,
                        // Initialize public fields to avoid UnknownPropertyKey warnings
                        b.website='',
                        b.tagline='',
                        b.address='',
                        b.hours='',
                        b.description='',
                        b.hero_url='',
                        b.tags=[],
                        b.eco_contributed_total=0,
                        b.eco_given_total=0,
                        b.minted_eco=0,
                        b.visible_on_map=true,
                        b.createdAt=timestamp()
        SET b.eco_mint_ratio = 1
        MERGE (u)-[:OWNS]->(b)
        MERGE (q:QR {code:$qr})
          ON CREATE SET q.active=true, q.createdAt=timestamp()
        MERGE (q)-[:OF]->(b)
        RETURN b.id AS business_id, q.code AS qr_code
        """,
        uid=user_id,
        bid=biz_id,
        name=business_name,
        industry_group=industry_group,
        size=size,
        area=area,
        pledge_tier=pledge_tier,
        qr=qr_code,
    ).single()
    return {"business_id": rec["business_id"], "qr_code": rec["qr_code"]}
def business_by_owner(s: Session, *, user_id: str) -> Optional[Dict[str, Any]]:
    rec = s.run(
        """
        MATCH (u:User {id:$uid})-[:OWNS]->(b:BusinessProfile)
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
    """
    Allowed keys now include tags.
    """
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
        MATCH (u:User {{id:$uid}})-[:OWNS]->(b:BusinessProfile {{id:$bid}})
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
        raise PermissionError("Not owner or business not found")
    return rec["out"]


# ---------- Stripe contributions (1:1 ECO) ----------
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
          ON CREATE SET t.amount=$eco,
                        t.kind="contribution",
                        t.source="stripe",
                        t.status="settled",
                        t.createdAt=$now
        MERGE (b)-[:CONTRIBUTED]->(t)
        SET b.eco_contributed_total = coalesce(b.eco_contributed_total,0) + $eco,
            b.minted_eco            = coalesce(b.minted_eco,0) + $eco
        """,
        bid=business_id, tx_id=tx_id, eco=eco, now=_now_ms(),
    )
    return {"ok": True, "tx_id": tx_id, "eco": eco, "business_id": business_id}


# ---------- Metrics & Activity ----------
def get_business_metrics(s: Session, *, business_id: str) -> Dict[str, Any]:
    rec = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})
        OPTIONAL MATCH (b)-[:CONTRIBUTED|TRIGGERED]->(t:EcoTx)
        WITH b, collect(t) AS txs
        WITH b,
             coalesce(b.eco_contributed_total,0) AS contributed,
             coalesce(b.eco_given_total,0)       AS given,
             coalesce(b.minted_eco,0)             AS total,
             [t IN txs WHERE t.createdAt >= $since | coalesce(t.amount,0)] AS recent
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
        since=(datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000,
    ).single()
    if not rec:
        raise ValueError("Business not found")
    return rec["out"]


def get_business_activity(
    s: Session, *, business_id: str, limit: int = 50
) -> List[Dict[str, Any]]:
    rows = s.run(
        """
        MATCH (b:BusinessProfile {id:$bid})-[:TRIGGERED|:CONTRIBUTED]->(t:EcoTx)
        OPTIONAL MATCH (u:User)-[:EARNED]->(t)
        RETURN {
          id: t.id,
          kind: t.kind,
          source: t.source,
          amount: coalesce(t.amount,0),
          createdAt: t.createdAt,
          user_id: u.id
        } AS row
        ORDER BY t.createdAt DESC
        LIMIT $lim
        """,
        bid=business_id, lim=max(1, min(limit, 200))
    )
    return [r["row"] for r in rows]


# ---------- Offers (unchanged storage; tags supported) ----------
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
            WHERE $visible_only = false OR o.visible = true
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
