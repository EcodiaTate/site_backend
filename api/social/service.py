from __future__ import annotations
from typing import List, Dict, Any, Optional
from uuid import uuid4
from datetime import datetime
import os

from neo4j import Session, Transaction

# --------------------------------------------------------------------------
# Config / constants
# --------------------------------------------------------------------------
REQUEST_TTL_DAYS = 21
MAX_SEARCH_RESULTS = 40
LEADERBOARD_LIMIT = 20
ACTIVITY_LIMIT = 50

TIER_THRESHOLDS = {
    "seedling": 0,
    "sapling": 500,
    "canopy": 1500,
    "elder": 5000,
}

DEV_MODE = os.getenv("ECODIA_DEV_DIAGNOSTICS", "0") == "1"

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _now_iso() -> str:
    # Stored in properties for audit; we still prefer Neo4j datetime() for graph ops
    return datetime.utcnow().isoformat()

def expire_stale_requests(session: Session):
    """Mark pending FriendRequest older than TTL as expired."""
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (fr:FriendRequest {status:'pending'})
            WHERE fr.created_at < datetime() - duration({days:$ttl})
            SET fr.status = 'expired', fr.updated_at = datetime()
            """,
            ttl=REQUEST_TTL_DAYS,
        )
    )

def _user_basic(session: Session, uid: str) -> Optional[Dict[str, Any]]:
    rec = session.run(
        """
        MATCH (u:User {id:$uid})
        RETURN u.id as id, u.display_name as display_name,
               toInteger(coalesce(u.eco_reputation,0)) as eco_score
        """,
        uid=uid,
    ).single()
    return dict(rec) if rec else None


# --------------------------------------------------------------------------
# Core: list/search/friendship lifecycle
# --------------------------------------------------------------------------
def list_friends(session: Session, uid: str) -> List[Dict[str, Any]]:
    """
    Returns your friends (outgoing edge definition; you should have both directions after accept).
    """
    rows = session.run(
        """
        MATCH (:User {id:$uid})-[:FRIENDS_WITH]->(f:User)
        WHERE NOT EXISTS { MATCH (:User {id:$uid})-[:BLOCKED]->(f) }
          AND NOT EXISTS { MATCH (f)-[:BLOCKED]->(:User {id:$uid}) }
        RETURN f.id AS id,
               f.display_name AS display_name,
               toInteger(coalesce(f.eco_reputation,0)) AS eco_score
        ORDER BY display_name ASC
        """,
        uid=uid,
    )
    return [dict(r) for r in rows]
def list_requests(session: Session, uid: str) -> dict:
    expire_stale_requests(session)

    # INCOMING: someone → me
    incoming = session.run(
        """
        MATCH (fromU:User)-[:REQUESTED]->(fr:FriendRequest {status:'pending'})-[:TO]->(me:User {id:$uid})
        // mutuals between me and sender
        OPTIONAL MATCH (me)-[:FRIENDS_WITH]->(m:User)<-[:FRIENDS_WITH]-(fromU)
        WITH fr, fromU, count(DISTINCT m) AS mutuals
        RETURN
          fr.id AS id,
          fromU.id AS from_id,
          fromU.display_name AS from_name,
          'incoming' AS kind,
          toString(fr.created_at) AS at,
          mutuals
        ORDER BY fr.created_at DESC
        """,
        uid=uid,
    ).data()

    # OUTGOING: me → someone
    outgoing = session.run(
        """
        MATCH (me:User {id:$uid})-[:REQUESTED]->(fr:FriendRequest {status:'pending'})-[:TO]->(toU:User)
        RETURN
          fr.id AS id,
          toU.id AS to_id,
          toU.display_name AS to_name,
          'outgoing' AS kind,
          toString(fr.created_at) AS at
        ORDER BY fr.created_at DESC
        """,
        uid=uid,
    ).data()

    return {"incoming": incoming, "outgoing": outgoing}



def search_users(session: Session, uid: str, q: str) -> List[Dict[str, Any]]:
    """
    Balanced search:
      - display_name CONTAINS (case-insensitive) when len(q) >= 3
      - OR exact/prefix match on cand.id (case-insensitive)
    Excludes: self, existing friends, mutual blocks, and pending requests.
    Ordered by eco_score desc, then display_name asc.
    """
    q = (q or "").strip()
    if not q:
        return []

    rows = session.run(
        """
        MATCH (cand:User)
        WHERE cand.id <> $uid
          AND (
            // ID exact or prefix (case-insensitive)
            toLower(cand.id) = toLower($q)
            OR toLower(cand.id) STARTS WITH toLower($q)
            // Name contains gate: only when length ≥ 3
            OR (size($q) >= 3 AND toLower(cand.display_name) CONTAINS toLower($q))
          )
          AND NOT EXISTS { MATCH (:User {id:$uid})-[:FRIENDS_WITH]->(cand) }
          AND NOT EXISTS { MATCH (cand)-[:FRIENDS_WITH]->(:User {id:$uid}) }
          AND NOT EXISTS { MATCH (:User {id:$uid})-[:BLOCKED]->(cand) }
          AND NOT EXISTS { MATCH (cand)-[:BLOCKED]->(:User {id:$uid}) }
          AND NOT EXISTS {
            MATCH (:User {id:$uid})-[:REQUESTED]->(:FriendRequest {status:'pending'})-[:TO]->(cand)
          }
          AND NOT EXISTS {
            MATCH (cand)-[:REQUESTED]->(:FriendRequest {status:'pending'})-[:TO]->(:User {id:$uid})
          }
        RETURN cand.id AS id,
               cand.display_name AS display_name,
               toInteger(coalesce(cand.eco_reputation,0)) AS eco_score
        ORDER BY eco_score DESC, display_name ASC
        LIMIT $limit
        """,
        uid=uid,
        q=q,
        limit=MAX_SEARCH_RESULTS,
    )

    return [dict(r) for r in rows]


# ---------- Invite by email (privacy-first) ----------
def request_friend_by_email(session: Session, uid: str, email: str) -> Dict[str, Any]:
    # 1) sender must exist
    sender = session.run(
        "MATCH (u:User {id:$uid}) RETURN u.id AS id",
        uid=uid
    ).single()
    if not sender:
        return {"ok": True, **({"_dev": "sender_not_found"} if DEV_MODE else {})}

    # 2) target by multiple fields (no enumeration)
    rec = session.run(
        """
        MATCH (u:User)
        WHERE toLower(u.email) = toLower($email)
           OR toLower(u.primary_email) = toLower($email)
           OR any(e IN coalesce(u.emails, []) WHERE toLower(e) = toLower($email))
           OR any(e IN coalesce(u.auth_emails, []) WHERE toLower(e) = toLower($email))
        RETURN u.id AS id
        LIMIT 1
        """,
        email=(email or "").strip(),
    ).single()

    if rec and rec.get("id"):
        out = request_friend(session, uid, rec["id"])
        if DEV_MODE:
            out["_dev"] = "created"
        return out

    # Intentional no-op (privacy)
    return {"ok": True, **({"_dev": "target_not_found"} if DEV_MODE else {})}


# ---------- Transaction helpers ----------
def _tx_create_request(tx: Transaction, a: str, b: str, rid: str):
    tx.run(
        """
        MATCH (a:User {id:$a}), (b:User {id:$b})
        CREATE (a)-[:REQUESTED]->(fr:FriendRequest {
            id:$rid,
            status:'pending',
            created_at: datetime(),
            updated_at: datetime()
        })-[:TO]->(b)
        """,
        a=a, b=b, rid=rid,
    )


def request_friend(session: Session, uid: str, to_id: str) -> Dict[str, Any]:
    if uid == to_id:
        raise ValueError("Cannot friend yourself.")

    # Block/duplicate checks
    blocked = session.run(
        """
        MATCH (a:User {id:$a}),(b:User {id:$b})
        WHERE EXISTS { MATCH (a)-[:BLOCKED]->(b) }
           OR EXISTS { MATCH (b)-[:BLOCKED]->(a) }
        RETURN 1 AS x
        """,
        a=uid, b=to_id,
    ).single()
    if blocked:
        raise ValueError("Friend request blocked by user settings.")

    already = session.run(
        """
        MATCH (a:User {id:$a})-[:FRIENDS_WITH]-(b:User {id:$b})
        RETURN 1 AS x
        """,
        a=uid, b=to_id,
    ).single()
    if already:
        return {"ok": True, "already_friends": True}

    pending = session.run(
        """
        MATCH (a:User {id:$a})-[:REQUESTED]->(fr:FriendRequest {status:'pending'})-[:TO]->(b:User {id:$b})
        RETURN 1 AS x
        """,
        a=uid, b=to_id,
    ).single()
    if pending:
        return {"ok": True, "already_requested": True}

    rid = str(uuid4())
    session.execute_write(_tx_create_request, uid, to_id, rid)
    return {"ok": True, "request_id": rid, "created_at": _now_iso()}


def _tx_accept_request(tx: Transaction, uid: str, rid: str) -> Optional[Dict[str, Any]]:
    rec = tx.run(
        """
        MATCH (me:User {id:$uid})<-[:TO]-(fr:FriendRequest {id:$rid, status:'pending'})-[:REQUESTED]-(from:User)
        SET fr.status = 'accepted', fr.updated_at = datetime()
        WITH me, from
        MERGE (from)-[r1:FRIENDS_WITH]->(me)
          ON CREATE SET r1.created_at = datetime(), r1.xp_shared = 0, r1.tier = 'seedling'
        MERGE (me)-[r2:FRIENDS_WITH]->(from)
          ON CREATE SET r2.created_at = datetime(), r2.xp_shared = 0, r2.tier = 'seedling'
        RETURN from.id AS from_id, me.id AS me_id
        """,
        uid=uid, rid=rid,
    ).single()
    return dict(rec) if rec else None


def accept_friend(session: Session, uid: str, request_id: str) -> Dict[str, Any]:
    expire_stale_requests(session)
    rec = session.execute_write(_tx_accept_request, uid, request_id)
    if not rec:
        raise ValueError("Request not found, not pending, or not addressed to you.")
    return {"ok": True, "friend_id": rec["from_id"]}


# --------------------------------------------------------------------------
# Leaderboard / activity / reputation
# --------------------------------------------------------------------------
def get_leaderboard(session: Session) -> List[Dict[str, Any]]:
    rows = session.run(
        """
        MATCH (u:User)
        RETURN u.id AS id,
               u.display_name AS display_name,
               toInteger(coalesce(u.eco_reputation,0)) AS eco_score
        ORDER BY eco_score DESC, display_name ASC
        LIMIT $limit
        """,
        limit=LEADERBOARD_LIMIT,
    )
    return [dict(r) for r in rows]


def list_friend_activities(session: Session, uid: str) -> List[Dict[str, Any]]:
    rows = session.run(
        """
        MATCH (me:User {id:$uid})
        OPTIONAL MATCH (me)-[:FRIENDS_WITH]->(f:User)
        WITH me, collect(f) AS friends
        UNWIND friends AS friend
        MATCH (friend)-[:PERFORMED]->(a:Activity)
        RETURN friend.id AS user_id,
               friend.display_name AS display_name,
               a.type AS type,
               a.points AS points,
               a.created_at AS created_at
        ORDER BY created_at DESC
        LIMIT $limit
        """,
        uid=uid,
        limit=ACTIVITY_LIMIT,
    )
    return [dict(r) for r in rows]


def compute_reputation(session: Session, uid: str) -> Dict[str, Any]:
    rec = session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (u:User {id:$uid})
            OPTIONAL MATCH (u)-[:PERFORMED]->(a:Activity)
            WITH u, coalesce(sum(a.points),0) AS pts
            OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(f:User)
            WITH u, pts, count(f) AS fc
            WITH u, toInteger(pts + 5*fc) AS new_rep
            SET u.eco_reputation = new_rep
            RETURN new_rep
            """,
            uid=uid,
        ).single()
    )
    return {"ok": True, "eco_reputation": int(rec["new_rep"]) if rec else 0}


# --------------------------------------------------------------------------
# NEW lifecycle (decline/cancel/remove), blocking, discovery, notes, stats, XP
# --------------------------------------------------------------------------
def decline_friend(session: Session, uid: str, request_id: str) -> Dict[str, Any]:
    expire_stale_requests(session)
    rec = session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (:User {id:$uid})<-[:TO]-(fr:FriendRequest {id:$rid, status:'pending'})
            SET fr.status = 'declined', fr.updated_at = datetime()
            RETURN {ok:true} AS ok
            """,
            uid=uid, rid=request_id,
        ).single()
    )
    if not rec:
        raise ValueError("Request not found or already handled.")
    return rec["ok"]


def cancel_request(session: Session, uid: str, request_id: str) -> Dict[str, Any]:
    expire_stale_requests(session)
    rec = session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (:User {id:$uid})-[:REQUESTED]->(fr:FriendRequest {id:$rid, status:'pending'})-[:TO]->(:User)
            DETACH DELETE fr
            RETURN {ok:true} AS ok
            """,
            uid=uid, rid=request_id,
        ).single()
    )
    if not rec:
        raise ValueError("Request not found or not owned by you.")
    return rec["ok"]


def remove_friend(session: Session, uid: str, friend_id: str) -> Dict[str, Any]:
    def _tx_remove(tx: Transaction, a: str, b: str):
        tx.run(
            """
            MATCH (a:User {id:$a})-[r:FRIENDS_WITH]->(b:User {id:$b})
            DELETE r
            """,
            a=a, b=b,
        )
        tx.run(
            """
            MATCH (b:User {id:$b})-[r:FRIENDS_WITH]->(a:User {id:$a})
            DELETE r
            """,
            a=a, b=b,
        )
    session.execute_write(_tx_remove, uid, friend_id)
    return {"ok": True}


def block_user(session: Session, uid: str, target_id: str) -> Dict[str, Any]:
    def _tx_block(tx: Transaction, a: str, b: str) -> Dict[str, Any]:
        removed = tx.run(
            """
            MATCH (a:User {id:$a})-[r:FRIENDS_WITH]-(b:User {id:$b})
            WITH r
            DELETE r
            RETURN count(*) AS c
            """,
            a=a, b=b,
        ).single()["c"]

        removed_req = tx.run(
            """
            MATCH (a:User {id:$a})-[:REQUESTED]->(fr:FriendRequest)-[:TO]->(b:User {id:$b})
            DETACH DELETE fr
            WITH count(*) AS c1
            MATCH (b:User {id:$b})-[:REQUESTED]->(fr2:FriendRequest)-[:TO]->(a:User {id:$a})
            DETACH DELETE fr2
            RETURN c1 + count(*) AS total
            """,
            a=a, b=b,
        ).single()["total"]

        tx.run(
            """
            MATCH (a:User {id:$a}), (b:User {id:$b})
            MERGE (a)-[:BLOCKED]->(b)
            """,
            a=a, b=b,
        )
        return {"ok": True, "removed_friendship": bool(removed), "removed_requests": removed_req}

    return session.execute_write(_tx_block, uid, target_id)


def unblock_user(session: Session, uid: str, target_id: str) -> Dict[str, Any]:
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (:User {id:$a})-[r:BLOCKED]->(:User {id:$b})
            DELETE r
            """,
            a=uid, b=target_id,
        )
    )
    return {"ok": True}


def list_suggestions(session: Session, uid: str, limit: int = 20) -> List[Dict[str, Any]]:
    rows = session.run(
        """
        MATCH (me:User {id:$uid})
        MATCH (me)-[:FRIENDS_WITH]->(f1:User)-[:FRIENDS_WITH]->(cand:User)
        WHERE cand.id <> $uid
          AND NOT (me)-[:FRIENDS_WITH]->(cand)
          AND NOT EXISTS { MATCH (me)-[:BLOCKED]->(cand) }
          AND NOT EXISTS { MATCH (cand)-[:BLOCKED]->(me) }
        WITH cand, count(DISTINCT f1) AS mutuals
        RETURN cand.id AS id,
               cand.display_name AS display_name,
               toInteger(coalesce(cand.eco_reputation,0)) AS eco_score,
               mutuals
        ORDER BY mutuals DESC, eco_score DESC
        LIMIT $limit
        """,
        uid=uid, limit=limit,
    )
    return [dict(r) for r in rows]


def get_mutuals(session: Session, uid: str, other_id: str) -> Dict[str, Any]:
    rows = session.run(
        """
        MATCH (me:User {id:$uid})-[:FRIENDS_WITH]->(m:User)<-[:FRIENDS_WITH]-(:User {id:$other})
        RETURN m.id AS id
        """,
        uid=uid, other=other_id,
    ).data()
    return {
        "user_id": uid,
        "other_id": other_id,
        "mutual_count": len(rows),
        "mutual_ids": [r["id"] for r in rows],
    }


def set_friend_note(session: Session, uid: str, friend_id: str, note: str) -> Dict[str, Any]:
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (:User {id:$a})-[r:FRIENDS_WITH]->(:User {id:$b})
            SET r.note = $note, r.note_updated_at = datetime()
            """,
            a=uid, b=friend_id, note=note,
        )
    )
    return {"friend_id": friend_id, "note": note}


def get_friend_note(session: Session, uid: str, friend_id: str) -> Optional[Dict[str, Any]]:
    rec = session.run(
        """
        MATCH (:User {id:$a})-[r:FRIENDS_WITH]->(:User {id:$b})
        RETURN r.note AS note
        """,
        a=uid, b=friend_id,
    ).single()
    if rec and rec["note"] is not None:
        return {"friend_id": friend_id, "note": rec["note"]}
    return None


def get_friend_stats(session: Session, uid: str) -> Dict[str, Any]:
    row = session.run(
        """
        MATCH (u:User {id:$uid})
        OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(f:User)
        WITH u, count(f) AS friends_count
        OPTIONAL MATCH (u)-[:PERFORMED]->(sa:Activity)<-[:PERFORMED]-(f2:User)<-[:FRIENDS_WITH]-(u)
        WITH u, friends_count, count(DISTINCT sa) AS mutual_eco_actions
        OPTIONAL MATCH (u)-[:PART_OF]->(:Team)<-[:PART_OF]-(f3:User)<-[:FRIENDS_WITH]-(u)
        WITH u, friends_count, mutual_eco_actions, count(DISTINCT f3) AS team_challenges_completed
        RETURN friends_count,
               mutual_eco_actions,
               team_challenges_completed,
               toInteger(coalesce(u.weekly_bonds_strengthened,0)) AS weekly_bonds_strengthened,
               toInteger(coalesce(u.eco_reputation,0)) AS eco_reputation
        """,
        uid=uid,
    ).single()
    return dict(row) if row else {
        "friends_count": 0,
        "mutual_eco_actions": 0,
        "team_challenges_completed": 0,
        "weekly_bonds_strengthened": 0,
        "eco_reputation": 0,
    }


def increase_friend_xp(session: Session, uid: str, friend_id: str, amount: int):
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (a:User {id:$a})-[r:FRIENDS_WITH]->(b:User {id:$b})
            SET r.xp_shared = coalesce(r.xp_shared, 0) + $amt
            WITH r,
              CASE
                WHEN r.xp_shared > 5000 THEN 'elder'
                WHEN r.xp_shared > 1500 THEN 'canopy'
                WHEN r.xp_shared > 500  THEN 'sapling'
                ELSE coalesce(r.tier, 'seedling')
              END AS new_tier
            SET r.tier = new_tier
            """,
            a=uid, b=friend_id, amt=amount,
        )
    )


def bump_friend_xp(session: Session, uid: str, friend_id: str, amount: int) -> Dict[str, Any]:
    increase_friend_xp(session, uid, friend_id, amount)
    return {"ok": True}


def get_tier_thresholds() -> Dict[str, Any]:
    return {"thresholds": TIER_THRESHOLDS}
