# site_backend/routers/teams/service.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
from uuid import uuid4
from neo4j import Session

# NOTE ON DATA:
# - Team node now also stores: banner_url, theme_color, timezone, lat, lng, tags (list), rules_md, socials (map),
#   allow_auto_join_public (bool), require_approval_private (bool), join_questions (list)
# - MEMBER_OF rel: {role, joined_at}
# - JoinRequest node: {id, created_at, status, message, answers:list?}
# - Invite node: {id, created_at, status}
# - InviteLink node: {code, created_at, created_by, uses:int, max_uses:int?, expires_at:datetime?}  (referrals)
# - Announcement node: {id, created_at, title, body_md}
# - Submissions: read team activity via Submission.team_id as before
#
# Requires APOC (already referenced elsewhere).

def _team_shape() -> str:
    # Rebuild socials map from flattened properties: socials_*
    return """
    t{
      .*, 
      created_at: toString(t.created_at),
      tags: coalesce(t.tags, []),
      join_questions: coalesce(t.join_questions, []),
      socials: apoc.map.fromPairs(
        [k IN [x IN keys(t) WHERE x STARTS WITH 'socials_'] |
          [substring(k, 9), t[k]]
        ]
      )
    } AS t
    """

def _member_row() -> str:
    return """{id:m.id, role: r.role, joined_at: toString(r.joined_at)}"""

def _ensure_slug_unique(session: Session, slug: str) -> None:
    rec = session.run("MATCH (t:Team {slug:$slug}) RETURN t LIMIT 1", slug=slug).single()
    if rec: raise ValueError("slug_taken")

def _count_members(session: Session, tid: str) -> int:
    return session.run("MATCH (:User)-[:MEMBER_OF]->(:Team {id:$tid}) RETURN count(*) AS c", tid=tid).single()["c"]

# ------------------ core crud ------------------
def create_team(session: Session, uid: str, name: str, slug: str, visibility: str,
                avatar_url: Optional[str] = None, bio: Optional[str] = None, max_members: int = 50,
                # NEW fields (all optional)
                banner_url: Optional[str] = None, theme_color: Optional[str] = None,
                timezone: Optional[str] = None, lat: Optional[float] = None, lng: Optional[float] = None,
                tags: Optional[List[str]] = None, rules_md: Optional[str] = None,
                socials: Optional[Dict[str, Any]] = None,
                allow_auto_join_public: bool = True, require_approval_private: bool = True,
                join_questions: Optional[List[str]] = None
                ) -> Dict[str, Any]:
    _ensure_slug_unique(session, slug)
    tid = uuid4().hex
    rec = session.run("""
    WITH apoc.text.random(6,'A-Za-z0-9') AS code
    MATCH (u:User {id:$uid})
    CREATE (t:Team {
      id:$tid, name:$name, slug:$slug, created_at:datetime(),
      join_code:code, visibility:$vis, avatar_url:$ava, bio:$bio, max_members:$maxm,
      banner_url:$ban, theme_color:$theme, timezone:$tz, lat:$lat, lng:$lng,
      tags: coalesce($tags, []), rules_md:$rules,
      allow_auto_join_public: $auto_pub, require_approval_private: $req_priv,
      join_questions: coalesce($join_qs, [])
    })
    MERGE (u)-[:OWNS]->(t)
    MERGE (u)-[:MEMBER_OF {role:'owner', joined_at:datetime()}]->(t)

    // Flatten socials_* only if there are keys
    WITH t, $socials AS socials
    CALL apoc.do.when(
      socials IS NULL OR size(keys(socials)) = 0,
      'RETURN t AS t',
      '
        SET t += apoc.map.fromPairs(
          [k IN keys(socials) | ["socials_" + k, socials[k]]]
        )
        RETURN t AS t
      ',
      {t:t, socials:socials}
    ) YIELD value
    WITH value.t AS t
    RETURN """+_team_shape(),
    {
      "uid": uid, "tid": tid, "name": name, "slug": slug,
      "vis": visibility, "ava": avatar_url, "bio": bio, "maxm": int(max_members),
      "ban": banner_url, "theme": theme_color, "tz": timezone, "lat": lat, "lng": lng,
      "tags": tags, "rules": rules_md, "socials": socials,
      "auto_pub": bool(allow_auto_join_public), "req_priv": bool(require_approval_private),
      "join_qs": join_questions
    }
  ).single()

    return dict(rec["t"])
def update_team(session: Session, uid: str, tid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    # ensure admin/owner
    auth = session.run("""
      MATCH (:User {id:$uid})-[r:MEMBER_OF]->(t:Team {id:$tid})
      WHERE r.role IN ['owner','admin']
      RETURN t
    """, uid=uid, tid=tid).single()
    if not auth:
        raise PermissionError("not_allowed")

    sets = []
    params: Dict[str, Any] = {"tid": tid}

    for k in [
        "name","slug","visibility","avatar_url","bio","max_members",
        "banner_url","theme_color","timezone","lat","lng","rules_md",
        "allow_auto_join_public","require_approval_private"
    ]:
        if k in payload and payload[k] is not None:
            if k == "slug":
                _ensure_slug_unique(session, payload[k])
            params[k] = payload[k]
            sets.append(f"t.{k} = ${k}")

    if "tags" in payload and payload["tags"] is not None:
        params["tags"] = payload["tags"]
        sets.append("t.tags = coalesce($tags, [])")
    if "join_questions" in payload and payload["join_questions"] is not None:
        params["join_qs"] = payload["join_questions"]
        sets.append("t.join_questions = coalesce($join_qs, [])")

    base_set = ", ".join(sets) if sets else "t.id = t.id"

    socials_stmt = ""
    if "socials" in payload:
        params["socials"] = payload["socials"]
        socials_stmt = """
          // wipe previous socials_* keys
          WITH t
          FOREACH (k IN [x IN keys(t) WHERE x STARTS WITH 'socials_'] | SET t[k] = NULL)
          WITH t, $socials AS socials
          CALL apoc.do.when(
            socials IS NULL OR size(keys(socials)) = 0,
            'RETURN t AS t',
            '
              SET t += apoc.map.fromPairs(
                [k IN keys(socials) | ["socials_" + k, socials[k]]]
              )
              RETURN t AS t
            ',
            {t:t, socials:socials}
          ) YIELD value
          WITH value.t AS t
        """

    cypher = f"""
      MATCH (t:Team {{id:$tid}})
      SET {base_set}
      {socials_stmt}
      RETURN """ + _team_shape()

    rec = session.run(cypher, params).single()
    return dict(rec["t"])


def regenerate_code(session: Session, uid: str, tid: str) -> Dict[str, Any]:
    auth = session.run("""
      MATCH (:User {id:$uid})-[r:MEMBER_OF]->(t:Team {id:$tid})
      WHERE r.role IN ['owner','admin']
      RETURN t
    """, uid=uid, tid=tid).single()
    if not auth: raise PermissionError("not_allowed")
    rec = session.run("""
      MATCH (t:Team {id:$tid})
      SET t.join_code = apoc.text.random(6,'A-Za-z0-9')
      RETURN """+_team_shape(), {"tid": tid}).single()
    return dict(rec["t"])

def my_teams(session: Session, uid: str) -> List[Dict[str, Any]]:
    rows = session.run("""
      MATCH (u:User {id:$uid})-[r:MEMBER_OF]->(t:Team)
      RETURN t{.*, created_at: toString(t.created_at), tags:coalesce(t.tags,[]),
               socials:coalesce(t.socials,{}), join_questions:coalesce(t.join_questions,[])} AS t
      ORDER BY toLower(t.name)
    """, uid=uid).data()
    return [dict(r["t"]) for r in rows]

def team_detail(session: Session, uid: str, team_id: str) -> Dict[str, Any]:
    rec = session.run("""
      MATCH (t:Team {id:$tid})
      OPTIONAL MATCH (m:User)-[r:MEMBER_OF]->(t)
      WITH t, collect("""+_member_row()+""") AS members
      RETURN { team: """+_team_shape().replace(" AS t","")+""", members: members } AS out
    """, {"tid": team_id}).single()
    if not rec: raise ValueError("not_found")
    return rec["out"]

def lookup_by_slug(session: Session, slug: str) -> Dict[str, Any]:
    rec = session.run("MATCH (t:Team {slug:$slug}) RETURN "+_team_shape(), slug=slug).single()
    if not rec: raise ValueError("not_found")
    return dict(rec["t"])

def search_teams(session: Session, q: str, limit: int = 25) -> List[Dict[str, Any]]:
    rows = session.run("""
      MATCH (t:Team)
      WHERE toLower(t.name) CONTAINS toLower($q)
         OR toLower(t.slug) CONTAINS toLower($q)
         OR any(tag IN coalesce(t.tags,[]) WHERE toLower(tag) CONTAINS toLower($q))
      RETURN """+_team_shape()+"""
      ORDER BY toLower(t.name)
      LIMIT $lim
    """, q=q, lim=limit).data()
    return [dict(r["t"]) for r in rows]

# ------------------ membership + growth ------------------
def _invite_link_shape() -> str:
    return """{code:il.code, team_id:t.id, created_at:toString(il.created_at),
               created_by:by.id, uses:coalesce(il.uses,0),
               max_uses:il.max_uses, expires_at: CASE WHEN il.expires_at IS NULL THEN NULL ELSE toString(il.expires_at) END}"""

# site_backend/api/teams/service.py  (or your actual path)

def create_invite_link(session: Session, admin_uid: str, tid: str,
                       max_uses: Optional[int], expires_days: Optional[int]) -> Dict[str, Any]:
    # owner/admin only
    auth = session.run("""
      MATCH (:User {id:$admin_uid})-[r:MEMBER_OF]->(t:Team {id:$tid})
      WHERE r.role IN ['owner','admin'] RETURN t
    """, admin_uid=admin_uid, tid=tid).single()
    if not auth:
        raise PermissionError("not_allowed")

    rec = session.run("""
      MATCH (t:Team {id:$tid}), (by:User {id:$admin})
      WITH t, by, apoc.text.random(8,'A-Za-z0-9') AS code
      CREATE (il:InviteLink {code:code, created_at:datetime(), uses:0,
                             max_uses:$max_uses,
                             expires_at: CASE WHEN $exp_days IS NULL THEN NULL ELSE datetime() + duration({days:$exp_days}) END})
      MERGE (by)-[:CREATED]->(il)
      MERGE (il)-[:FOR_TEAM]->(t)
      RETURN {code:il.code, team_id:t.id, created_at:toString(il.created_at),
              created_by:by.id, uses:coalesce(il.uses,0),
              max_uses:il.max_uses,
              expires_at: CASE WHEN il.expires_at IS NULL THEN NULL ELSE toString(il.expires_at) END} AS link
    """, {"tid": tid, "admin": admin_uid, "max_uses": max_uses, "exp_days": expires_days}).single()
    return dict(rec["link"])


def list_invite_links(session: Session, admin_uid: str, tid: str) -> List[Dict[str,Any]]:
    auth = session.run("""
      MATCH (:User {id:$admin_uid})-[r:MEMBER_OF]->(t:Team {id:$tid})
      WHERE r.role IN ['owner','admin'] RETURN t
    """, admin_uid=admin_uid, tid=tid).single()
    if not auth:
        raise PermissionError("not_allowed")

    rows = session.run("""
      MATCH (t:Team {id:$tid})<-[:FOR_TEAM]-(il:InviteLink)<-[:CREATED]-(by:User)
      RETURN {code:il.code, team_id:t.id, created_at:toString(il.created_at),
              created_by:by.id, uses:coalesce(il.uses,0),
              max_uses:il.max_uses,
              expires_at: CASE WHEN il.expires_at IS NULL THEN NULL ELSE toString(il.expires_at) END} AS link
      ORDER BY il.created_at DESC
    """, {"tid": tid}).data()
    return [dict(r["link"]) for r in rows]


def delete_invite_link(session: Session, admin_uid: str, code: str) -> Dict[str,Any]:
    rec = session.run("""
      MATCH (:User {id:$admin})-[r:MEMBER_OF]->(t:Team)
      WHERE r.role IN ['owner','admin']
      MATCH (il:InviteLink {code:$code})-[:FOR_TEAM]->(t)
      WITH il
      DETACH DELETE il
      RETURN {ok:true} AS out
    """, {"admin": admin_uid, "code": code}).single()
    if not rec: raise PermissionError("not_allowed_or_missing")
    return dict(rec["out"])

def _try_join_via_invitelink(session: Session, uid: str, code: str) -> Optional[Dict[str, Any]]:
    rec = session.run("""
      MATCH (il:InviteLink {code:$code})-[:FOR_TEAM]->(t:Team)
      OPTIONAL MATCH (by:User)-[:CREATED]->(il)
      WITH il, t, by,
           (coalesce(il.max_uses, 999999) - coalesce(il.uses,0)) AS remaining,
           (CASE WHEN il.expires_at IS NULL THEN false ELSE datetime() > il.expires_at END) AS expired
      WHERE remaining > 0 AND expired = false
      RETURN t.id AS tid, t.name AS tname, by.id AS referrer
    """, {"code": code}).single()
    if not rec: 
        return None
    # Join & increment usage
    out = session.run("""
      MATCH (t:Team {id:$tid}), (u:User {id:$uid})
      MERGE (u)-[m:MEMBER_OF]->(t)
        ON CREATE SET m.role='member', m.joined_at=datetime()
      WITH t, u
      MATCH (il:InviteLink {code:$code})-[:FOR_TEAM]->(t)
      SET il.uses = coalesce(il.uses,0) + 1
      RETURN t.id AS id, t.name AS name
    """, {"tid": rec["tid"], "uid": uid, "code": code}).single()
    # Feed: member_joined with referrer
    session.run("""
      MATCH (t:Team {id:$tid}), (u:User {id:$uid})
      OPTIONAL MATCH (il:InviteLink {code:$code})<-[:CREATED]-(ref:User)
      WITH t,u,ref
      CREATE (f:TeamFeed {id:apoc.create.uuid(), at:datetime(), type:'member_joined',
                          title: u.display_name + ' joined the team',
                          ref_user_id: u.id, by_user_id: CASE WHEN ref IS NULL THEN NULL ELSE ref.id END})
      MERGE (f)-[:FOR_TEAM]->(t)
    """, {"tid": rec["tid"], "uid": uid, "code": code})
    return {"id": out["id"], "name": out["name"]}

def join_by_code(session: Session, uid: str, code: str) -> Dict[str, Any]:
    # First try InviteLink (referral); fallback to team.join_code
    via_link = _try_join_via_invitelink(session, uid, code)
    if via_link:
        return via_link

    rec = session.run("""
      MATCH (t:Team {join_code:$code})
      WITH t, size([( :User)-[:MEMBER_OF]->(t) | 1]) AS members
      WHERE members < coalesce(t.max_members, 50)
      WITH t
      // public auto-join or private must be by join_code holder explicitly
      MATCH (u:User {id:$uid})
      MERGE (u)-[m:MEMBER_OF]->(t)
        ON CREATE SET m.role='member', m.joined_at=datetime()
      RETURN t.id AS id, t.name AS name
    """, {"code": code, "uid": uid}).single()
    if not rec: raise ValueError("invalid_or_full")
    # Feed: member_joined
    session.run("""
      MATCH (t:Team {id:$tid}), (u:User {id:$uid})
      CREATE (f:TeamFeed {id:apoc.create.uuid(), at:datetime(), type:'member_joined',
                          title: u.display_name + ' joined the team',
                          ref_user_id: u.id})
      MERGE (f)-[:FOR_TEAM]->(t)
    """, {"tid": rec["id"], "uid": uid})
    return {"id": rec["id"], "name": rec["name"]}

def request_to_join(session: Session, uid: str, tid: str, message: Optional[str], answers: Optional[List[str]] = None) -> Dict[str, Any]:
    # private teams only
    t = session.run("MATCH (t:Team {id:$tid}) RETURN t.visibility AS vis", tid=tid).single()
    if not t: raise ValueError("not_found")
    if t["vis"] != "private": raise ValueError("use_join_code")

    rid = uuid4().hex
    rec = session.run("""
      MATCH (t:Team {id:$tid}), (u:User {id:$uid})
      CREATE (jr:JoinRequest {id:$rid, created_at:datetime(), status:'pending', message:$msg,
                              answers: coalesce($answers, [])})
      MERGE (u)-[:REQUESTED_JOIN]->(jr)
      MERGE (jr)-[:FOR_TEAM]->(t)
      RETURN jr {.*, created_at: toString(jr.created_at), team_id: t.id, from_user_id: u.id } AS jr
    """, {"tid": tid, "uid": uid, "rid": rid, "msg": message, "answers": answers}).single()
    return dict(rec["jr"])

def handle_join_request(session: Session, admin_uid: str, req_id: str, approve: bool) -> Dict[str, Any]:
    rec = session.run("""
      MATCH (jr:JoinRequest {id:$rid, status:'pending'})-[:FOR_TEAM]->(t:Team)
      MATCH (:User {id:$admin})-[r:MEMBER_OF]->(t)
      WHERE r.role IN ['owner','admin']
      WITH jr, t
      SET jr.status = CASE WHEN $approve THEN 'approved' ELSE 'rejected' END
      WITH jr, t
      CALL apoc.do.when($approve,
        'MATCH (u:User)-[:REQUESTED_JOIN]->(jr) MERGE (u)-[m:MEMBER_OF]->(t) ON CREATE SET m.role="member", m.joined_at=datetime() RETURN u AS u',
        'MATCH (u:User)-[:REQUESTED_JOIN]->(jr) RETURN u AS u',
        {jr:jr, t:t}) YIELD value
      WITH jr, t, value.u AS u
      CALL apoc.do.when($approve,
        'CREATE (f:TeamFeed {id:apoc.create.uuid(), at:datetime(), type:"member_joined", title: u.display_name + " joined the team", ref_user_id:u.id}) MERGE (f)-[:FOR_TEAM]->(t) RETURN 1 AS x',
        'RETURN 0 AS x', {u:u, t:t}) YIELD value AS _x
      RETURN jr {.*, team_id: t.id } AS jr
    """, {"rid": req_id, "admin": admin_uid, "approve": approve}).single()
    if not rec: raise PermissionError("not_allowed_or_missing")
    out = dict(rec["jr"]); out["created_at"] = str(out.get("created_at"))
    return out

def invite_user(session: Session, admin_uid: str, tid: str, to_user_id: str) -> Dict[str, Any]:
    auth = session.run("""
      MATCH (:User {id:$admin})-[r:MEMBER_OF]->(t:Team {id:$tid})
      WHERE r.role IN ['owner','admin'] RETURN t
    """, admin_uid=admin_uid, tid=tid).single()
    if not auth: raise PermissionError("not_allowed")
    iid = uuid4().hex
    rec = session.run("""
      MATCH (t:Team {id:$tid}), (to:User {id:$to}), (from:User {id:$admin})
      CREATE (inv:Invite {id:$iid, created_at:datetime(), status:'pending'})
      MERGE (from)-[:SENT]->(inv)
      MERGE (inv)-[:TO]->(to)
      MERGE (inv)-[:FOR_TEAM]->(t)
      RETURN inv {.*, created_at: toString(inv.created_at), team_id: t.id, to_user_id: to.id, from_user_id: from.id } AS inv
    """, {"tid": tid, "to": to_user_id, "admin": admin_uid, "iid": iid}).single()
    return dict(rec["inv"])

def respond_invite(session: Session, uid: str, invite_id: str, accept: bool) -> Dict[str, Any]:
    rec = session.run("""
      MATCH (:User)-[:SENT]->(inv:Invite {id:$iid, status:'pending'})-[:TO]->(to:User {id:$uid})
      MATCH (inv)-[:FOR_TEAM]->(t:Team)
      SET inv.status = CASE WHEN $accept THEN 'approved' ELSE 'rejected' END
      WITH inv, t, to
      CALL apoc.do.when($accept,
        'MERGE (to)-[m:MEMBER_OF]->(t) ON CREATE SET m.role="member", m.joined_at:datetime() RETURN to AS u',
        'RETURN to AS u', {to:to, t:t}) YIELD value
      WITH inv, t, value.u AS u
      CALL apoc.do.when($accept,
        'CREATE (f:TeamFeed {id:apoc.create.uuid(), at:datetime(), type:"member_joined", title: u.display_name + " joined the team", ref_user_id:u.id}) MERGE (f)-[:FOR_TEAM]->(t) RETURN 1 AS x',
        'RETURN 0 AS x', {u:u, t:t}) YIELD value AS _x
      RETURN inv {.*, created_at: toString(inv.created_at), team_id: t.id, to_user_id: to.id } AS inv
    """, {"iid": invite_id, "uid": uid, "accept": accept}).single()
    if not rec: raise PermissionError("not_allowed_or_missing")
    return dict(rec["inv"])

def change_role(session: Session, admin_uid: str, tid: str, member_id: str, role: str) -> Dict[str, Any]:
    if role not in ["admin","member"]: raise ValueError("invalid_role")
    rec = session.run("""
      MATCH (:User {id:$admin})-[ra:MEMBER_OF]->(t:Team {id:$tid})
      WHERE ra.role IN ['owner','admin']
      MATCH (m:User {id:$mid})-[r:MEMBER_OF]->(t)
      SET r.role = $role
      RETURN {id:m.id, role:r.role, joined_at: toString(r.joined_at)} AS member
    """, {"admin": admin_uid, "tid": tid, "mid": member_id, "role": role}).single()
    if not rec: raise PermissionError("not_allowed_or_missing")
    return dict(rec["member"])

def remove_member(session: Session, admin_uid: str, tid: str, member_id: str) -> Dict[str, Any]:
    rec = session.run("""
      MATCH (:User {id:$admin})-[ra:MEMBER_OF]->(t:Team {id:$tid})
      WHERE ra.role IN ['owner','admin']
      MATCH (m:User {id:$mid})-[r:MEMBER_OF]->(t)
      DELETE r
      RETURN {id:$mid} AS removed
    """, {"admin": admin_uid, "tid": tid, "mid": member_id}).single()
    if not rec: raise PermissionError("not_allowed_or_missing")
    return dict(rec["removed"])

def leave_team(session: Session, uid: str, tid: str) -> Dict[str, Any]:
    owners = session.run("""
      MATCH (:User)-[r:MEMBER_OF {role:'owner'}]->(t:Team {id:$tid}) RETURN count(*) AS c
    """, tid=tid).single()["c"]
    is_owner = session.run("""
      MATCH (:User {id:$uid})-[r:MEMBER_OF {role:'owner'}]->(t:Team {id:$tid}) RETURN r LIMIT 1
    """, uid=uid, tid=tid).single()
    if is_owner and owners <= 1:
        raise PermissionError("transfer_ownership_first")
    session.run("MATCH (:User {id:$uid})-[r:MEMBER_OF]->(:Team {id:$tid}) DELETE r", uid=uid, tid=tid)
    return {"ok": True}

# ------------------ announcements ------------------
def create_announcement(session: Session, admin_uid: str, tid: str, title: str, body_md: Optional[str]) -> Dict[str,Any]:
    auth = session.run("""
      MATCH (:User {id:$admin})-[r:MEMBER_OF]->(t:Team {id:$tid})
      WHERE r.role IN ['owner','admin'] RETURN t
    """, {"admin": admin_uid, "tid": tid}).single()
    if not auth: raise PermissionError("not_allowed")
    aid = uuid4().hex
    rec = session.run("""
      MATCH (t:Team {id:$tid}), (by:User {id:$admin})
      CREATE (a:Announcement {id:$aid, created_at:datetime(), title:$title, body_md:$body})
      MERGE (a)-[:FOR_TEAM]->(t)
      MERGE (by)-[:POSTED]->(a)
      WITH a,t,by
      CREATE (f:TeamFeed {id:apoc.create.uuid(), at:datetime(), type:'announcement_posted',
                          title:$title, by_user_id:by.id})
      MERGE (f)-[:FOR_TEAM]->(t)
      RETURN {id:a.id, team_id:t.id, at:toString(a.created_at), title:a.title, body_md:a.body_md, by_user_id:by.id} AS a
    """, {"tid": tid, "admin": admin_uid, "aid": aid, "title": title, "body": body_md}).single()
    return dict(rec["a"])

def list_announcements(session: Session, tid: str, limit: int = 20) -> List[Dict[str,Any]]:
    rows = session.run("""
      MATCH (a:Announcement)-[:FOR_TEAM]->(t:Team {id:$tid})
      OPTIONAL MATCH (by:User)-[:POSTED]->(a)
      RETURN {id:a.id, team_id:t.id, at:toString(a.created_at), title:a.title, body_md:a.body_md,
              by_user_id: by.id} AS a
      ORDER BY a.at DESC
      LIMIT $lim
    """, {"tid": tid, "lim": limit}).data()
    return [dict(r["a"]) for r in rows]

# ------------------ stats / feed / leaderboards ------------------
def _emit_milestones(session: Session, tid: str) -> None:
    # simple thresholds (can be extended): members {5,10,25,50}, eco {1k,5k,10k}
    members = _count_members(session, tid)
    eco = session.run("""
      MATCH (sub:Submission {state:'approved'})-[:FOR]->(:Sidequest)
      WHERE sub.team_id = $tid
      OPTIONAL MATCH (t:EcoTx)-[:PROOF]->(sub)
      RETURN toInteger(sum(coalesce(t.eco,0))) AS eco
    """, {"tid": tid}).single()["eco"] or 0
    thresholds = []
    for n in [5,10,25,50,100]:
        if members == n: thresholds.append(f"Hit {n} members!")
    for e in [1000,5000,10000,25000]:
        if eco == e: thresholds.append(f"Reached {e} Eco!")
    for title in thresholds:
        session.run("""
          MATCH (t:Team {id:$tid})
          CREATE (f:TeamFeed {id:apoc.create.uuid(), at:datetime(), type:'milestone_reached',
                              title:$title})
          MERGE (f)-[:FOR_TEAM]->(t)
        """, {"tid": tid, "title": title})

def team_stats(session: Session, tid: str) -> Dict[str, Any]:
    # members
    members = _count_members(session, tid)
    totals = session.run("""
      MATCH (sub:Submission {state:'approved'})-[:FOR]->(:Sidequest)
      WHERE sub.team_id = $tid
      OPTIONAL MATCH (t:EcoTx)-[:PROOF]->(sub)
      WITH toInteger(sum(coalesce(t.eco,0))) AS eco_total
      RETURN eco_total AS eco
    """, tid=tid).single()["eco"] or 0

    week = session.run("""
      MATCH (sub:Submission {state:'approved'})-[:FOR]->(:Sidequest)
      WHERE sub.team_id = $tid AND sub.created_at >= datetime() - duration('P7D')
      OPTIONAL MATCH (t:EcoTx)-[:PROOF]->(sub)
      RETURN toInteger(sum(coalesce(t.eco,0))) AS eco
    """, tid=tid).single()["eco"] or 0
    month = session.run("""
      MATCH (sub:Submission {state:'approved'})-[:FOR]->(:Sidequest)
      WHERE sub.team_id = $tid AND sub.created_at >= datetime() - duration('P30D')
      OPTIONAL MATCH (t:EcoTx)-[:PROOF]->(sub)
      RETURN toInteger(sum(coalesce(t.eco,0))) AS eco
    """, tid=tid).single()["eco"] or 0

    approvals = session.run("""
      MATCH (sub:Submission {state:'approved'}) WHERE sub.team_id = $tid
      RETURN count(sub) AS c
    """, tid=tid).single()["c"] or 0

    # opportunistically emit milestone feed items (no-op if not on thresholds)
    _emit_milestones(session, tid)

    return {"team_id": tid, "members_count": members, "eco_total": int(totals),
            "eco_week": int(week), "eco_month": int(month),
            "submissions_approved": int(approvals)}
def team_feed(session: Session, tid: str, limit: int = 30) -> List[Dict[str, Any]]:
    rows = session.run("""
      // Branch 1: approved submissions
      CALL {
        MATCH (sub:Submission {state:'approved'}) WHERE sub.team_id = $tid
        OPTIONAL MATCH (u:User)-[:SUBMITTED]->(sub)
        RETURN { id: sub.id,
                 at: toString(sub.created_at),
                 type:'submission_approved',
                 title: coalesce(sub.caption,'Sidequest approved'),
                 eco_delta: 0,
                 by_user_id: u.id,
                 submission_id: sub.id } AS item
        ORDER BY sub.created_at DESC
        LIMIT $lim
      }
      RETURN item
      UNION ALL
      // Branch 2: member joins
      CALL {
        MATCH (f:TeamFeed {type:'member_joined'})-[:FOR_TEAM]->(:Team {id:$tid})
        RETURN f{.*, at:toString(f.at)} AS item
        ORDER BY f.at DESC
        LIMIT $lim
      }
      RETURN item
      UNION ALL
      // Branch 3: announcements
      CALL {
        MATCH (f:TeamFeed {type:'announcement_posted'})-[:FOR_TEAM]->(:Team {id:$tid})
        RETURN f{.*, at:toString(f.at)} AS item
        ORDER BY f.at DESC
        LIMIT $lim
      }
      RETURN item
      UNION ALL
      // Branch 4: milestones
      CALL {
        MATCH (f:TeamFeed {type:'milestone_reached'})-[:FOR_TEAM]->(:Team {id:$tid})
        RETURN f{.*, at:toString(f.at)} AS item
        ORDER BY f.at DESC
        LIMIT $lim
      }
      RETURN item
      ORDER BY item.at DESC
      LIMIT $lim
    """, {"tid": tid, "lim": limit}).data()

    return [dict(r["item"]) for r in rows]

def teams_leaderboard(session: Session, period: str = "monthly", limit: int = 50) -> Dict[str, Any]:
    if period not in ["weekly","monthly","total"]: raise ValueError("bad_period")
    if period == "weekly":
        cond = "sub.created_at >= datetime() - duration('P7D')"
    elif period == "monthly":
        cond = "sub.created_at >= datetime() - duration('P30D')"
    else:
        cond = "true"

    rows = session.run(f"""
      MATCH (t:Team)
      OPTIONAL MATCH (sub:Submission {{state:'approved'}})
        WHERE sub.team_id = t.id AND ({cond})
      OPTIONAL MATCH (tx:EcoTx)-[:PROOF]->(sub)
      WITH t, toInteger(sum(coalesce(tx.eco,0))) AS eco
      RETURN t.id AS team_id, t.name AS team_name, eco
      ORDER BY eco DESC, team_name ASC
      LIMIT $lim
    """, lim=limit).data()

    out = []
    last = None; rank = 0
    for idx, r in enumerate(rows, start=1):
        e = int(r["eco"] or 0)
        if last is None or e < last: rank = idx
        last = e
        out.append({"team_id": r["team_id"], "team_name": r["team_name"], "eco": e, "rank": rank})
    return {"period": period, "rows": out}

# NEW: member leaderboard within a team
def members_leaderboard(session: Session, tid: str, period: str = "monthly", limit: int = 50) -> Dict[str,Any]:
    if period not in ["weekly","monthly","total"]: raise ValueError("bad_period")
    if period == "weekly":
        cond = "sub.created_at >= datetime() - duration('P7D')"
    elif period == "monthly":
        cond = "sub.created_at >= datetime() - duration('P30D')"
    else:
        cond = "true"
    rows = session.run(f"""
      MATCH (t:Team {{id:$tid}})
      MATCH (u:User)-[:MEMBER_OF]->(t)
      OPTIONAL MATCH (u)-[:SUBMITTED]->(sub:Submission {{state:'approved'}})
        WHERE sub.team_id = t.id AND ({cond})
      OPTIONAL MATCH (tx:EcoTx)-[:PROOF]->(sub)
      WITH u, toInteger(sum(coalesce(tx.eco,0))) AS eco
      RETURN u.id AS uid, u.display_name AS uname, eco
      ORDER BY eco DESC, uname ASC
      LIMIT $lim
    """, {"tid": tid, "lim": limit}).data()

    out = []
    last = None; rank = 0
    for idx, r in enumerate(rows, start=1):
        e = int(r["eco"] or 0)
        if last is None or e < last: rank = idx
        last = e
        out.append({"user_id": r["uid"], "user_name": r["uname"], "eco": e, "rank": rank})
    return {"team_id": tid, "period": period, "rows": out}

