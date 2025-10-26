# site_backend/routers/teams/router_public.py
from __future__ import annotations
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from neo4j import Session
from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

from .schema import (
    Team, TeamCreate, TeamUpdate, TeamDetail, TeamInviteCreate, TeamInvite,
    JoinRequestCreate, JoinRequest, TeamStats, TeamFeedItem, TeamLeaderboard,
    MemberLeaderboard, InviteLinkCreate, InviteLink, Announcement, AnnouncementCreate
)
from .service import (
    create_team, update_team, regenerate_code, my_teams, team_detail,
    join_by_code, request_to_join, handle_join_request, invite_user, respond_invite,
    search_teams, lookup_by_slug, team_stats, team_feed, teams_leaderboard,
    change_role, remove_member, leave_team,
    # NEW
    create_invite_link, list_invite_links, delete_invite_link,
    members_leaderboard, create_announcement, list_announcements
)

router = APIRouter(prefix="/teams", tags=["teams"])

# ---- Create / Update / Read ----
@router.post("", response_model=Team)
def r_create_team(
    payload: TeamCreate,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return create_team(
        session, uid, payload.name, payload.slug, payload.visibility,
        avatar_url=payload.avatar_url, bio=payload.bio, max_members=payload.max_members,
        banner_url=payload.banner_url, theme_color=payload.theme_color, timezone=payload.timezone,
        lat=payload.lat, lng=payload.lng, tags=payload.tags, rules_md=payload.rules_md,
        socials=(payload.socials.model_dump() if payload.socials else None),
        allow_auto_join_public=payload.allow_auto_join_public,
        require_approval_private=payload.require_approval_private,
        join_questions=payload.join_questions
    )

@router.patch("/{team_id}", response_model=Team)
def r_update_team(
    team_id: str,
    payload: TeamUpdate,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    # Convert nested socials to dict if present
    p = payload.model_dump(exclude_none=True)
    if "socials" in p and p["socials"] is not None and hasattr(payload.socials, "model_dump"):
        p["socials"] = payload.socials.model_dump()
    return update_team(session, uid, team_id, p)

@router.post("/regenerate-code/{team_id}", response_model=Team)
def r_regenerate_code(team_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return regenerate_code(session, uid, team_id)

@router.get("/mine", response_model=List[Team])
def r_my_teams(session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return my_teams(session, uid)

@router.get("/{team_id}", response_model=TeamDetail)
def r_team_detail(team_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return team_detail(session, uid, team_id)

@router.get("/lookup/slug/{slug}", response_model=Team)
def r_lookup_slug(slug: str, session: Session = Depends(session_dep)):
    return lookup_by_slug(session, slug)


@router.get("/search", response_model=List[Team])
def r_search(q: str = Query(min_length=2), session: Session = Depends(session_dep)):
    return search_teams(session, q)


# ---- Membership flows ----
@router.post("/join")
def r_join_by_code(code: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return join_by_code(session, uid, code)

@router.post("/join/request", response_model=JoinRequest)
def r_request_join(
    payload: JoinRequestCreate,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return request_to_join(session, uid, payload.team_id, payload.message, payload.answers)

@router.post("/join/handle/{request_id}", response_model=JoinRequest)
def r_handle_join(
    request_id: str,
    approve: bool = True,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return handle_join_request(session, uid, request_id, approve)

@router.post("/{team_id}/invite", response_model=TeamInvite)
def r_invite_user(
    team_id: str,
    payload: TeamInviteCreate,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return invite_user(session, uid, team_id, payload.to_user_id)

@router.post("/invite/{invite_id}/respond", response_model=TeamInvite)
def r_respond_invite(invite_id: str, accept: bool = True, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return respond_invite(session, uid, invite_id, accept)

@router.post("/{team_id}/role/{member_id}")
def r_change_role(team_id: str, member_id: str, role: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return change_role(session, uid, team_id, member_id, role)

@router.post("/{team_id}/kick/{member_id}")
def r_remove_member(team_id: str, member_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return remove_member(session, uid, team_id, member_id)

@router.post("/{team_id}/leave")
def r_leave_team(team_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return leave_team(session, uid, team_id)

# ---- Invite links (referrals) ----
@router.post("/{team_id}/invite-links", response_model=InviteLink)
def r_create_invite_link(
    team_id: str,
    payload: InviteLinkCreate,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return create_invite_link(session, uid, team_id, payload.max_uses, payload.expires_days)

@router.get("/{team_id}/invite-links", response_model=List[InviteLink])
def r_list_invite_links(team_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return list_invite_links(session, uid, team_id)

@router.delete("/invite-links/{code}")
def r_delete_invite_link(code: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return delete_invite_link(session, uid, code)

# ---- Announcements ----
@router.post("/{team_id}/announcements", response_model=Announcement)
def r_create_announcement(
    team_id: str,
    payload: AnnouncementCreate,
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return create_announcement(session, uid, team_id, payload.title, payload.body_md)

@router.get("/{team_id}/announcements", response_model=List[Announcement])
def r_list_announcements(team_id: str, limit: int = 20, session: Session = Depends(session_dep)):
    return list_announcements(session, team_id, limit)

# ---- Stats / Feed / Leaderboard ----
@router.get("/{team_id}/stats", response_model=TeamStats)
def r_team_stats(team_id: str, session: Session = Depends(session_dep)):
    return team_stats(session, team_id)

@router.get("/{team_id}/feed", response_model=List[TeamFeedItem])
def r_team_feed(team_id: str, limit: int = 30, session: Session = Depends(session_dep)):
    return team_feed(session, team_id, limit)

@router.get("/leaderboard", response_model=TeamLeaderboard)
def r_teams_leaderboard(period: str = Query(default="monthly", pattern="^(weekly|monthly|total)$"),
                        session: Session = Depends(session_dep)):
    return teams_leaderboard(session, period=period)

# NEW: member leaderboard within a team
@router.get("/{team_id}/members/leaderboard", response_model=MemberLeaderboard)
def r_members_leaderboard(
    team_id: str,
    period: str = Query(default="monthly", pattern="^(weekly|monthly|total)$"),
    limit: int = 50,
    session: Session = Depends(session_dep)
):
    return members_leaderboard(session, team_id, period=period, limit=limit)
