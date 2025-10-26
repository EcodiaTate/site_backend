# site_backend/social/router.py (router_public.py)

from fastapi import APIRouter, Depends, Query
from neo4j import Session
from datetime import datetime
from site_backend.core.neo_driver import session_dep
from site_backend.core.user_guard import current_user_id

from .schema import (
    Friend, FriendRequests, LeaderboardOut, FriendActivity, FriendStats,
    Suggestion, MutualsOut, BlockResult, FriendNoteIn, FriendNoteOut, TierThresholds
)
from .service import (
    list_friends, list_requests, search_users,
    request_friend, accept_friend,
    get_leaderboard, list_friend_activities, compute_reputation,
    # NEW:
    decline_friend, cancel_request, remove_friend,
    block_user, unblock_user, list_suggestions, get_mutuals,
    set_friend_note, get_friend_note, get_friend_stats, bump_friend_xp, get_tier_thresholds
)

router = APIRouter(prefix="/social", tags=["social"])

# ---------- existing ----------
@router.get("/friends", response_model=list[Friend])
def r_list_friends(session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return list_friends(session, uid)

@router.get("/friends/requests", response_model=FriendRequests)
def r_list_requests(session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return list_requests(session, uid)


@router.get("/friends/search", response_model=list[Friend])
def r_search_friends(
    q: str = Query(min_length=2),
    session: Session = Depends(session_dep),
    uid: str = Depends(current_user_id),
):
    return search_users(session, uid, q)


@router.post("/friends/request")
def r_request_friend(to_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return request_friend(session, uid, to_id)

@router.post("/friends/accept")
def r_accept_friend(request_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return accept_friend(session, uid, request_id)

@router.get("/leaderboard", response_model=LeaderboardOut)
def r_get_leaderboard(session: Session = Depends(session_dep)):
    # return alias-correct payload
    return {"top_friends": get_leaderboard(session), "updated_at": datetime.utcnow()}

@router.get("/activities", response_model=list[FriendActivity])
def r_friend_activity(session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return list_friend_activities(session, uid)

@router.post("/reputation")
def r_recalculate_reputation(session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return compute_reputation(session, uid)

# ---------- NEW lifecycle ----------
@router.post("/friends/decline")
def r_decline_friend(request_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return decline_friend(session, uid, request_id)

@router.post("/friends/cancel")
def r_cancel_request(request_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return cancel_request(session, uid, request_id)

@router.post("/friends/remove")
def r_remove_friend(friend_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return remove_friend(session, uid, friend_id)

# ---------- NEW blocking ----------
@router.post("/friends/block", response_model=BlockResult)
def r_block_user(target_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return block_user(session, uid, target_id)

@router.post("/friends/unblock")
def r_unblock_user(target_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return unblock_user(session, uid, target_id)

# ---------- NEW discovery ----------
@router.get("/friends/suggestions", response_model=list[Suggestion])
def r_list_suggestions(session: Session = Depends(session_dep), uid: str = Depends(current_user_id), limit: int = 20):
    return list_suggestions(session, uid, limit)

@router.get("/friends/mutuals", response_model=MutualsOut)
def r_get_mutuals(other_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return get_mutuals(session, uid, other_id)

# ---------- NEW notes ----------
@router.post("/friends/note", response_model=FriendNoteOut)
def r_set_note(body: FriendNoteIn, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return set_friend_note(session, uid, body.friend_id, body.note)

@router.get("/friends/note", response_model=FriendNoteOut | None)
def r_get_note(friend_id: str, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return get_friend_note(session, uid, friend_id)

# ---------- NEW stats / tiers / xp ----------
@router.get("/friends/stats", response_model=FriendStats)
def r_friend_stats(session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return get_friend_stats(session, uid)

@router.get("/friends/tiers", response_model=TierThresholds)
def r_tier_thresholds():
    return get_tier_thresholds()

@router.post("/friends/bump-xp")
def r_bump_friend_xp(friend_id: str, amount: int = 50, session: Session = Depends(session_dep), uid: str = Depends(current_user_id)):
    return bump_friend_xp(session, uid, friend_id, amount)
