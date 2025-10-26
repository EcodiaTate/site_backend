# site_backend/social/schema.py

from __future__ import annotations
from typing import List, Literal, Optional, Dict
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict, model_validator

# -----------------------------
# Core Types
# -----------------------------
FriendTier = Literal["seedling", "sapling", "canopy", "elder"]
RequestStatus = Literal["pending", "accepted", "declined", "expired"]

# -----------------------------
# Friend / Friend Models
# -----------------------------
class Friend(BaseModel):
    id: str
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    eco_score: int = 0
    friendship_tier: FriendTier = "seedling"
    xp_shared: int = 0
    joined_at: Optional[datetime] = None

class FriendRequestIncoming(BaseModel):
    id: str
    from_id: str
    from_name: Optional[str] = None
    kind: Literal["incoming"] = "incoming"
    at: str
    mutuals: int = 0

class FriendRequestOutgoing(BaseModel):
    id: str
    to_id: str
    to_name: Optional[str] = None
    kind: Literal["outgoing"] = "outgoing"
    at: str

class FriendRequests(BaseModel):
    incoming: List[FriendRequestIncoming]
    outgoing: List[FriendRequestOutgoing]

# -----------------------------
# Gamification / Reputation
# -----------------------------
class FriendStats(BaseModel):
    friends_count: int
    mutual_eco_actions: int
    team_challenges_completed: int
    weekly_bonds_strengthened: int
    eco_reputation: int

class LeaderboardEntry(BaseModel):
    user_id: str
    name: Optional[str]
    eco_reputation: int
    friends: int
    xp: int

class LeaderboardOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    top_friends: List[LeaderboardEntry] = Field(alias="top_allies")
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="before")
    @classmethod
    def normalize_keys(cls, values):
        if isinstance(values, dict):
            if "top_friends" in values:
                return values
            if "top_allies" in values:
                values = dict(values)
                values["top_friends"] = values.pop("top_allies")
        return values

# -----------------------------
# Friend Activity Feed
# -----------------------------
class FriendActivity(BaseModel):
    id: str
    user_id: str
    type: Literal["completed_sidequest", "earned_badge", "joined_team"]
    title: str
    eco_change: int
    at: datetime

# -----------------------------
# NEW: Rich friend features
# -----------------------------
class Suggestion(BaseModel):
    id: str
    display_name: Optional[str] = None
    mutuals: int = 0
    eco_score: int = 0

class MutualsOut(BaseModel):
    user_id: str
    other_id: str
    mutual_count: int
    mutual_ids: List[str] = []

class BlockResult(BaseModel):
    ok: bool
    removed_friendship: bool = False
    removed_requests: int = 0

class FriendNoteIn(BaseModel):
    friend_id: str
    note: str = Field(max_length=500)

class FriendNoteOut(BaseModel):
    friend_id: str
    note: str

class TierThresholds(BaseModel):
    thresholds: Dict[FriendTier, int]  # XP required per tier
