from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field

# ─────────────────────────────────────────────────────────────
# Shared literals
# ─────────────────────────────────────────────────────────────
Period = Literal["weekly", "monthly", "total"]
Scope = Literal["youth", "business"]

# ─────────────────────────────────────────────────────────────
# Public: badges, awards, stats
# ─────────────────────────────────────────────────────────────
class BadgeAwardOut(BaseModel):
    id: str
    at: str
    tier: Optional[int] = None
    badge_id: str
    season: Optional[str] = None

class AwardOut(BaseModel):
    id: str
    at: str
    rank: int
    period: Literal["weekly", "monthly"]
    award_type_id: str
    season: Optional[str] = None

class StatsOut(BaseModel):
    total_eco: int
    total_xp: int
    actions_total: int
    season_actions: int
    streak_days: int
    level: int
    next_level_xp: int
    xp_to_next: int
    progress_pct: int
    next_badge_hint: Optional[str] = None
    prestige_level: int
    active_multipliers: Dict[str, float]
    anomaly_flag: Optional[str] = None

class MeBadgesResponse(BaseModel):
    badges: List[BadgeAwardOut]
    awards: List[AwardOut]
    stats: StatsOut

class BusinessAwardsResponse(BaseModel):
    awards: List[AwardOut]

# ─────────────────────────────────────────────────────────────
# Public: progress preview
# ─────────────────────────────────────────────────────────────
class ProgressPreviewOut(BaseModel):
    level: int
    xp_to_next: int
    next_badge_hint: Optional[str] = None
    daily_available: bool
    weekly_available: bool
    monthly_available: bool
    recommended_title: Optional[str] = None

# ─────────────────────────────────────────────────────────────
# Public: claim & prestige & referral & streak freeze
# ─────────────────────────────────────────────────────────────
class ClaimRequest(BaseModel):
    quest_type_id: str = Field(..., min_length=1)
    amount: int = Field(1, ge=1, le=1000)
    metadata: Optional[Dict[str, Any]] = None

class ClaimAwarded(BaseModel):
    xp: int
    eco: int
    per_xp: int
    per_eco: int

class ClaimWindow(BaseModel):
    start: str
    end: str
    used: int
    limit: int

class ClaimResponse(BaseModel):
    claim_id: str
    tx_id: str
    awarded: ClaimAwarded
    badges_granted: List[str]
    stats: StatsOut
    window: ClaimWindow

class GenericOK(BaseModel):
    ok: bool = True

class PrestigeResponse(GenericOK):
    new_prestige: int

class StreakFreezeResponse(BaseModel):
    freeze_id: str

class ReferralLinkRequest(BaseModel):
    referrer_id: str
    referee_id: str

class ReferralLinkResponse(GenericOK):
    awarded: bool
    amounts: Optional[Dict[str, int]] = None

# ─────────────────────────────────────────────────────────────
# Public: leaderboards
# ─────────────────────────────────────────────────────────────
class LeaderboardRow(BaseModel):
    id: str
    eco: int
    rank: int

class LeaderboardResponse(BaseModel):
    period: Period
    scope: Scope
    start: Optional[str] = None
    end: Optional[str] = None
    rows: List[LeaderboardRow]
    page: int
    page_size: int
    me: Optional[LeaderboardRow] = None  # <— NEW


# ─────────────────────────────────────────────────────────────
# Admin: catalogs & seasons & multipliers & quests
# ─────────────────────────────────────────────────────────────
class BadgeTypeIn(BaseModel):
    id: str
    name: str
    icon: Optional[str] = None
    color: Optional[str] = None
    kind: Optional[str] = None
    rule: Optional[Dict[str, Any]] = None
    tier: Optional[int] = None
    max_tier: Optional[int] = None

class BadgeTypeOut(BadgeTypeIn):
    pass

class AwardTypeIn(BaseModel):
    id: str
    name: str
    icon: Optional[str] = None
    color: Optional[str] = None
    scope: Literal["youth", "business", "global"] = "youth"
    rank_limit: Optional[int] = None

class AwardTypeOut(AwardTypeIn):
    pass

class SeasonIn(BaseModel):
    id: str
    label: str
    start: str  # ISO datetime
    end: str    # ISO datetime
    theme: Optional[str] = None
    xp_boost: Optional[float] = 1.0

class SeasonOut(SeasonIn):
    pass

class MultiplierConfigIn(BaseModel):
    id: str
    label: str
    value: float
    max_stack: Optional[int] = None
    conditions: Optional[Dict[str, Any]] = None

class MultiplierConfigOut(MultiplierConfigIn):
    pass

class QuestTypeIn(BaseModel):
    id: str
    label: str
    cadence: Literal["once", "daily", "weekly", "monthly", "seasonal"] = "daily"
    base_xp: int = 0
    base_eco: int = 0
    limit_per_window: int = 1
    icon: Optional[str] = None
    color: Optional[str] = None
    extra_rules: Optional[Dict[str, Any]] = None

class QuestTypeOut(QuestTypeIn):
    pass
