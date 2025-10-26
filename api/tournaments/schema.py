from __future__ import annotations
from typing import List, Literal, Optional
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl

Mode = Literal["team", "solo"]
Metric = Literal["eco", "completions", "eco_per_member"]

Visibility = Literal["public", "private"]
Status = Literal["draft", "upcoming", "active", "ended", "archived"]

TieBreaker = Literal["highest_single_day", "most_completions", "earliest_finish"]

class Prize(BaseModel):
    place: int = Field(gt=0, description="1 for champion, 2 for runner-up, etc.")
    title: str
    description: Optional[str] = None
    badge_key: Optional[str] = None

class Rules(BaseModel):
    rules_url: Optional[HttpUrl] = None
    text_md: Optional[str] = None
    anti_cheat: List[str] = Field(default_factory=list)
    allowed_sidequest_kinds: Optional[List[str]] = None   # e.g. ["weekly","eco_action"]

class Window(BaseModel):
    start: str  # ISO datetime
    end: str    # ISO datetime

class Tournament(BaseModel):
    id: str
    name: str
    season: Optional[str] = None
    start: str
    end: str
    mode: Mode
    metric: Metric
    visibility: Visibility = "public"
    status: Status = "draft"
    division: Optional[str] = None                 # e.g., "APAC", "Under-25", "Corporate"
    max_participants: Optional[int] = Field(default=None, ge=2, le=100000)
    allow_late_join: bool = False
    min_team_size: Optional[int] = Field(default=None, ge=1, le=1000)
    max_team_size: Optional[int] = Field(default=None, ge=1, le=1000)
    tie_breaker: TieBreaker = "highest_single_day"
    rules: Optional[Rules] = None
    prizes: List[Prize] = Field(default_factory=list)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class TournamentCreate(BaseModel):
    name: str
    season: Optional[str] = None
    start: str
    end: str
    mode: Mode
    metric: Metric = "eco"
    visibility: Visibility = "public"
    status: Status = "upcoming"
    division: Optional[str] = None
    max_participants: Optional[int] = Field(default=None, ge=2, le=100000)
    allow_late_join: bool = False
    min_team_size: Optional[int] = Field(default=None, ge=1, le=1000)
    max_team_size: Optional[int] = Field(default=None, ge=1, le=1000)
    tie_breaker: TieBreaker = "highest_single_day"
    rules: Optional[Rules] = None
    prizes: List[Prize] = Field(default_factory=list)

class TournamentUpdate(BaseModel):
    name: Optional[str] = None
    season: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    metric: Optional[Metric] = None
    visibility: Optional[Visibility] = None
    status: Optional[Status] = None
    division: Optional[str] = None
    max_participants: Optional[int] = Field(default=None, ge=2, le=100000)
    allow_late_join: Optional[bool] = None
    min_team_size: Optional[int] = Field(default=None, ge=1, le=1000)
    max_team_size: Optional[int] = Field(default=None, ge=1, le=1000)
    tie_breaker: Optional[TieBreaker] = None
    rules: Optional[Rules] = None
    prizes: Optional[List[Prize]] = None

class TournamentEnrollResult(BaseModel):
    ok: bool
    scope: Optional[Literal["team", "solo"]] = None
    already_enrolled: bool = False
    entrant_id: Optional[str] = None           # team_id or user_id for convenience
    note: Optional[str] = None

class StandingRow(BaseModel):
    id: str
    name: Optional[str] = None
    eco: int = 0
    completions: int = 0
    score: float = 0.0
    rank: int
    members: Optional[int] = None              # for team modes
    last_activity_at: Optional[str] = None

class EnrollmentOut(BaseModel):
    entrants: int
    capacity: Optional[int] = None

class LeaderboardOut(BaseModel):
    tid: str
    metric: Metric
    window: Window
    rows: List[StandingRow]
