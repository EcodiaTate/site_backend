from typing import Literal, Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime

ItemType = Literal["wallet","missions","rewards","map","calendar","friends","badges","inbox","settings","now"]

class HomeItem(BaseModel):
    id: str
    type: ItemType
    x: float
    y: float
    w: Optional[float] = None
    h: Optional[float] = None
    z: Optional[int] = None
    skin: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

class HomeLayout(BaseModel):
    id: str                      # user id
    bg: str
    items: List[HomeItem]
    version: int = 1
    updated_at: datetime
