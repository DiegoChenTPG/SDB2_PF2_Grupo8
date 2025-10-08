from typing import Optional, List
from pydantic import BaseModel, Field, constr

class NameBasicIn(BaseModel):
    nconst: constr(strip_whitespace=True, min_length=2, max_length=20)
    primaryName: constr(strip_whitespace=True, min_length=1, max_length=512)
    birthYear: Optional[int] = Field(default=None, ge=0, le=9999)
    deathYear: Optional[int] = Field(default=None, ge=0, le=9999)

class BatchIn(BaseModel):
    items: List[NameBasicIn]
    upsert: bool = True  # si True, hace ON CONFLICT DO UPDATE
