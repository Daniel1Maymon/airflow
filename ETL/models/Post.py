from sqlmodel import SQLModel, Field
from datetime import datetime, timezone
from typing import Optional

class Post(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    mongo_id: Optional[str] = Field(default=None, unique=True)
    content: str
    rooms: Optional[float] = None
    size: Optional[float] = None
    price: Optional[float] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return f'<Post {self.id} - {self.content[:20]}...>'

    def to_dict(self):
        return self.dict()
