import sqlite3
import asyncio
from typing import Any
from typing import Optional
from dataclasses import dataclass
from enum import Enum


class RequestType(Enum):
    LISTENER_WRITE = 0
    REQUEST_POST = 1
    GENERATOR_WRITE = 5
    REQUEST_OUR_POST = 6
    POST_PUBLISHED = 2
    POST_FAILED = 3
    POST_IGNORED = 4


@dataclass
class ReadPost:
    """Represents a row in the `readPost` table: a post read from the timeline."""

    id: int
    author: str
    author_bot: bool
    content: str
    post_date: str
    status: str
    language: str
    url: str
    local_id: int
    ignore_reason: Optional[str] = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ReadPost":
        return cls(
            id=row["id"],
            local_id=row["local_id"],
            author=row["author"],
            author_bot=bool(row["author_bot"]),
            content=row["content"],
            post_date=row["post_date"],
            status=row["status"],
            language=row["language"],
            url=row["url"],
            ignore_reason=row["ignore_reason"],
        )


@dataclass
class OurPost:
    """Represents a row in the `ourPost` table: a post generated and sent by the bot."""

    status: str
    content: str
    response_to_id: int
    post_date: str
    seconds_to_generate: int
    id: Optional[int] = None
    local_id: Optional[int] = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "OurPost":
        return cls(
            id=row["id"],
            local_id=row["local_id"],
            status=row["status"],
            content=row["content"],
            response_to_id=row["response_to_id"],
            post_date=row["post_date"],
            seconds_to_generate=row["seconds_to_generate"],
        )


@dataclass
class DBRequest:
    """A command for the single task that owns the SQLite connection."""

    request_type: RequestType
    content: Any = None
    response_queue: asyncio.Queue | None = None
