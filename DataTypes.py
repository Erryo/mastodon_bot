import sqlite3
import asyncio
from typing import Any
from dataclasses import dataclass
from enum import Enum


class RequestType(Enum):
    LISTENER_WRITE = 0
    REQUEST_POST = 1
    POST_PUBLISHED = 2
    POST_FAILED = 3


class Post:
    id: int
    author: str
    content: str
    post_date: str
    author_bot: bool
    language: str
    url: str
    status: str

    def __init__(
        self, id, author, author_bot, content, post_date, language, status, url
    ) -> None:
        self.id = id
        self.author = author
        self.author_bot = author_bot
        self.content = content
        self.post_date = post_date
        self.language = language
        self.status = status
        self.url = url

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Post":
        return cls(
            id=row["id"],
            author=row["author"],
            author_bot=row["author_bot"],
            content=row["content"],
            post_date=row["post_date"],
            status=row["status"],
            language=row["language"],
            url=row["url"],
        )


@dataclass
class DBRequest:
    """A command for the single task that owns the SQLite connection."""

    request_type: RequestType
    content: Any = None
    response_queue: asyncio.Queue[Post] | None = None
