import warnings
import time
import sqlite3
import os
from mastodon import StreamListener
from mastodon import Mastodon, MastodonError
from dotenv import load_dotenv, dotenv_values
import mastodon
from mastodon.errors import MastodonWarning
from mastodon.types_base import T, IdType
from mastodon.return_types import Announcement, Notification, Status

from enum import Enum
import asyncio
from dataclasses import dataclass
from typing import Any

# treat warnings as errors

warnings.filterwarnings("error")


timeline_names = ["public", "local", "home", "hashtag", "list", "link"]

# load .env file
load_dotenv()

SQL_QUERIES = [
    """CREATE TABLE IF NOT EXISTS readPost (
    id INTEGER PRIMARY KEY,
    author text NOT NULL,
    author_bot BOOLEAN NOT NULL,
    content text NOT NULL,
    post_date TEXT NOT NULL,
    status TEXT NOT NULL,
    language TEXT NOT NULL
);""",
    """CREATE TABLE IF NOT EXISTS ourPost (
    id INTEGER PRIMARY KEY,
    author text NOT NULL,
    content text NOT NULL,
    response_to_id  INTEGER,
    post_date TEXT NOT NULL,
    FOREIGN KEY (response_to_id)
    REFERENCES readPost(id)
    );""",
]

INSERT_READ_QUERY = """INSERT OR IGNORE INTO readPost(id,author,author_bot,content,post_date,status,language)
VALUES(?,?,?,?,?,?,?)"""


class RequestType(Enum):
    LISTENER_WRITE = 0
    REQUEST_POST = 1
    POST_PUBLISHED = 2


@dataclass
class DBRequest:
    """A command for the single task that owns the SQLite connection."""

    request_type: RequestType
    content: Any = None
    response_queue: asyncio.Queue | None = None


class Post:
    id: int
    author: str
    content: str
    post_date: str
    author_bot: bool
    language: str
    status: str

    def __init__(
        self, id, author, author_bot, content, post_date, language, status
    ) -> None:
        self.id = id
        self.author = author
        self.author_bot = author_bot
        self.content = content
        self.post_date = post_date
        self.language = language
        self.status = status

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
        )


class Listener(StreamListener):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        ende_q: asyncio.Queue,
        all_q: asyncio.Queue,
    ) -> None:
        self.loop = loop
        self.en_de_q = ende_q
        self.all_q = all_q
        self.local_q = []
        self.local_q_max = 12

    def on_update(self, status: Status):
        print(
            f"{status.language}: {len(self.local_q)}/{self.local_q_max}:{
                self.en_de_q.qsize()
            } "
        )
        if status.language == "en" or status.language == "de":
            self.local_q.append(status)

        if len(self.local_q) > self.local_q_max:
            print("flushing queue")
            self.loop.call_soon_threadsafe(
                self.en_de_q.put_nowait,
                DBRequest(RequestType.LISTENER_WRITE, self.local_q),
            )
            self.local_q = []

        # don't care about overwhelming
        self.loop.call_soon_threadsafe(
            self.all_q.put_nowait,
            DBRequest(RequestType.LISTENER_WRITE, status),
        )

    def handle_heartbeat(self):
        print("ping")


class Streamer:
    def __init__(self):
        try:
            self.client_id = os.environ["STREAMID"]
            self.client_secret = os.environ["STREAMSECRET"]
            self.client_token = os.environ["STREAMTOKEN"]
            self.local_url = os.environ["STREAMURL"]
        except KeyError as e:
            print(f"Environment variables not set:{e}")
            exit(-1)

        mastodon = Mastodon(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.client_token,
            api_base_url=self.local_url,
            ratelimit_method="wait",
        )
        self.app = mastodon
        self.last_post_id = 0

        try:
            print(f"Streaming api healthy:{self.app.stream_healthy()}")
        except MastodonWarning as e:
            print(f"Streaming health error:\n\t{e}")
            return False

        print(f"Instance api health:{self.app.instance_health()}")

        print("Mastodon App was initialized")

    async def run(
        self,
        en_de_q: asyncio.Queue,
        all_q: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ):
        listener = Listener(loop, en_de_q, all_q)

        try:
            await asyncio.to_thread(
                self.app.stream_public,
                listener,
            )
        except Exception as error:
            print(f"Stream disconnected: {error}; retrying in 5 seconds")
            await asyncio.sleep(5)
        while True:
            time.sleep(1)


class Poster:
    def __init__(self):
        try:
            self.client_id = os.environ["MASTODONID"]
            self.client_secret = os.environ["MASTODONSECRET"]
            self.client_token = os.environ["MASTODONTOKEN"]
            self.local_url = os.environ["MASTODONURL"]
        except KeyError as e:
            print(f"Environment variables not set:{e}")
            exit(-1)

        mastodon = Mastodon(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.client_token,
            api_base_url="https://mstdn.social",
            ratelimit_method="wait",
        )
        self.app = mastodon

        print("Mastodon App was initialized")

    async def run(
        self,
        database_queue: asyncio.Queue[DBRequest],
        response_queue: asyncio.Queue,
    ):
        """Ask the database for work, then record successfully sent replies."""
        while True:
            await database_queue.put(
                DBRequest(RequestType.REQUEST_POST, response_queue=response_queue)
            )
            status = await response_queue.get()
            print("Processing:", status.id)
            try:
                if status is None:
                    # Do not spin while the stream has not delivered a post yet.
                    await asyncio.sleep(15)
                    continue

            except Exception as error:
                pass
            finally:
                print("error,done")
                response_queue.task_done()


class DataBase:
    def __init__(self, db_path: str, en_de_filter: bool) -> None:

        try:
            self.db = sqlite3.connect(db_path)
            self.db_path = db_path
            self.en_de_filter = en_de_filter

            self.db.row_factory = sqlite3.Row
            cursor = self.db.cursor()

            for query in SQL_QUERIES:
                cursor.execute(query)

            self.db.commit()
            print(
                f"SQLite DB {db_path} was initialized with filter {
                    en_de_filter
                } with ver.{sqlite3.sqlite_version}"
            )

            cursor.execute("SELECT MAX(id) FROM readPost")
            self.last_post_id = cursor.fetchone()[0]
            print("Last read Post:", self.last_post_id)

        except sqlite3.OperationalError as e:
            print(f"failed to create tables:{e}")

    def insert_read_post_batch(self, posts):
        print("inserting batch", self.db_path)
        if not isinstance(posts, list):
            posts = [posts]
        cursor = self.db.cursor()
        batch_data = [
            (
                post.id,
                post.account.acct,
                post.account.bot,
                post.content,
                post.created_at.isoformat(),
                "unreviewed",
                post.language,
            )
            for post in posts
        ]

        # 2. Execute the batch query
        cursor.executemany(INSERT_READ_QUERY, batch_data)
        self.db.commit()

    def next_unreacted_post(self) -> Post | None:
        cursor = self.db.cursor()
        cursor.execute(""" SELECT id, author, author_bot, content, post_date,language,status FROM readPost
        WHERE status = 'unreviewed' ORDER BY post_date, id LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            return None
        cursor.execute(
            "UPDATE readPost SET status = 'pending' WHERE id = ?", (row["id"],)
        )
        self.db.commit()
        return Post.from_row(row)

    def record_published_post(self, source_post: Post, posted_post) -> None:
        cursor = self.db.cursor()
        cursor.execute(
            """INSERT OR IGNORE INTO ourPost(id, author, content, response_to_id, post_date)
               VALUES (?, ?, ?, ?, ?)""",
            (
                posted_post.id,
                posted_post.account.acct,
                posted_post.content,
                source_post.id,
                posted_post.created_at.isoformat(),
            ),
        )
        cursor.execute(
            "UPDATE readPost SET status = 'posted' WHERE id = ?", (source_post.id)
        )
        self.db.commit()

    async def run(self, queue: asyncio.Queue[DBRequest]):
        """Process all SQLite reads/writes and answer poster requests."""
        while True:
            request = await queue.get()
            print(request.request_type)
            try:
                if request.request_type is RequestType.LISTENER_WRITE:
                    self.insert_read_post_batch(request.content)
                elif request.request_type is RequestType.REQUEST_POST:
                    status = self.next_unreacted_post()
                    if request.response_queue is not None:
                        await request.response_queue.put(status)
                elif request.request_type is RequestType.POST_PUBLISHED:
                    self.record_published_post(*request.content)
            except Exception as error:
                print(f"Database request failed: {error}")
            finally:
                queue.task_done()

    def __del__(self):
        if hasattr(self, "db"):
            self.db.close()


class Bot:
    def __init__(self):

        try:
            en_de_db = os.environ["EnDeDB"]
            all_db = os.environ["AllDB"]
        except KeyError as e:
            print(f"Environment variable not set:{e}")

        self.en_de_db_queue: asyncio.Queue[DBRequest] = asyncio.Queue()
        self.all_db_queue: asyncio.Queue[DBRequest] = asyncio.Queue()
        self.poster_response_queue: asyncio.Queue = asyncio.Queue()
        self.db_en_de = DataBase(en_de_db, True)
        self.db_all = DataBase(all_db, False)
        self.poster = Poster()
        self.streamer = Streamer()

        print("Bot was fully initialized\n")

    async def run(self):
        loop = asyncio.get_running_loop()

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.db_en_de.run(self.en_de_db_queue))
            tasks.create_task(self.db_all.run(self.all_db_queue))
            tasks.create_task(
                self.streamer.run(
                    en_de_q=self.en_de_db_queue, all_q=self.all_db_queue, loop=loop
                )
            )
            tasks.create_task(
                self.poster.run(self.en_de_db_queue, self.poster_response_queue)
            )


async def main():
    bot = Bot()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
