import sqlite3
from array import array
import asyncio
import dataclasses
import logging
from DataTypes import ReadPost, OurPost, DBRequest, RequestType

logger = logging.getLogger(__name__)


class InvalidStatus(Exception):
    pass


CREATE_TABLE_QUERYS = [
    """CREATE TABLE IF NOT EXISTS readPost (
      id INTEGER PRIMARY KEY,
      local_id int,
      author text NOT NULL,
      author_bot BOOLEAN NOT NULL,
      content text NOT NULL,
      post_date TEXT NOT NULL,
      status TEXT NOT NULL,
      language TEXT NOT NULL,
      url TEXT NOT NULL,
      ignore_reason TEXT
    );""",
    """CREATE TABLE IF NOT EXISTS ourPost (
      id INTEGER PRIMARY KEY,
      local_id INTEGER,
      status TEXT NOT NULL,
      content text NOT NULL,
      response_to_id  INTEGER NOT NULL,
      post_date TEXT NOT NULL,
      seconds_to_generate int NOT NULL,
      FOREIGN KEY (response_to_id)
      REFERENCES readPost(id)
    );""",
]

INSERT_READ_QUERY = """INSERT OR IGNORE INTO readPost
    (id,author,author_bot,content,post_date,status,language,url)
    VALUES(?,?,?,?,?,?,?,?)"""

INSERT_OUR_QUERY = """INSERT INTO ourPost
    (local_id,status,content,response_to_id,post_date,seconds_to_generate)
    VALUES(:local_id,:status,:content,:response_to_id,:post_date,:seconds_to_generate)"""


GET_NEXT_POST = """ SELECT * FROM readPost
            WHERE status = 'unreviewed' ORDER BY post_date, id LIMIT 1
            """
GET_NEXT_OUR_POST = """ SELECT * FROM ourPost
            WHERE status = 'generated' ORDER BY post_date, id LIMIT 1
            """
Valid_Statuses = ["unreviewed", "pending", "generated", "posted", "ignored", "failed"]


class DataBase:
    def __init__(self, db_path: str) -> None:

        try:
            self.db = sqlite3.connect(db_path)
            self.db_path = db_path

            self.db.row_factory = sqlite3.Row
            cursor = self.db.cursor()

            for query in CREATE_TABLE_QUERYS:
                cursor.execute(query)

            self.db.commit()
            logger.info(
                "SQLite database %s initialized (SQLite %s)",
                db_path,
                sqlite3.sqlite_version,
            )

        except sqlite3.OperationalError:
            logger.exception("Failed to initialize SQLite database %s", db_path)

    def insert_read_post_batch(self, posts: array[ReadPost] | ReadPost):
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
                post.url,
            )
            for post in posts
        ]

        # 2. Execute the batch query
        cursor.executemany(INSERT_READ_QUERY, batch_data)
        self.db.commit()

    def change_status(self, id: int, status: str, table: str) -> None:
        try:
            if status not in Valid_Statuses:
                raise InvalidStatus
            if table not in ("readPost", "ourPost"):
                raise ValueError(f"invalid table: {table}")
            cursor = self.db.cursor()
            cursor.execute(f"UPDATE {table} SET status = ? WHERE id = ?", (status, id))
            self.db.commit()
        except Exception:
            logger.exception("Failed to set %s row %s to status %s", table, id, status)

    def next_our_post(self) -> (OurPost, str) | None:
        cursor = self.db.cursor()
        try:
            cursor.execute(GET_NEXT_OUR_POST)
            row = cursor.fetchone()
        except Exception:
            logger.exception("Failed to select the next generated post")
            return None

        if row is None:
            return None

        try:
            post = OurPost.from_row(row)
            cursor.execute(
                "SELECT url FROM readPost WHERE id = ?", (post.response_to_id,)
            )
            responsee_url = cursor.fetchone()
            self.change_status(row["id"], "pending", "ourPost")
            return (post, responsee_url)
        except Exception:
            logger.exception("Failed to load the next generated post")
            return None

    def next_unreacted_post(self) -> ReadPost | None:
        cursor = self.db.cursor()
        try:
            cursor.execute(GET_NEXT_POST)
            row = cursor.fetchone()
        except Exception:
            logger.exception("Failed to select the next unreviewed post")
            return None

        if row is None:
            return None

        try:
            post = ReadPost.from_row(row)
            self.change_status(row["id"], "pending", "readPost")
            return post
        except Exception:
            logger.exception("Failed to load the next unreviewed post")
            return None

    def store_our_post(self, ourPost: OurPost) -> None:
        cursor = self.db.cursor()

        try:
            cursor.execute(INSERT_OUR_QUERY, dataclasses.asdict(ourPost))
        except Exception:
            logger.exception("Failed to insert generated post")
            raise

        try:
            self.change_status(ourPost.response_to_id, "generated", "readPost")
        except Exception:
            logger.exception("Failed to update source post after generation")
            raise
        self.db.commit()

    async def run(self, queue: asyncio.Queue[DBRequest]):
        while True:
            request = await queue.get()
            try:
                if request.request_type is RequestType.LISTENER_WRITE:
                    self.insert_read_post_batch(request.content)
                elif request.request_type is RequestType.REQUEST_POST:
                    read_post = self.next_unreacted_post()
                    if request.response_queue is not None:
                        await request.response_queue.put(read_post)
                elif request.request_type is RequestType.REQUEST_OUR_POST:
                    item = self.next_our_post()
                    if request.response_queue is not None:
                        await request.response_queue.put(item)
                elif request.request_type is RequestType.GENERATOR_WRITE:
                    post = request.content[0]
                    self.store_our_post(post)
                    cursor = self.db.cursor()
                    cursor.execute(
                        "UPDATE readPost SET local_id = ? WHERE id = ? ",
                        (request.content[1], post.response_to_id),
                    )
                    self.db.commit()
                elif request.request_type is RequestType.POST_PUBLISHED:
                    post = request.content
                    self.change_status(post.id, "posted", "ourPost")
                    self.change_status(post.response_to_id, "posted", "readPost")

                    cursor = self.db.cursor()
                    cursor.execute(
                        "UPDATE ourPost SET local_id = ? WHERE id = ? ",
                        (post.local_id, post.id),
                    )
                    self.db.commit()

                elif request.request_type is RequestType.POST_FAILED:
                    self.change_status(request.content[0], "failed", "readPost")
                    if request.content[1] is not None:
                        self.change_status(request.content[0], "failed", "readPost")
                elif request.request_type is RequestType.POST_IGNORED:
                    id_req = request.content[0].id
                    self.change_status(id_req, "ignored", "readPost")
                    reason = request.content[1]
                    cursor = self.db.cursor()
                    cursor.execute(
                        "UPDATE readPost SET ignore_reason = ? WHERE id = ? ",
                        (reason, id_req),
                    )
                    self.db.commit()

            except Exception:
                logger.exception(
                    "Database request failed: %s", request.request_type.name
                )
            finally:
                queue.task_done()

    def __del__(self):
        if hasattr(self, "db"):
            self.db.close()
