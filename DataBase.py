import sqlite3
from array import array
import asyncio
from datetime import datetime
from DataTypes import Post, DBRequest, RequestType

SQL_QUERIES = [
    """CREATE TABLE IF NOT EXISTS readPost (
    id INTEGER PRIMARY KEY,
    author text NOT NULL,
    author_bot BOOLEAN NOT NULL,
    content text NOT NULL,
    post_date TEXT NOT NULL,
    status TEXT NOT NULL,
    language TEXT NOT NULL,
    url TEXT NOT NULL
);""",
    """CREATE TABLE IF NOT EXISTS ourPost (
    id INTEGER PRIMARY KEY,
    content text NOT NULL,
    response_to_id  INTEGER,
    post_date TEXT NOT NULL,
    seconds_to_generate int,
    FOREIGN KEY (response_to_id)
    REFERENCES readPost(id)
    );""",
]

INSERT_READ_QUERY = """INSERT OR IGNORE INTO
        readPost(id,author,author_bot,content,post_date,status,language,url)
        VALUES(?,?,?,?,?,?,?,?)"""
GET_NEXT_POST = """ SELECT * FROM readPost
            WHERE status = 'unreviewed' ORDER BY post_date, id LIMIT 1
            """


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

    def insert_read_post_batch(self, posts: array[Post] | Post):
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

    def next_unreacted_post(self) -> Post | None:
        cursor = self.db.cursor()
        try:
            cursor.execute(GET_NEXT_POST)
            row = cursor.fetchone()
        except Exception as e:
            print("SELECT:", e)
        if row is None:
            return None
        try:
            cursor.execute(
                "UPDATE readPost SET status = 'pending' WHERE id = ?", (row["id"],)
            )
            self.db.commit()
        except Exception as e:
            print("UPDATE:", e)

        try:
            post = Post.from_row(row)
            return post
        except Exception as e:
            print("Post.from row:", e)
            return None

    def store_our_post(
        self, post_id: int, source_id: int, posted_post: str, seconds_to_generate: int
    ) -> None:
        cursor = self.db.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO ourPost(id,content, response_to_id, post_date,seconds_to_generate)
                VALUES (?,?, ?, ?,?)
                """,
                (
                    post_id,
                    posted_post,
                    source_id,
                    datetime.now().isoformat(),
                    seconds_to_generate,
                ),
            )
        except Exception as e:
            print("INSERT:", type(e).__name__, repr(e))
            raise

        try:
            cursor.execute(
                "UPDATE readPost SET status = 'posted' WHERE id = ?",
                (source_id,),
            )
        except Exception as e:
            print("UPDATE:", type(e).__name__, repr(e))
            raise
        self.db.commit()

    async def run(self, queue: asyncio.Queue[DBRequest]):
        """Process all SQLite reads/writes and answer poster requests."""
        while True:
            request = await queue.get()
            try:
                if request.request_type is RequestType.LISTENER_WRITE:
                    self.insert_read_post_batch(request.content)
                elif request.request_type is RequestType.REQUEST_POST:
                    status = self.next_unreacted_post()
                    if request.response_queue is not None:
                        await request.response_queue.put(status)
                elif request.request_type is RequestType.POST_PUBLISHED:
                    self.store_our_post(*request.content)
            except Exception as error:
                print(f"Database request failed: {error}")
            finally:
                queue.task_done()

    def __del__(self):
        if hasattr(self, "db"):
            self.db.close()
