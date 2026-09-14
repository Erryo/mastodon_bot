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
    reacted_to BOOLEAN NOT NULL
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

INSERT_READ_QUERY = """INSERT  OR IGNORE INTO readPost(id,author,author_bot,content,post_date,reacted_to)
VALUES(?,?,?,?,?,?)"""

tmp_add = """ALTER TABLE readPost
ADD COLUMN reacted_to BOOLEAN DEFAULT 0;"""


class RequestType(Enum):
    listener_write = 0
    request_post = 1
    db_resp = 2


class DB_Req:
    def __init__(self, req_type: RequestType, content) -> None:
        self.req_type = req_type
        self.content = content


class Listener(StreamListener):
    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue) -> None:
        self.loop = loop
        self.queue = queue

    def on_update(self, status: Status):
        print(f"UPDATE: Acc:{status.account.acct}: {status.content} ")
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait, DB_Req(RequestType(0), status)
        )

    def on_announcement(self, annoucement: Announcement):
        print(f"ANNOUNCEMENT: {annoucement.content}")

    def on_delete(self, status_id: IdType):
        print(f"DELETE: {status_id}")

    def on_notification(self, notification: Notification):
        print(f"NOTIFICATION: {notification.account} {notification.event}")

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

    async def run(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        listener = Listener(loop, queue)

        while True:
            try:
                await asyncio.to_thread(
                    self.app.stream_hashtag,
                    "politics",
                    listener,
                )
            except Exception as error:
                print(f"Stream disconnected: {error}; retrying in 5 seconds")
                await asyncio.sleep(5)


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

    async def run(self, queue: asyncio.Queue):
        pass


class DataBase:
    def __init__(self) -> None:
        try:
            DB_PATH = os.environ["DBPATH"]
        except KeyError as e:
            print(f"Environment variable not set:{e}")
            exit(-1)

        try:
            self.db = sqlite3.connect(DB_PATH)
            cursor = self.db.cursor()

            for query in SQL_QUERIES:
                cursor.execute(query)

            self.db.commit()
            print(f"SQLite DB was initialized with ver.{sqlite3.sqlite_version}")

            cursor.execute("SELECT MAX(id) FROM readPost")
            self.last_post_id = cursor.fetchone()[0]
            print("Last read Post:", self.last_post_id)

        except sqlite3.OperationalError as e:
            print(f"failed to create tables:{e}")

    def insert_read_post(self, post):
        cursor = self.db.cursor()
        data = (
            post.id,
            post.account.acct,
            post.account.bot,
            post.content,
            post.created_at.isoformat(),
            False,
        )
        cursor.execute(INSERT_READ_QUERY, data)
        self.db.commit()

        return post.id

    async def run(self, queue: asyncio.Queue):
        while True:
            request = await queue.get()

            if request.content is Status:
                status = request.content
            else:
                continue

            try:
                # Decide what to post from the received status.
                message = f"Received a post from @{status.account.acct}"

                print(f"Posted response for status {status.id}")
            except Exception as error:
                print(f"Could not post for status {status.id}: {error}")
            finally:
                queue.task_done()

    def __del__(self):
        self.db.close()


class Bot:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.db = DataBase()
        self.poster = Poster()
        self.streamer = Streamer()

        print("Bot was fully initialized\n")

    async def run(self):
        loop = asyncio.get_running_loop()

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.streamer.run(self.queue, loop))
            tasks.create_task(self.poster.run(self.queue))


async def main():
    bot = Bot()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
