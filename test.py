from mastodon.errors import MastodonWarning
from mastodon.types_base import T, IdType
from mastodon.return_types import Announcement, Notification, Status

import mastodon
from dotenv import load_dotenv, dotenv_values
from mastodon import Mastodon, MastodonError
from mastodon import StreamListener
import os
import sqlite3
import time

# treat warnings as errors
import warnings

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
    post_date TEXT NOT NULL
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

INSERT_READ_QUERY = """INSERT  OR IGNORE INTO readPost(id,author,author_bot,content,post_date)
VALUES(?,?,?,?,?)"""


class Listener(StreamListener):
    def on_update(self, status: Status):
        print(f"UPDATE: Acc:{status.account.acct}: {status.content} ")

    def on_announcement(self, annoucement: Announcement):
        print(f"ANNOUNCEMENT: {annoucement.content}")

    def on_delete(self, status_id: IdType):
        print(f"DELETE: {status_id}")

    def on_notification(self, notification: Notification):
        print(f"NOTIFICATION: {notification.account} {notification.event}")

    def handle_heartbeat(self):
        print("ping")


class Bot:
    def init_mastodon(self):
        try:
            Client_ID = os.environ["MASTODONID"]
            Client_Secret = os.environ["MASTODONSECRET"]
            Client_Token = os.environ["MASTODONTOKEN"]
        except KeyError as e:
            print(f"Environment variables not set:{e}")
            exit(-1)

        mastodon = Mastodon(
            client_id=Client_ID,
            client_secret=Client_Secret,
            access_token=Client_Token,
            api_base_url="https://mastodon.social",
            ratelimit_method="wait",
        )
        self.app = mastodon
        self.listener = Listener()
        self.streaming_warning = False
        self.last_post_id = 0

        try:
            print(f"Streaming api healthy:{self.app.stream_healthy()}")
        except MastodonWarning as e:
            self.streaming_warning = True
            print(f"Streaming health error:\n\t{e}")

        print(f"Instance api health:{self.app.instance_health()}")

        self.init_timelines()

        print("Mastodon App was initialized")

    def init_timelines(self):
        self.available_timelines = {}
        self.prefered_timeline = None

        for name in timeline_names:
            try:
                self.available_timelines[name] = self.app.timeline_is_available(
                    name, fail_hard=True
                )
            except MastodonError as e:
                self.available_timelines[name] = False
                print(e)

        print("available timelines:", self.available_timelines)
        self.prefered_timeline = next(
            (name for name, available in self.available_timelines.items() if available),
            None,
        )

        if self.prefered_timeline is None:
            raise RuntimeError("No usable timeline found")

        print(f"Preferred timeline:{self.prefered_timeline}")

    def init_db(self):
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

    # Clean Up

    def __del__(self):
        self.db.close()

    def __init__(self):
        self.init_mastodon()
        self.init_db()
        print("Bot was fully initialized\n")

    def insert_read_post(self, post):
        cursor = self.db.cursor()
        data = (
            post.id,
            post.account.acct,
            post.account.bot,
            post.content,
            post.created_at.isoformat(),
        )
        cursor.execute(INSERT_READ_QUERY, data)
        self.db.commit()

        return post.id

    def read_timeline(self):
        if not toast.available_timelines[toast.prefered_timeline]:
            exit(-1)

        if toast.prefered_timeline is None:
            exit(-1)
        if toast.prefered_timeline == "hashtag":
            self.read_hashtag_timeline()

    def read_hashtag_timeline(self):
        if not self.available_timelines["hashtag"]:
            return
        while True:
            for post in toast.app.timeline_hashtag(
                "politics", since_id=self.last_post_id
            ):
                print(f"Post:{post.id} by: {post.account.acct}")
                self.last_post_id = self.insert_read_post(post)

            time.sleep(1)


toast = Bot()
toast.read_timeline()
