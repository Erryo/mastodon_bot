from mastodon import Mastodon
from mastodon import StreamListener
import os
import sqlite3
import time

from dotenv import load_dotenv, dotenv_values
from mastodon.return_types import Announcement, Notification, Status
from mastodon.types_base import T, IdType


# load .env file
load_dotenv()

SQL_QUERIES = [
    """CREATE TABLE IF NOT EXISTS readPost (
    id INTEGER PRIMARY KEY,
    author text NOT NULL,
    author_bot BOOLEAN NOT NULL,
    content text NOT NULL,
    post_date DATE NOT NULL
);""",
    """CREATE TABLE IF NOT EXISTS ourPost (
    id INTEGER PRIMARY KEY,
    author text NOT NULL,
    content text NOT NULL,
    response_to_id  INTEGER,
    post_date DATE NOT NULL,
    FOREIGN KEY (response_to_id)
    REFERENCES readPost(id)
    );""",
]

INSERT_READ_QUERY = """INSERT INTO readPost(id,author,author_bot,content,post_date)
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
        )
        self.app = mastodon
        #        print(self.app.auth_request_url())
        #        mastodon.log_in(
        #            code=input("Enter the OAuth authorization code: "),
        #            to_file="pytooter_usercred.secret",
        #        )
        self.listener = Listener()
        print(f"Streaming api healthy:{self.app.stream_healthy()}")
        print(f"Instance api health:{self.app.instance_health()}")
        print("Mastodon App was initialized")

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
            post.created_at,
        )
        cursor.execute(INSERT_READ_QUERY, data)
        self.db.commit()


toast = Bot()

toast.app.stream_public(toast.listener, run_async=True, reconnect_async=True)

time.sleep(10)
toast.app.status_post("Hello world! I am still in development. ⚙️#TestPost #Testing")
while True:
    time.sleep(1)
