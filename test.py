from mastodon import Mastodon
import os
import sqlite3

from dotenv import load_dotenv, dotenv_values


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
for post in toast.app.timeline_hashtag("politics"):
    if "trump" in post["content"].lower():
        toast.insert_read_post(post)
        print(f"id: {post.id}|{post.content}\n")
# for tag in toast.app.trending_tags(10):
#   print(f"Name:{tag.name}")

# mastodon.toot("Tooting from Python using #mastodonpy !")
