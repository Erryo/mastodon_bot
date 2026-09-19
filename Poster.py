import os
import asyncio
import time
from mastodon import Mastodon
from DataTypes import DBRequest, OurPost, RequestType

HOUR_IN_SEC = 60 * 60


class Poster:
    def __init__(self):
        try:
            self.client_id = os.environ["POSTERID"]
            self.client_secret = os.environ["POSTERSECRET"]
            self.client_token = os.environ["POSTERTOKEN"]
            self.local_url = os.environ["POSTERURL"]
        except KeyError as e:
            print(f"Environment variables not set:{e}")
            exit(-1)

        self.app = Mastodon(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.client_token,
            api_base_url=self.local_url,
            ratelimit_method="wait",
        )

        self.rate_limit_pph = 30  # Posts per hour
        self.post_count = 0
        self.time_first = None
        print("Mastodon App was initialized")

    async def post(self, post: OurPost, responsee_url: int):
        try:
            result = self.app.search_v2(q=responsee_url, resolve=True)
            replying_to = result["statuses"][0]
        except Exception as e:
            print("Error getting status of post to reply to:", e)
            await self.db_q.put(
                DBRequest(
                    RequestType.POST_FAILED, content=(post.response_to_id, post.id)
                )
            )
            return

        try:
            published = self.app.status_reply(
                to_status=replying_to, status=post.content
            )
            post.local_id = published.id
            print("posted")
            self.post_count += 1
            if self.time_first is None:
                self.time_first = time.time()

            await self.db_q.put(DBRequest(RequestType.POST_PUBLISHED, post))
        except Exception as e:
            print("Failed to reply", e)

    def check_rate(self) -> bool:
        if self.time_first is None:
            self.time_first = time.time()
            return True
        print(f"Rate:{self.post_count}/{self.rate_limit_pph}")
        if self.post_count >= self.rate_limit_pph:
            if time.time() - self.time_first >= HOUR_IN_SEC:
                self.post_count = 0
                self.time_first = time.time()
                return True
            return False

        return True

    async def run(
        self,
        database_queue: asyncio.Queue[DBRequest],
        response_queue: asyncio.Queue[OurPost],
    ):

        self.db_q = database_queue
        self.response_q = response_queue
        while True:
            while not self.check_rate():
                print("sleeping")
                wait = (self.time_first + HOUR_IN_SEC) - time.time()
                await asyncio.sleep(max(wait, 0))

            await database_queue.put(
                DBRequest(RequestType.REQUEST_OUR_POST, response_queue=response_queue)
            )
            item = await response_queue.get()
            try:
                if item is None:
                    # Do not spin while the stream has not delivered a post yet.
                    await asyncio.sleep(15)
                    continue

                ourPost, respnosee_url = item
                await self.post(ourPost, respnosee_url)
            except Exception as error:
                print(f"Poster failed to process  response: {error}")
            finally:
                response_queue.task_done()
