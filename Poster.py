import os
import asyncio
import logging
import time
from mastodon import Mastodon
from DataTypes import DBRequest, OurPost, RequestType

HOUR_IN_SEC = 60 * 60
WAIT_BETWEEN_POST = 120
logger = logging.getLogger(__name__)


class Poster:
    def __init__(self):
        try:
            self.client_id = os.environ["POSTERID"]
            self.client_secret = os.environ["POSTERSECRET"]
            self.client_token = os.environ["POSTERTOKEN"]
            self.local_url = os.environ["POSTERURL"]
        except KeyError as e:
            logger.critical("Required environment variable is not set: %s", e)
            raise

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
        self.last_post = None
        logger.info("Poster initialized")

    async def post(self, post: OurPost, responsee_url: str):
        try:
            result = await asyncio.to_thread(
                self.app.search_v2, q=responsee_url, resolve=True
            )
            replying_to = result["statuses"][0]
        except Exception:
            logger.exception(
                "Failed to resolve status for reply to post %s", post.response_to_id
            )
            await self.db_q.put(
                DBRequest(
                    RequestType.POST_FAILED, content=(post.response_to_id, post.id)
                )
            )
            return

        try:
            published = await asyncio.to_thread(
                self.app.status_reply,
                to_status=replying_to,
                status=post.content,
            )
            post.local_id = published.id
            logger.info(
                "Published reply %s to post %s", published.id, post.response_to_id
            )
            self.last_post = time.time()
            self.post_count += 1

            await self.db_q.put(DBRequest(RequestType.POST_PUBLISHED, post))
        except Exception:
            logger.exception("Failed to publish reply to post %s", post.response_to_id)

    def check_rate(self) -> bool:
        if self.time_first is None:
            self.time_first = time.time()
            return True

        logger.debug("Posting rate: %s/%s", self.post_count, self.rate_limit_pph)
        if self.post_count >= self.rate_limit_pph:
            if time.time() - self.time_first >= HOUR_IN_SEC:
                self.post_count = 0
                self.time_first = time.time()
                return True
            return False

        return True

    def check_between(self) -> bool:
        if self.last_post is None:
            self.last_post = time.time()
            return True
        return time.time() - self.last_post >= WAIT_BETWEEN_POST - 2  # margin of 2

    async def run(
        self,
        database_queue: asyncio.Queue[DBRequest],
        response_queue: asyncio.Queue[OurPost],
    ):
        self.db_q = database_queue
        self.response_q = response_queue
        while True:
            while not self.check_rate():
                wait = (self.time_first + HOUR_IN_SEC) - time.time()
                await asyncio.sleep(max(wait, 0))
            while not self.check_between():
                wait = (self.last_post + WAIT_BETWEEN_POST) - time.time()
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
            except Exception:
                logger.exception("Poster failed to process a generated reply")
            finally:
                response_queue.task_done()
