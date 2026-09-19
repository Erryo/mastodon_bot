import asyncio
import os
import logging
import time
from bs4 import BeautifulSoup
from mastodon import Mastodon
from AI.harness import SkipPost
from datetime import datetime
from mastodon.return_types import Status
from DataTypes import DBRequest, ReadPost, OurPost, RequestType

logger = logging.getLogger(__name__)


class Generator:
    def __init__(self):
        try:
            self.client_id = os.environ["GENERATORID"]
            self.client_secret = os.environ["GENERATORSECRET"]
            self.client_token = os.environ["GENERATORTOKEN"]
            self.local_url = os.environ["GENERATORURL"]
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
        self.ai = None
        logger.info("Generator initialized")

    async def generate_text(self, rPost: ReadPost) -> str:
        soup = BeautifulSoup(rPost.content, "html.parser")
        clean = soup.get_text()
        prompt = f"AUTHOR:{rPost.author} MESSAGE:{clean}"

        def chat() -> str:
            self.ai.reset()
            return self.ai.chat(prompt)

        return await asyncio.to_thread(chat)

    async def get_status_from_url(self, url: str) -> Status | None:
        try:
            search_result = await asyncio.to_thread(
                self.app.search, q=url, resolve=True
            )
            if search_result["statuses"]:
                return search_result["statuses"][0]
            else:
                return None

        except Exception:
            logger.exception("Failed to resolve status URL: %s", url)
            raise

    async def create_post(self, rPost: ReadPost):
        local_status = await self.get_status_from_url(rPost.url)
        if local_status is None:
            logger.warning("No local status found for post %s", rPost.id)
            await self.db_q.put(
                DBRequest(RequestType.POST_FAILED, content=(rPost.id, None))
            )
            return

        try:
            logger.info("Generating reply for post %s", rPost.id)
            start = time.time()
            gen_text = await self.generate_text(rPost)
            end = time.time()
            logger.debug("Generating Ended reply for post %s", rPost.id)
        except SkipPost as e:
            logger.info("Skipped post %s: %s", rPost.id, e.reason)
            await self.db_q.put(
                DBRequest(RequestType.POST_IGNORED, content=(rPost, e.reason))
            )
            return

        if len(gen_text) >= 500:
            logger.warning(
                "Generated reply for post %s exceeds the character limit", rPost.id
            )
            return

        try:
            await self.db_q.put(
                DBRequest(
                    RequestType.GENERATOR_WRITE,
                    (
                        OurPost(
                            status="generated",
                            content=gen_text,
                            response_to_id=rPost.id,
                            post_date=datetime.now().isoformat(),
                            seconds_to_generate=int(end - start),
                        ),
                        local_status.id,
                    ),
                )
            )

        except Exception:
            logger.exception("Failed to store generated reply for post %s", rPost.id)

    async def run(
        self,
        database_queue: asyncio.Queue[DBRequest],
        response_queue: asyncio.Queue[ReadPost],
    ):
        """Ask the database for work, then record successfully sent replies."""
        if self.ai is None:
            return

        self.db_q = database_queue
        self.response_q = response_queue
        while True:
            await database_queue.put(
                DBRequest(RequestType.REQUEST_POST, response_queue=response_queue)
            )
            rPost = await response_queue.get()
            try:
                if rPost is None:
                    # Do not spin while the stream has not delivered a post yet.
                    await asyncio.sleep(15)
                    continue

                await self.create_post(rPost)
            except Exception:
                logger.exception("Generator failed to process post")
            finally:
                response_queue.task_done()
