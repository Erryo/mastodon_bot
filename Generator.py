import asyncio
import os
import time
from bs4 import BeautifulSoup
from mastodon import Mastodon
from AI.harness import SkipPost
from datetime import datetime
from mastodon.return_types import Status
from DataTypes import DBRequest, ReadPost, OurPost, RequestType


class Generator:
    def __init__(self):
        try:
            self.client_id = os.environ["GENERATORID"]
            self.client_secret = os.environ["GENERATORSECRET"]
            self.client_token = os.environ["GENERATORTOKEN"]
            self.local_url = os.environ["GENERATORURL"]
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
        self.ai = None
        print("Generator Initted")

    def generate_text(self, rPost: ReadPost) -> str:
        self.ai.reset()
        soup = BeautifulSoup(rPost.content, "html.parser")
        clean = soup.get_text()
        return self.ai.chat(f"AUTHOR:{rPost.author} MESSAGE:{clean}")

    def get_status_from_url(self, url: str) -> Status | None:
        try:
            search_result = self.app.search(q=url, resolve=True)
            if search_result["statuses"]:
                return search_result["statuses"][0]
            else:
                return None

        except Exception as e:
            print("get_status_from_url:", e)
            raise e

    async def create_post(self, rPost: ReadPost):
        local_status = self.get_status_from_url(rPost.url)
        if local_status is None:
            print("failed to get local status")
            await self.db_q.put(
                DBRequest(RequestType.POST_FAILED, content=(rPost.id, None))
            )
            return

        try:
            print("Start gen:", datetime.now())
            start = time.time()
            gen_text = self.generate_text(rPost)
            end = time.time()
        except SkipPost as e:
            print(f"Post {rPost.id} skipped: {e.reason}")
            await self.db_q.put(
                DBRequest(RequestType.POST_IGNORED, content=(rPost, e.reason))
            )
            return

        if len(gen_text) >= 500:
            print("exceeded len")
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
        except Exception as e:
            print("Failed to reply", e)

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
            except Exception as error:
                print(f"Generator failed to process  response: {error}")
            finally:
                response_queue.task_done()
