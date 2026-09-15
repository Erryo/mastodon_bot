import os
import asyncio
import time
from mastodon import Mastodon
from mastodon.return_types import Status
from DataTypes import DBRequest, Post, RequestType
from bs4 import BeautifulSoup


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

        self.app = Mastodon(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.client_token,
            api_base_url=self.local_url,
            ratelimit_method="wait",
        )
        self.ai = None

        print("Mastodon App was initialized")

    def write_post(self, post: Post):
        self.ai.reset()
        soup = BeautifulSoup(post.content, "html.parser")
        clean = soup.get_text()
        return self.ai.chat(f"AUTHOR:{post.author} MESSAGE:{clean}")

    def get_status_from_url(self, url: str) -> Status | None:
        search_result = self.app.search(q=url, resolve=True)
        if search_result["statuses"]:
            return search_result["statuses"][0]
        else:
            return None

    async def run(
        self,
        database_queue: asyncio.Queue[DBRequest],
        response_queue: asyncio.Queue,
    ):
        """Ask the database for work, then record successfully sent replies."""
        if self.ai is None:
            return
        while True:
            await database_queue.put(
                DBRequest(RequestType.REQUEST_POST, response_queue=response_queue)
            )
            status = await response_queue.get()
            try:
                if status is None:
                    # Do not spin while the stream has not delivered a post yet.
                    await asyncio.sleep(15)
                    continue
                start = time.time()
                gen_post = self.write_post(status)
                if len(gen_post) >= 500:
                    print("exceeded len")
                end = time.time()
                local_status = self.get_status_from_url(status.url)
                if local_status is None:
                    await database_queue.put(
                        DBRequest(RequestType.POST_FAILED, content=status)
                    )
                    continue

                try:
                    post = self.app.status_reply(
                        to_status=local_status, status=gen_post
                    )
                    await database_queue.put(
                        DBRequest(
                            RequestType.POST_PUBLISHED,
                            (post.id, status.id, gen_post, int(end - start)),
                        )
                    )
                except Exception as e:
                    print("Failed to reply", e)

            except Exception as error:
                print(f"Poster failed to process database response: {error}")
            finally:
                response_queue.task_done()
