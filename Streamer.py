import asyncio
import os
from array import array
from DataTypes import DBRequest, RequestType
from mastodon import StreamListener
from mastodon import Mastodon
from mastodon.errors import MastodonWarning
from mastodon.return_types import Status


class Listener(StreamListener):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        ende_q: asyncio.Queue,
        all_q: asyncio.Queue,
    ) -> None:
        self.loop = loop
        self.en_de_q = ende_q
        self.all_q = all_q
        self.local_q = []
        self.local_q_max = 1

    def on_update(self, status: Status):
        if status.language == "en" or status.language == "de":
            print(status.id)
            self.local_q.append(status)

        if len(self.local_q) >= self.local_q_max:
            self.loop.call_soon_threadsafe(
                self.en_de_q.put_nowait,
                DBRequest(RequestType.LISTENER_WRITE, self.local_q),
            )
            self.local_q = []

        # don't care about overwhelming
        self.loop.call_soon_threadsafe(
            self.all_q.put_nowait,
            DBRequest(RequestType.LISTENER_WRITE, status),
        )


class Streamer:
    def __init__(self, hashtags: array[str]):
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
        self.hashtags = hashtags

        try:
            print(f"Streaming api healthy:{self.app.stream_healthy()}")
        except MastodonWarning as e:
            print(f"Streaming health error:\n\t{e}")
            return False

        print(f"Instance api health:{self.app.instance_health()}")

        print("Mastodon App was initialized")

    async def run(
        self,
        en_de_q: asyncio.Queue,
        all_q: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ):
        await asyncio.gather(
            *(self._stream_hashtag(tag, en_de_q, all_q, loop) for tag in self.hashtags)
        )

    async def _stream_hashtag(
        self,
        hashtag: str,
        en_de_q: asyncio.Queue,
        all_q: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ):
        listener = Listener(loop, en_de_q, all_q)

        while True:
            try:
                await asyncio.to_thread(
                    self.app.stream_hashtag,
                    hashtag,
                    listener,
                )
            except Exception as error:
                print(
                    f"Stream disconnected ({hashtag}): {error}; retrying in 5 seconds"
                )
                await asyncio.sleep(5)
