import warnings
import os
import asyncio
import logging

from dotenv import load_dotenv
from DataBase import DataBase
from Streamer import Streamer
from Generator import Generator
from Poster import Poster
from AI.harness import BonsaiHarness, HarnessConfig
from AI.tools import WebSearchTool, SkipPostTool

from DataTypes import DBRequest, OurPost, ReadPost

load_dotenv()

logger = logging.getLogger(__name__)


# treat warnings as errors
MODEL_PATH = "AI/qwen2.5-7b-instruct-q5_k_m-00001-of-00002.gguf"
# MODEL_PATH = "AI/Bonsai-27B-Q1_0.gguf"

warnings.filterwarnings("error")


timeline_names = ["public", "local", "home", "hashtag", "list", "link"]


class Bot:
    def __init__(self):

        try:
            en_de_db = os.environ["EnDeDB"]
            all_db = os.environ["AllDB"]
            sysprompt_path = os.environ["SYSPROMPT"]
        except KeyError as e:
            logger.critical("Required environment variable is not set: %s", e)
            raise

        self.en_de_db_queue: asyncio.Queue[DBRequest] = asyncio.Queue()
        self.all_db_queue: asyncio.Queue[DBRequest] = asyncio.Queue()
        self.poster_response_queue: asyncio.Queue[OurPost] = asyncio.Queue()
        self.generator_response_queue: asyncio.Queue[ReadPost] = asyncio.Queue()
        self.db_en_de = DataBase(en_de_db)
        self.db_all = DataBase(all_db)
        self.streamer = Streamer(("politik", "politics", "trump", "putin"))
        self.generator = Generator()
        self.poster = Poster()
        self.read_prompt(sysprompt_path)

        config = HarnessConfig(
            model_path=MODEL_PATH,
            n_ctx=8192,
            n_threads=8,
            n_gpu_layers=0,
        )

        self.ai = BonsaiHarness(
            config=config,
            system_prompt=self.system_prompt,
            tools=[
                WebSearchTool(),
                SkipPostTool(),
            ],
        )
        self.generator.ai = self.ai
        logger.info("Loaded model: %s", MODEL_PATH)

    async def run(self):

        loop = asyncio.get_running_loop()

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.db_en_de.run(self.en_de_db_queue))
            tasks.create_task(self.db_all.run(self.all_db_queue))
            tasks.create_task(
                self.streamer.run(
                    en_de_q=self.en_de_db_queue, all_q=self.all_db_queue, loop=loop
                )
            )
            tasks.create_task(
                self.generator.run(self.en_de_db_queue, self.generator_response_queue)
            )
            tasks.create_task(
                self.poster.run(
                    database_queue=self.en_de_db_queue,
                    response_queue=self.poster_response_queue,
                )
            )

    def read_prompt(self, path):
        try:
            with open(path, "r", encoding="utf-8") as file:
                self.system_prompt = file.read()
        except FileNotFoundError:
            logger.warning("System prompt file was not found: %s", path)
            self.system_prompt = "You are a helpful assistant."  # Fallback prompt


async def main():
    bot = Bot()

    try:
        await bot.run()
    except asyncio.CancelledError:
        logger.info("Bot cancelled")
        raise
    finally:
        logger.info("Shutting down")


def configure_logging():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")
