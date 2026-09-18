import warnings
import os
import asyncio

from dotenv import load_dotenv
from DataBase import DataBase
from Streamer import Streamer
from Poster import Poster
from AI.harness import BonsaiHarness, HarnessConfig
from AI.tools import WebSearchTool, SkipPostTool

from DataTypes import DBRequest, Post

load_dotenv()


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
            print(f"Environment variable not set:{e}")

        self.en_de_db_queue: asyncio.Queue[DBRequest] = asyncio.Queue()
        self.all_db_queue: asyncio.Queue[DBRequest] = asyncio.Queue()
        self.poster_response_queue: asyncio.Queue[Post] = asyncio.Queue()
        self.db_en_de = DataBase(en_de_db, True)
        self.db_all = DataBase(all_db, False)
        self.poster = Poster()
        self.streamer_en = Streamer("politics")
        self.streamer_de = Streamer("politik")
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
        self.poster.ai = self.ai
        print("Model:", MODEL_PATH)

    async def run(self):
        loop = asyncio.get_running_loop()

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.db_en_de.run(self.en_de_db_queue))
            tasks.create_task(self.db_all.run(self.all_db_queue))
            tasks.create_task(
                self.streamer_en.run(
                    en_de_q=self.en_de_db_queue, all_q=self.all_db_queue, loop=loop
                )
            )
            tasks.create_task(
                self.streamer_de.run(
                    en_de_q=self.en_de_db_queue, all_q=self.all_db_queue, loop=loop
                )
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
            print("Error: The system prompt file was not found.")
            self.system_prompt = "You are a helpful assistant."  # Fallback prompt


async def main():
    bot = Bot()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
