from harness import BonsaiHarness, HarnessConfig
from tools import WebSearchTool


config = HarnessConfig(
    model_path="qwen2.5-0.5b-instruct-q4_k_m.gguf",
    n_ctx=8192,
    n_threads=8,
    n_gpu_layers=0,
)

ai = BonsaiHarness(
    config=config,
    system_prompt=(
        "Du bist ein hilfreicher Assistent. "
        "Verwende das Web-Suchtool, wenn aktuelle "
        "Informationen benötigt werden."
    ),
    tools=[
        WebSearchTool(),
    ],
)

print(ai.chat("Was sind die aktuellen Nachrichten in Deutschland?"))
