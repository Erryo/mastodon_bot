from __future__ import annotations

import json
import re
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from llama_cpp import Llama


# ============================================================
# Tool Interface
# ============================================================

class Tool(ABC):
    """
    Basisklasse für alle Tools.

    Ein Tool besteht aus:
      - name
      - description
      - parameters (JSON Schema)
      - run(...)
    """

    name: str = ""
    description: str = ""

    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }

    def schema(self) -> dict[str, Any]:
        """OpenAI-kompatibles Tool-Schema für llama-cpp."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }

    @abstractmethod
    def run(self, **kwargs) -> str:
        """Tool ausführen."""
        raise NotImplementedError


# ============================================================
# Beispiel-Tools
# ============================================================

class CalculatorTool(Tool):
    name = "calculator"
    description = (
        "Führt einfache mathematische Berechnungen aus. "
        "Verwende dieses Tool für numerische Berechnungen."
    )

    parameters = {
        "type": "object",
        "properties": {
            "expression": {
                "type": "string",
                "description": "Mathematischer Ausdruck, z.B. 25 * 4 + 10",
            }
        },
        "required": ["expression"],
        "additionalProperties": False,
    }

    def run(self, expression: str) -> str:
        # Für Produktion besser einen sicheren Parser verwenden.
        allowed = re.fullmatch(r"[0-9+\-*/(). %]+", expression)

        if not allowed:
            return "Fehler: Ungültiger mathematischer Ausdruck."

        try:
            result = eval(expression, {"__builtins__": {}}, {})
            return str(result)
        except Exception as e:
            return f"Fehler bei Berechnung: {e}"


class CurrentTimeTool(Tool):
    name = "current_time"
    description = "Gibt die aktuelle lokale Systemzeit zurück."

    parameters = {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }

    def run(self) -> str:
        from datetime import datetime

        return datetime.now().isoformat()


# ============================================================
# Tool Registry
# ============================================================

class ToolRegistry:
    """
    Zentrale Verwaltung aller Tools.

    Neue Tools können jederzeit registriert werden.
    """

    def __init__(self):
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        if not tool.name:
            raise ValueError(
                f"Tool {tool.__class__.__name__} besitzt keinen Namen."
            )

        if tool.name in self._tools:
            raise ValueError(
                f"Tool '{tool.name}' ist bereits registriert."
            )

        self._tools[tool.name] = tool

    def register_many(self, *tools: Tool) -> None:
        for tool in tools:
            self.register(tool)

    def unregister(self, name: str) -> None:
        self._tools.pop(name, None)

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def all(self) -> list[Tool]:
        return list(self._tools.values())

    def schemas(self) -> list[dict[str, Any]]:
        return [tool.schema() for tool in self._tools.values()]

    def names(self) -> list[str]:
        return list(self._tools.keys())


# ============================================================
# Tool Call Parser
# ============================================================

class ToolCallParser:
    """
    Parser für native llama.cpp Tool Calls UND
    XML/Text-Fallbacks wie:

        <tool_call>
        {"name": "...", "parameters": {...}}
        </tool_call>

    """

    TOOL_CALL_PATTERN = re.compile(
        r"<tool_call>\s*(.*?)\s*</tool_call>",
        re.DOTALL | re.IGNORECASE,
    )

    @staticmethod
    def parse(message: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Versucht zuerst native tool_calls zu verwenden.
        Danach XML/Text-Fallback.
        """

        native_calls = message.get("tool_calls")

        if native_calls:
            return native_calls

        content = message.get("content") or ""

        return ToolCallParser.parse_text(content)

    @staticmethod
    def parse_text(content: str) -> list[dict[str, Any]]:
        matches = ToolCallParser.TOOL_CALL_PATTERN.findall(content)

        if not matches:
            return []

        calls = []

        for raw in matches:
            raw = raw.strip()

            # Häufiger Fehler kleiner/quantisierter Modelle:
            # {{ ... }}
            if raw.startswith("{{") and raw.endswith("}}"):
                raw = raw[1:-1]

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                # Eventuell ```json ... ```
                raw = re.sub(r"^```json\s*", "", raw)
                raw = re.sub(r"\s*```$", "", raw)

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue

            name = data.get("name")

            if not name:
                continue

            parameters = data.get(
                "parameters",
                data.get("arguments", {}),
            )

            calls.append({
                "id": f"call_{uuid.uuid4().hex[:12]}",
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": json.dumps(
                        parameters,
                        ensure_ascii=False,
                    ),
                },
            })

        return calls


# ============================================================
# Bonsai Harness
# ============================================================

@dataclass
class HarnessConfig:
    model_path: str

    # Context
    n_ctx: int = 8192

    # CPU
    n_threads: int = 8

    # GPU:
    # 0 = CPU only
    # -1 = alle Layer auf GPU
    n_gpu_layers: int = 0

    # Sampling
    temperature: float = 0.2
    top_p: float = 0.9
    top_k: int = 40

    # Tool loop
    max_tool_rounds: int = 10

    # Output
    max_tokens: int = 2048

    # llama.cpp
    verbose: bool = False


class BonsaiHarness:
    """
    Modulares lokales LLM-Harness.

    Architektur:

        User
          |
          v
      BonsaiHarness
          |
          +---- ToolRegistry
          |
          +---- llama.cpp
          |
          +---- ToolCallParser
          |
          v
      Tool execution
          |
          v
      final response
    """

    def __init__(
        self,
        config: HarnessConfig,
        system_prompt: str = "",
        tools: list[Tool] | None = None,
    ):
        self.config = config
        self.system_prompt = system_prompt

        self.registry = ToolRegistry()
        self.messages: list[dict[str, Any]] = []

        # ----------------------------------------------------
        # Tools registrieren
        # ----------------------------------------------------

        for tool in tools or []:
            self.registry.register(tool)

        # ----------------------------------------------------
        # llama.cpp
        # ----------------------------------------------------

        print("Loading Bonsai model...")

        self.llm = Llama(
            model_path=config.model_path,

            n_ctx=config.n_ctx,

            n_threads=config.n_threads,
            n_gpu_layers=config.n_gpu_layers,

            verbose=config.verbose,
        )

        print("Bonsai model loaded.")

    # ========================================================
    # Tool Management
    # ========================================================

    def register_tool(self, tool: Tool) -> None:
        self.registry.register(tool)

    def register_tools(self, *tools: Tool) -> None:
        self.registry.register_many(*tools)

    def unregister_tool(self, name: str) -> None:
        self.registry.unregister(name)

    # ========================================================
    # Conversation
    # ========================================================

    def reset(self) -> None:
        self.messages.clear()

    def history(self) -> list[dict[str, Any]]:
        return list(self.messages)

    # ========================================================
    # Chat
    # ========================================================

    def chat(self, user_message: str) -> str:
        """
        Führt einen kompletten Chat-Turn aus.

        Beispiel:

            User
              ↓
            Bonsai
              ↓
            Tool Call?
             /   \
           nein   ja
            ↓      ↓
          answer  tool
                   ↓
                 result
                   ↓
                 Bonsai
                   ↓
                answer
        """

        self.messages.append({
            "role": "user",
            "content": user_message,
        })

        for round_index in range(
            self.config.max_tool_rounds
        ):
            response = self._generate()

            choice = self._get_choice(response)

            if not choice:
                return "Fehler: Keine Modellantwort erhalten."

            message = choice.get("message", {})

            if not message:
                return "Fehler: Leere Modellantwort."

            # ------------------------------------------------
            # Tool Calls erkennen
            # ------------------------------------------------

            tool_calls = ToolCallParser.parse(message)

            # Native/parsed calls in Verlauf übernehmen
            if tool_calls:
                message = dict(message)

                # Bei XML-Fallback Content entfernen
                if not message.get("tool_calls"):
                    message["content"] = None
                    message["tool_calls"] = tool_calls

            self.messages.append(message)

            # ------------------------------------------------
            # Kein Tool → finale Antwort
            # ------------------------------------------------

            if not tool_calls:
                content = message.get("content") or ""

                return self._clean_response(content)

            # ------------------------------------------------
            # Tools ausführen
            # ------------------------------------------------

            for tool_call in tool_calls:
                result = self._execute_tool(tool_call)

                self.messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": result,
                })

        return (
            "Fehler: Maximale Anzahl an Tool-Runden "
            "erreicht."
        )

    # ========================================================
    # Generation
    # ========================================================

    def _generate(self) -> dict[str, Any]:
        messages = []

        if self.system_prompt:
            messages.append({
                "role": "system",
                "content": self.system_prompt,
            })

        messages.extend(self.messages)

        kwargs: dict[str, Any] = {
            "messages": messages,

            "max_tokens": self.config.max_tokens,

            "temperature": self.config.temperature,
            "top_p": self.config.top_p,
            "top_k": self.config.top_k,
        }

        # ----------------------------------------------------
        # Tools
        # ----------------------------------------------------

        if self.registry.all():
            kwargs["tools"] = self.registry.schemas()

            # llama.cpp unterstützt OpenAI-artige Tool Choice
            kwargs["tool_choice"] = "auto"

        return self.llm.create_chat_completion(
            **kwargs
        )

    # ========================================================
    # Response helpers
    # ========================================================

    @staticmethod
    def _get_choice(
        response: dict[str, Any],
    ) -> dict[str, Any]:
        choices = response.get("choices")

        if not choices:
            return {}

        if isinstance(choices, list):
            return choices[0]

        if isinstance(choices, dict):
            return choices

        return {}

    @staticmethod
    def _clean_response(content: str) -> str:
        content = content.strip()

    # Thinking entfernen
        content = re.sub(
            r"<think>.*?</think>",
            "",
            content,
            flags=re.DOTALL | re.IGNORECASE,
        )

    # Falls </think> ohne <think> auftaucht
        if "</think>" in content.lower():
            content = re.sub(
                r".*?</think>",
                "",
                content,
                flags=re.DOTALL | re.IGNORECASE,
            )

    # XML Tool Calls entfernen
        content = re.sub(
            r"<tool_call>.*?</tool_call>",
            "",
            content,
            flags=re.DOTALL | re.IGNORECASE,
        )

        return content.strip()


    # ========================================================
    # Tool Execution
    # ========================================================

    def _execute_tool(
        self,
        tool_call: dict[str, Any],
    ) -> str:

        function = tool_call.get(
            "function",
            {},
        )

        name = function.get("name", "")

        tool = self.registry.get(name)

        if tool is None:
            return (
                f"Tool '{name}' existiert nicht. "
                f"Verfügbare Tools: "
                f"{', '.join(self.registry.names())}"
            )

        arguments = function.get(
            "arguments",
            "{}",
        )

        # ----------------------------------------------------
        # Argumente parsen
        # ----------------------------------------------------

        if isinstance(arguments, dict):
            args = arguments

        else:
            try:
                args = json.loads(arguments)

            except json.JSONDecodeError as e:
                return (
                    f"Fehler: Ungültige JSON-Argumente "
                    f"für Tool '{name}': {e}"
                )

        if not isinstance(args, dict):
            return (
                f"Fehler: Argumente für '{name}' "
                f"müssen ein JSON-Objekt sein."
            )

        # ----------------------------------------------------
        # Tool ausführen
        # ----------------------------------------------------

        try:
            result = tool.run(**args)

            return str(result)

        except TypeError as e:
            return (
                f"Fehlerhafte Argumente für "
                f"'{name}': {e}"
            )

        except Exception as e:
            return (
                f"Fehler bei Ausführung von "
                f"'{name}': {e}"
            )


# ============================================================
# Beispiel
# ============================================================

if __name__ == "__main__":

    config = HarnessConfig(
        model_path="bonsai-27b-q1.gguf",

        # Für 27B ggf. deutlich größer wählen,
        # abhängig von RAM/VRAM.
        n_ctx=8192,

        # An deine CPU anpassen.
        n_threads=8,

        # CPU only:
        n_gpu_layers=0,

        temperature=0.2,
        top_p=0.9,

        max_tool_rounds=10,
        max_tokens=2048,
    )

    system_prompt = """
Du bist Bonsai, ein lokaler KI-Assistent.

Du kannst verfügbare Tools verwenden.

Regeln:
- Verwende Tools nur, wenn sie für die Aufgabe notwendig sind.
- Erfinde niemals Tool-Ergebnisse.
- Warte nach einem Tool-Aufruf auf dessen Ergebnis.
- Antworte dem Benutzer erst, wenn die Aufgabe abgeschlossen ist.
- Antworte auf Deutsch, wenn der Benutzer Deutsch spricht.
"""

    ai = BonsaiHarness(
        config=config,
        system_prompt=system_prompt,
        tools=[
            CalculatorTool(),
            CurrentTimeTool(),
        ],
    )

    while True:

        try:
            user = input("\nDu: ").strip()

        except KeyboardInterrupt:
            print("\nBeendet.")
            break

        if not user:
            continue

        if user.lower() in {
            "exit",
            "quit",
            "bye",
        }:
            break

        if user.lower() == "/reset":
            ai.reset()
            print("Konversation zurückgesetzt.")
            continue

        if user.lower() == "/tools":
            print(
                "Tools:",
                ", ".join(ai.registry.names()),
            )
            continue

        answer = ai.chat(user)

        print("\nBonsai:", answer)
