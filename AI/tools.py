from harness import Tool


class WebSearchTool(Tool):
    name = "web_search"

    description = (
        "Durchsucht das Internet nach aktuellen oder öffentlichen "
        "Informationen. Gibt Titel, URL und Textausschnitt zurück."
    )

    parameters = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Die Suchanfrage, z. B. "
                    "'aktuelles Wetter in Berlin'"
                ),
            },
            "max_results": {
                "type": "integer",
                "description": "Anzahl der Ergebnisse (1-10).",
                "default": 5,
            },
        },
        "required": ["query"],
    }

    def run(
        self,
        query: str,
        max_results: int = 5,
    ) -> str:

        # Werte begrenzen
        max_results = max(1, min(max_results, 10))

        # DDGS erst laden, wenn das Tool tatsächlich benutzt wird
        try:
            from ddgs import DDGS
        except ImportError:
            return (
                "Fehler: Das Python-Paket 'ddgs' ist nicht installiert."
            )

        try:
            print(f"Suche nach: {query}")

            with DDGS() as ddgs:
                results = list(
                    ddgs.text(
                        query,
                        max_results=max_results,
                    )
                )

        except Exception as e:
            return f"Suche fehlgeschlagen: {e}"

        if not results:
            return "Keine Ergebnisse gefunden."

        output = []

        for i, result in enumerate(results, start=1):
            title = result.get("title", "Ohne Titel")
            url = result.get("href", "")
            body = result.get("body", "")

            output.append(
                f"[{i}] {title}\n"
                f"{url}\n"
                f"{body}"
            )

        return "\n\n".join(output)