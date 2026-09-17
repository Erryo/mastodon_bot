"""
Test script for ToolCallParser.

Reads sample "posts" (model outputs) from a text file and checks
whether ToolCallParser.parse_text() extracts the expected tool call
(or correctly finds none) for each one.

File format (see test_posts.txt):

    ### TEST: <case name>
    EXPECT: <tool_name>          (or: EXPECT: none)
    <raw post text, one or more lines>

    ### TEST: <next case name>
    ...

Usage:
    python test_tool_call_parser.py             # uses test_posts.txt next to this script
    python test_tool_call_parser.py other.txt   # or point at a different file
"""

import json
import re
import sys
from pathlib import Path

from AI.harness import ToolCallParser

# Allowlist of tool names the bot actually knows about — mirrors how
# the real bot calls parse_text(..., tool_names=[...]).
TOOL_NAMES = ["skip_post", "reply_post"]

DEFAULT_POSTS_FILE = Path(__file__).parent / "test_posts.txt"


def call_name(call: dict) -> str | None:
    """Pull the tool name out of a parsed call, whichever shape
    _make_call() produces: either the OpenAI-style
    {"function": {"name": ...}} nesting, or a flat {"name": ...}."""
    if isinstance(call.get("function"), dict):
        return call["function"].get("name")
    return call.get("name")


def call_arguments(call: dict) -> dict:
    """Pull the parsed arguments dict out of a call, handling both the
    OpenAI-style nesting (where "arguments" is a JSON string) and a
    flat {"parameters": {...}} dict."""
    if isinstance(call.get("function"), dict):
        raw = call["function"].get("arguments", "{}")
        return json.loads(raw) if isinstance(raw, str) else raw
    return call.get("parameters", {})


def load_test_cases(path: Path) -> list[dict]:
    """Parse the ### TEST: / EXPECT: / <post text> file format."""
    text = path.read_text(encoding="utf-8")
    # drop anything before the first block
    blocks = re.split(r"(?m)^### TEST:\s*", text)[1:]

    cases = []
    for block in blocks:
        lines = block.splitlines()
        case_name = lines[0].strip()

        rest = lines[1:]
        expect_idx = next(
            i for i, line in enumerate(rest) if line.strip().startswith("EXPECT:")
        )
        expected = rest[expect_idx].split("EXPECT:", 1)[1].strip()

        post_lines = rest[expect_idx + 1 :]
        while post_lines and not post_lines[0].strip():
            post_lines.pop(0)
        while post_lines and not post_lines[-1].strip():
            post_lines.pop()

        cases.append(
            {
                "name": case_name,
                "expected": expected,
                "post": "\n".join(post_lines),
            }
        )

    return cases


def run_file_based_tests(path: Path) -> tuple[int, int]:
    cases = load_test_cases(path)
    passed = failed = 0

    for case in cases:
        calls = ToolCallParser.parse_text(case["post"], tool_names=TOOL_NAMES)
        found_names = [call_name(c) for c in calls]

        ok = (
            len(found_names) == 0
            if case["expected"] == "none"
            else case["expected"] in found_names
        )

        print(f"[{'PASS' if ok else 'FAIL'}] {case['name']}")
        if not ok:
            print(f"         expected: {case['expected']!r}")
            print(f"         got calls: {calls}")

        passed += ok
        failed += not ok

    return passed, failed


def run_parse_unit_tests() -> tuple[int, int]:
    """A couple of direct checks on ToolCallParser.parse() itself,
    covering the native tool_calls passthrough path that the file
    of sample posts can't exercise (that path needs a dict message,
    not raw text)."""
    passed = failed = 0

    # 1) native tool_calls present -> returned untouched, no text parsing
    message = {"tool_calls": [{"name": "reply_post", "parameters": {"text": "hi"}}]}
    result = ToolCallParser.parse(message, tool_names=TOOL_NAMES)
    ok = result == message["tool_calls"]
    print(f"[{'PASS' if ok else 'FAIL'}] parse_native_tool_calls_passthrough")
    passed += ok
    failed += not ok

    # 2) no native tool_calls -> falls back to parsing message["content"]
    message = {"content": 'skip_post {"reason": "fallback path"}'}
    result = ToolCallParser.parse(message, tool_names=TOOL_NAMES)
    ok = len(result) == 1 and call_name(result[0]) == "skip_post"
    print(f"[{'PASS' if ok else 'FAIL'}] parse_falls_back_to_text_content")
    if not ok:
        print(f"         got: {result}")
    passed += ok
    failed += not ok

    # 3) empty/missing content -> no calls, no crash
    result = ToolCallParser.parse({})
    ok = result == []
    print(f"[{'PASS' if ok else 'FAIL'}] parse_empty_message_returns_empty_list")
    passed += ok
    failed += not ok

    return passed, failed


def main() -> int:
    posts_file = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_POSTS_FILE

    print(f"Running text/XML parsing tests from {posts_file.name}...\n")
    p1, f1 = run_file_based_tests(posts_file)

    print("\nRunning parse() unit tests...\n")
    p2, f2 = run_parse_unit_tests()

    total_passed, total_failed = p1 + p2, f1 + f2
    print(
        f"\n{total_passed} passed, {total_failed} failed, {
            total_passed + total_failed
        } total"
    )

    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
