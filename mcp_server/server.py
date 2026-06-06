"""A very small MCP server exposing web search over a private SearXNG instance.

Built on the official MCP Python SDK (FastMCP) and served over streamable-HTTP
so any agent on the network can use it. The agents reach it at
http://mcp-server:8000/mcp and call the single ``web_search`` tool.
"""
from __future__ import annotations

import html
import json
import os
import re

import httpx
from mcp.server.fastmcp import FastMCP

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://searxng:8080")

mcp = FastMCP("crewai-web-search", host="0.0.0.0", port=8000)

_TAG_RE = re.compile(r"<[^>]+>")


def _clean(text: str | None, limit: int) -> str:
    """Strip HTML tags/entities and collapse whitespace, then truncate.

    SearXNG snippets and titles can contain markup (e.g. <span> highlights).
    Removing it keeps the meaning while cutting the tokens sent to the LLM.
    """
    if not text:
        return ""
    text = html.unescape(_TAG_RE.sub(" ", text))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


@mcp.tool()
async def web_search(query: str, num_results: int = 6) -> str:
    """Search the public web for current information.

    Use this for the latest industry improvements, how market leaders are
    innovating, newly entered companies, and where VCs are investing. Returns
    ranked results as JSON text — each with a title, url and snippet. Always
    cite the returned urls as references.

    Args:
        query: The search query.
        num_results: Maximum number of results to return (default 6).
    """
    params = {"q": query, "format": "json"}
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{SEARXNG_URL}/search", params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001 - surface a usable message to the agent
        return json.dumps({"query": query, "error": str(exc), "results": []})

    results = [
        {
            "title": _clean(item.get("title"), 200),
            "url": item.get("url"),
            "snippet": _clean(item.get("content"), 300),
        }
        for item in (data.get("results") or [])[: max(1, num_results)]
    ]
    # Compact JSON (no indentation) to keep the payload — and token cost — small.
    return json.dumps(
        {"query": query, "results": results},
        ensure_ascii=False,
        separators=(",", ":"),
    )


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
