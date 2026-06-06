"""Expose the SearXNG-backed MCP server to CrewAI agents as tools.

All three agents can use web search: Scout to gather, Auditor to re-verify cited
URLs, Scribe to pull a supplementary fact. The MCP server runs as its own
container over streamable-HTTP, so this just points CrewAI at its URL.

Usage in an agent::

    from common.mcp_tools import open_mcp_tools
    with open_mcp_tools() as tools:
        agent = Agent(..., tools=tools)
        Crew(agents=[agent], tasks=[task]).kickoff()
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from . import settings

log = logging.getLogger(__name__)

MCP_PARAMS = {"url": settings.MCP_SERVER_URL, "transport": "streamable-http"}


@contextmanager
def open_mcp_tools() -> Iterator[list]:
    """Yield the MCP tool list, or an empty list if the server is unreachable.

    Returning ``[]`` on failure keeps an agent functional (it just can't search)
    rather than crashing the whole consumer on a transient MCP outage.
    """
    try:
        from crewai_tools import MCPServerAdapter
    except Exception:  # noqa: BLE001
        log.warning("crewai_tools MCPServerAdapter unavailable; no MCP tools")
        yield []
        return

    adapter = None
    try:
        adapter = MCPServerAdapter(MCP_PARAMS)
        tools = adapter.__enter__()
        log.info("MCP tools loaded: %s", [getattr(t, "name", "?") for t in tools])
        try:
            yield tools
        finally:
            adapter.__exit__(None, None, None)
    except Exception:  # noqa: BLE001
        log.exception("could not connect to MCP server at %s", settings.MCP_SERVER_URL)
        if adapter is not None:
            try:
                adapter.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
        yield []
