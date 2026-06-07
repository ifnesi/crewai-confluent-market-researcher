"""Graceful shutdown wiring shared by the agents and the UI.

Containers are stopped with SIGTERM (``docker compose stop``/``down``) and a
local run is killed with SIGINT (Ctrl-C). Without a handler the process dies
mid-poll, possibly dropping buffered producer messages and leaving consumer
offsets uncommitted. ``on_shutdown`` lets each process flush producers and break
its consume loop before exiting.
"""
from __future__ import annotations

import logging
import signal
from typing import Callable

log = logging.getLogger(__name__)


def on_shutdown(*callbacks: Callable[[], None]) -> None:
    """Run ``callbacks`` on SIGINT/SIGTERM, then exit.

    Each callback is guarded so one failing (e.g. a producer flush timing out)
    still lets the others run. Callbacks typically set a consumer's stop event
    and flush its producer (e.g. ``KafkaAvro.close`` / ``logging_bus.shutdown``).
    """

    def handler(signum, _frame):  # noqa: ANN001
        log.info("received signal %s — shutting down gracefully", signum)
        for cb in callbacks:
            try:
                cb()
            except Exception:  # noqa: BLE001 - one bad callback must not block the rest
                log.exception("shutdown callback %r failed", getattr(cb, "__name__", cb))
        # Raise in the main thread to unwind any blocking call (consumer.poll /
        # Flask's app.run); consume()'s ``finally`` still runs to close the
        # consumer and commit offsets.
        raise SystemExit(0)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
