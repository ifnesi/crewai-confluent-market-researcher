"""Flask backend for the CrewAI report console.

Responsibilities:
  * username-only "login" (a signed-cookie session per user, no password);
  * publish research requests to crewai-ui-request-report;
  * run background consumers for crewai-agent-report-ready and crewai-logs, and
    fan their messages out to the right user over Server-Sent Events.

Messages are matched to a user by the Kafka *key* (the username), so the SSE
stream a browser opens only ever receives that user's logs and report.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from queue import Empty, Queue

from flask import (
    Flask,
    Response,
    jsonify,
    request,
    send_from_directory,
    session,
)

from common import lifecycle, settings
from common.kafka_io import KafkaAvro
from common.util import new_report_id, now_ms

logging.basicConfig(level=logging.INFO, format="%(asctime)s ui %(levelname)s %(message)s")
log = logging.getLogger("ui")

HERE = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=os.path.join(HERE, "static"), static_url_path="/static")
app.secret_key = os.getenv("FLASK_SECRET", "crewai-kafka-demo-secret-change-me")

# Unique per process so each UI instance reads only new (latest) activity.
INSTANCE = uuid.uuid4().hex[:8]

# username -> set of subscriber Queues (one per open SSE connection).
_subscribers: dict[str, set[Queue]] = defaultdict(set)
_subs_lock = threading.Lock()

_producer = KafkaAvro()

# Background consumer clients, tracked so graceful shutdown can stop + flush them.
_consumers: list[KafkaAvro] = []

# Field options offered in the UI dropdown.
FIELDS = ["Technology", "Finance", "HR", "Healthcare", "Retail", "Manufacturing", "Energy", "Logistics"]


# --- SSE fan-out -------------------------------------------------------------
def _json_default(o):
    """JSON-encode values the stats stream introduces. Avro logical timestamps
    deserialize to ``datetime``; the frontend expects epoch-ms (``new Date(...)``),
    so collapse them back to milliseconds."""
    if isinstance(o, datetime):
        if o.tzinfo is None:  # local-timestamp-millis comes back naive (UTC)
            o = o.replace(tzinfo=timezone.utc)
        return int(o.timestamp() * 1000)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _fanout(username: str | None, payload: dict) -> None:
    if not username:
        return
    with _subs_lock:
        for q in list(_subscribers.get(username, ())):
            q.put_nowait(payload)


def _consume(topic: str, group_suffix: str, kind: str) -> None:
    kafka = KafkaAvro()
    _consumers.append(kafka)
    group = f"ui-{group_suffix}-{INSTANCE}"
    log.info("UI consumer started topic=%s group=%s", topic, group)
    for key, value in kafka.iter_messages(topic, group_id=group, auto_offset_reset="latest"):
        _fanout(key, {"kind": kind, "value": value})


def _start_background_consumers() -> None:
    # Read the Flink-derived stats stream (no `data`, with `latency_ms`) rather
    # than the raw crewai-logs topic — same per-user keying, less payload.
    threading.Thread(
        target=_consume, args=(settings.TOPIC_LOGS_STATS, "logs", "log"), daemon=True
    ).start()
    threading.Thread(
        target=_consume, args=(settings.TOPIC_REPORT_READY, "report", "report"), daemon=True
    ).start()


# --- pages -------------------------------------------------------------------
@app.get("/")
def index() -> Response:
    return send_from_directory(app.static_folder, "index.html")


@app.get("/healthz")
def healthz():
    return jsonify(status="ok")


# --- session -----------------------------------------------------------------
@app.get("/api/session")
def get_session():
    return jsonify(username=session.get("username"), fields=FIELDS)


@app.post("/api/login")
def login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    if not username:
        return jsonify(error="username required"), 400
    if not re.fullmatch(r"[A-Za-z0-9_-]+", username):
        return jsonify(error="username may only contain letters, numbers, _ and -"), 400
    session["username"] = username
    log.info("login username=%s", username)
    return jsonify(username=username)


@app.post("/api/logout")
def logout():
    session.pop("username", None)
    return jsonify(ok=True)


# --- request a report --------------------------------------------------------
@app.post("/api/request")
def request_report():
    username = session.get("username")
    if not username:
        return jsonify(error="not logged in"), 401
    data = request.get_json(silent=True) or {}
    field = (data.get("field") or "").strip()
    process = (data.get("process") or "").strip()
    if not field or not process:
        return jsonify(error="field and process are required"), 400

    report_id = new_report_id()
    _producer.produce(
        settings.TOPIC_UI_REQUEST,
        key=username,
        value={
            "report_id": report_id,
            "field": field,
            "process": process,
            "timestamp": now_ms(),
            "extra_context": None,
            "counter": 0,
        },
    )
    _producer.flush()
    log.info("request username=%s report_id=%s field=%s", username, report_id, field)
    return jsonify(report_id=report_id)


# --- SSE stream --------------------------------------------------------------
@app.get("/api/stream")
def stream():
    username = session.get("username")
    if not username:
        return jsonify(error="not logged in"), 401

    q: Queue = Queue()
    with _subs_lock:
        _subscribers[username].add(q)

    def gen():
        try:
            yield "retry: 3000\n\n"
            while True:
                try:
                    payload = q.get(timeout=15)
                    yield f"data: {json.dumps(payload, default=_json_default)}\n\n"
                except Empty:
                    yield ": keepalive\n\n"
        finally:
            with _subs_lock:
                _subscribers[username].discard(q)

    return Response(
        gen(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


def _shutdown() -> None:
    """Stop background consumers and flush the request producer on exit."""
    for kafka in _consumers:
        try:
            kafka.close()
        except Exception:  # noqa: BLE001
            log.exception("error closing UI consumer")
    _producer.flush()


_start_background_consumers()


if __name__ == "__main__":
    lifecycle.on_shutdown(_shutdown)
    # threaded=True so long-lived SSE connections don't block other requests.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8088")), threaded=True)
