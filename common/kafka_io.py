"""Kafka I/O with Confluent Schema Registry Avro (de)serialization.

Keys are UTF-8 strings (the username); values are Avro, with the schema id
embedded via the SR wire format (magic byte + id). A small façade keeps the
agents and UI free of confluent-kafka boilerplate.
"""
from __future__ import annotations

import logging
from typing import Callable, Iterator

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

from . import schemas, settings

log = logging.getLogger(__name__)

_STRING_SER = StringSerializer("utf_8")
_STRING_DESER = StringDeserializer("utf_8")


class KafkaAvro:
    """Shared producer + per-topic Avro serializers/deserializers."""

    def __init__(self, bootstrap: str | None = None, sr_url: str | None = None) -> None:
        self._sr = SchemaRegistryClient(
            {"url": sr_url or settings.SCHEMA_REGISTRY_URL}
        )
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap or settings.KAFKA_BOOTSTRAP_SERVERS,
                # Order/lossless-ness matters more than throughput in this demo.
                "enable.idempotence": True,
                "linger.ms": 20,
                # These payloads are text-heavy (markdown reports, prompts, JSON).
                # zstd gives the best compression ratio for text at high speed;
                # consumers decompress transparently. linger batches before
                # compressing so the ratio is better.
                "compression.type": "zstd",
            }
        )
        self._serializers: dict[str, AvroSerializer] = {}
        self._deserializers: dict[str, AvroDeserializer] = {}

    # --- serialization helpers ------------------------------------------------
    def _serializer(self, topic: str) -> AvroSerializer:
        ser = self._serializers.get(topic)
        if ser is None:
            ser = AvroSerializer(
                self._sr,
                schemas.load_schema_str(topic),
                conf={"auto.register.schemas": True},
            )
            self._serializers[topic] = ser
        return ser

    def _deserializer(self, topic: str) -> AvroDeserializer:
        deser = self._deserializers.get(topic)
        if deser is None:
            # Reader schema from file; writer schema is resolved from the id.
            deser = AvroDeserializer(self._sr, schemas.load_schema_str(topic))
            self._deserializers[topic] = deser
        return deser

    # --- producing ------------------------------------------------------------
    def produce(self, topic: str, key: str, value: dict) -> None:
        self._producer.produce(
            topic=topic,
            key=_STRING_SER(key, SerializationContext(topic, MessageField.KEY)),
            value=self._serializer(topic)(
                value, SerializationContext(topic, MessageField.VALUE)
            ),
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    # --- consuming ------------------------------------------------------------
    def consume(
        self,
        topic: str,
        group_id: str,
        handler: Callable[[str, dict], None],
        *,
        auto_offset_reset: str = "earliest",
    ) -> None:
        """Block forever, dispatching each (key, value) to ``handler``.

        Offsets are committed only after the handler returns, so a crash mid-task
        re-delivers the message (at-least-once) rather than dropping it.
        """
        consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        deser = self._deserializer(topic)
        consumer.subscribe([topic])
        log.info("consuming topic=%s group=%s", topic, group_id)
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.error("consumer error: %s", msg.error())
                    continue
                key = (
                    _STRING_DESER(
                        msg.key(), SerializationContext(topic, MessageField.KEY)
                    )
                    if msg.key() is not None
                    else None
                )
                value = deser(
                    msg.value(), SerializationContext(topic, MessageField.VALUE)
                )
                try:
                    handler(key, value)
                except Exception:  # noqa: BLE001 - keep the consumer alive
                    log.exception("handler failed for key=%s", key)
                consumer.commit(msg)
        finally:
            consumer.close()

    def iter_messages(
        self, topic: str, group_id: str, *, auto_offset_reset: str = "latest"
    ) -> Iterator[tuple[str | None, dict]]:
        """Generator variant used by the UI's background consumer threads."""
        consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": True,
            }
        )
        deser = self._deserializer(topic)
        consumer.subscribe([topic])
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.error("consumer error: %s", msg.error())
                    continue
                key = (
                    _STRING_DESER(
                        msg.key(), SerializationContext(topic, MessageField.KEY)
                    )
                    if msg.key() is not None
                    else None
                )
                value = deser(
                    msg.value(), SerializationContext(topic, MessageField.VALUE)
                )
                yield key, value
        finally:
            consumer.close()
