"""Shared library for the choreographed CrewAI + Kafka research system.

Every agent container and the Flask UI import from this package so the wire
format (Avro via Schema Registry), the Bedrock LLM wiring, the per-call logging
to ``crewai-logs`` and the MCP tool plumbing live in exactly one place.
"""
