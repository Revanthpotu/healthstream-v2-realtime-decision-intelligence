"""Mocked tests for dead-letter-queue routing.

No live Kafka is used: ``self.producer`` is a mock, and the test asserts
on the exact topic and JSON payload ``send_to_dlq`` produces.
"""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def producer_module():
    spec = importlib.util.spec_from_file_location(
        "csv_producer", ROOT / "producer" / "csv_producer.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_rejected_record_reaches_dlq_with_reason(producer_module):
    producer = producer_module.VitalsProducer.__new__(
        producer_module.VitalsProducer
    )
    producer.producer = MagicMock()
    producer.stats = {"sent_to_dlq": 0, "errors": 0}

    row = {"PATIENT": "patient-1", "VALUE": "N/A", "CODE": "8867-4"}
    producer.send_to_dlq(row, "INVALID_VALUE: Cannot parse 'N/A' as number")

    producer.producer.produce.assert_called_once()
    call = producer.producer.produce.call_args
    assert call.kwargs["topic"] == "dead-letter-queue"
    payload = json.loads(call.kwargs["value"].decode("utf-8"))
    assert json.loads(payload["original_data"]) == row
    assert payload["error_reason"] == "INVALID_VALUE: Cannot parse 'N/A' as number"
    assert payload["source_system"] == "csv_lab_export"
    assert payload["failed_at"]
    assert payload["topic_intended"] == "patient-vitals"
    assert producer.stats["sent_to_dlq"] == 1
