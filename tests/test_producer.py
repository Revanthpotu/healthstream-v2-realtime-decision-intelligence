"""Mocked tests for the CSV producer: validation and Avro failure routing.

No live Kafka, Schema Registry, Redis, Snowflake, or OpenAI is used.
``VitalsProducer`` is built with ``__new__`` so ``__init__`` (which creates
a real Kafka producer, registers an Avro schema, and exits if
``data/synthea/patients.csv`` is missing) is never run. Only
``demographics`` and ``VALID_RANGES`` are needed by ``validate_vital``.
"""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_producer_module():
    spec = importlib.util.spec_from_file_location(
        "csv_producer", ROOT / "producer" / "csv_producer.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def producer_module():
    return load_producer_module()


def make_producer(module):
    producer = module.VitalsProducer.__new__(module.VitalsProducer)
    producer.demographics = {
        "patient-1": {"birthdate": "1980-01-01", "gender": "M"},
    }
    producer.stats = {
        "total_rows": 0,
        "vitals_found": 0,
        "produced": 0,
        "skipped_non_vital": 0,
        "sent_to_dlq": 0,
        "errors": 0,
    }
    producer.producer = MagicMock()
    return producer


def base_row(**overrides):
    row = {
        "PATIENT": "patient-1",
        "ENCOUNTER": "enc-1",
        "DATE": "2024-01-01T10:00:00Z",
        "CODE": "8867-4",
        "DESCRIPTION": "Heart Rate",
        "VALUE": "72",
        "UNITS": "bpm",
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    "row,reason_prefix",
    [
        (base_row(VALUE=""), "MISSING_VALUE"),
        (base_row(VALUE="N/A"), "INVALID_VALUE"),
        (base_row(VALUE="999"), "OUT_OF_RANGE"),
        (base_row(PATIENT="unknown-patient"), "UNKNOWN_PATIENT"),
        (base_row(DATE=""), "MISSING_DATE"),
    ],
)
def test_validate_vital_rejects_bad_records(producer_module, row, reason_prefix):
    producer = make_producer(producer_module)
    is_valid, error_reason = producer.validate_vital(row)
    assert is_valid is False
    assert error_reason.startswith(reason_prefix)


def test_validate_vital_accepts_good_record(producer_module):
    producer = make_producer(producer_module)
    is_valid, error_reason = producer.validate_vital(base_row())
    assert is_valid is True
    assert error_reason is None


def test_avro_serializer_failure_routes_to_dlq_as_production_error(
    producer_module,
):
    producer = make_producer(producer_module)
    producer.avro_serializer = MagicMock(side_effect=RuntimeError("bad avro"))
    producer.string_serializer = MagicMock(return_value=b"patient-1")
    producer.produce_vital(base_row())

    dlq_calls = [
        call
        for call in producer.producer.produce.call_args_list
        if call.kwargs.get("topic") == producer_module.DLQ_TOPIC
    ]
    assert dlq_calls, "expected a produce call to the dead-letter-queue"
    payload = json.loads(dlq_calls[0].kwargs["value"].decode("utf-8"))
    assert payload["error_reason"].startswith("PRODUCTION_ERROR")
    assert payload["topic_intended"] == producer_module.VITALS_TOPIC
