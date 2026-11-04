"""Mocked tests for the risk-score consumer.

No live Kafka or Redis is used. The Kafka consumer and the module-level
Redis client are both patched. ``main()`` loops forever on ``poll`` in
production, so the test ends the loop with ``KeyboardInterrupt``, which
``main()`` already handles. ``main()`` itself is not refactored.
"""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]


def load_risk_consumer_module():
    spec = importlib.util.spec_from_file_location(
        "risk_score_consumer", ROOT / "risk_score_consumer.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_risk_score_consumer_writes_to_redis():
    module = load_risk_consumer_module()
    message_payload = {
        "patient_id": "patient-1",
        "risk_level": "HIGH",
        "risk_pct": 82.5,
        "readmit_probability": 0.825,
        "source": "snowflake_cortex_readmission_model",
    }
    fake_msg = MagicMock()
    fake_msg.error.return_value = None
    fake_msg.value.return_value = json.dumps(message_payload).encode("utf-8")

    mock_consumer = MagicMock()
    mock_consumer.poll.side_effect = [fake_msg, KeyboardInterrupt()]

    with patch.object(module, "Consumer", return_value=mock_consumer):
        with patch.object(module, "r") as mock_redis:
            module.main()

    mock_redis.set.assert_called_once()
    call = mock_redis.set.call_args
    assert call.args[0] == "risk:patient-1"
    stored = json.loads(call.args[1])
    assert stored == {
        "risk_level": "HIGH",
        "risk_pct": 82.5,
        "readmit_probability": 0.825,
        "source": "snowflake_cortex_readmission_model",
    }
    assert call.kwargs["ex"] == 86400
    mock_consumer.close.assert_called_once()
