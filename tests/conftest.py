"""Test-only import fallback.

The tests mock Kafka/Redis at the boundaries, so they do not need a live
broker. If the real ``confluent_kafka`` package (or one of its transitive
dependencies) is not importable in the test environment, stub its modules
before the producer module is loaded. When the real package is present,
it is used unchanged. This file is test-only: it is not imported by any
production code, and ``pytest`` itself is a test-only dependency, not
added to ``requirements.txt``.
"""

import sys
import types
from unittest.mock import MagicMock

try:
    import confluent_kafka.schema_registry.avro  # noqa: F401
except Exception:
    confluent_kafka = types.ModuleType("confluent_kafka")
    confluent_kafka.Producer = MagicMock
    confluent_kafka.Consumer = MagicMock
    confluent_kafka.KafkaError = MagicMock
    schema_registry = types.ModuleType("confluent_kafka.schema_registry")
    schema_registry.SchemaRegistryClient = MagicMock
    avro = types.ModuleType("confluent_kafka.schema_registry.avro")
    avro.AvroSerializer = MagicMock
    serialization = types.ModuleType("confluent_kafka.serialization")
    serialization.SerializationContext = MagicMock
    serialization.MessageField = MagicMock
    serialization.StringSerializer = MagicMock
    sys.modules["confluent_kafka"] = confluent_kafka
    sys.modules["confluent_kafka.schema_registry"] = schema_registry
    sys.modules["confluent_kafka.schema_registry.avro"] = avro
    sys.modules["confluent_kafka.serialization"] = serialization
try:
    import redis  # noqa: F401
except Exception:
    redis_module = types.ModuleType("redis")
    redis_module.Redis = MagicMock
    sys.modules["redis"] = redis_module
try:
    import dotenv  # noqa: F401
except Exception:
    dotenv_module = types.ModuleType("dotenv")
    dotenv_module.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_module
