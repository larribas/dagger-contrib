import io
import os
import tempfile

import pytest
from dagger import DeserializationError, SerializationError, Serializer

from dagger_contrib.serializer.as_yaml import AsYAML


def test__conforms_to_protocol():
    assert isinstance(AsYAML(), Serializer)


def test_serialization_and_deserialization_are_symmetric():
    serializer = AsYAML()
    valid_values = [
        None,
        1,
        2.3,
        True,
        "string",
        [],
        [1, "two", 3],
        {},
        {"one": 2, "three": [4, "five"]},
    ]

    with tempfile.TemporaryDirectory() as tmp:
        filename = os.path.join(tmp, "value.yaml")

        for value in valid_values:
            with open(filename, "wb") as writer:
                serializer.serialize(value, writer)

            with open(filename, "rb") as reader:
                deserialized_value = serializer.deserialize(reader)

            assert value == deserialized_value


def test_serialization_indentation():
    serializer = AsYAML(indent=5)

    with tempfile.TemporaryDirectory() as tmp:
        filename = os.path.join(tmp, "value.yaml")

        with open(filename, "wb") as writer:
            serializer.serialize({"c": {"c1": [{"c2": 2}, {"c3": 3}]}}, writer)

        with open(filename, "r") as f:
            serialized_value = f.read()
            assert (
                serialized_value
                == """c:
     c1:
     -    c2: 2
     -    c3: 3
"""
            )


def test_serialize_invalid_values():
    class CustomType:
        pass

    serializer = AsYAML()
    invalid_values = [
        CustomType,
        CustomType(),
    ]

    for value in invalid_values:
        with pytest.raises(SerializationError):
            serializer.serialize(value, io.BytesIO())


def test_deserialize_invalid_values():
    serializer = AsYAML()
    invalid_values = [
        b"\x04",
        b"}{",
        b"a: [b, ]c],",
    ]

    for value in invalid_values:
        with pytest.raises(DeserializationError):
            serializer.deserialize(io.BytesIO(value))
