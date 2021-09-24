"""Implementation of a YAML serializer (https://yaml.org/spec/)."""

import io
from typing import Any, BinaryIO

from dagger import DeserializationError, SerializationError


class AsYAML:
    """Serializer implementation that uses YAML to marshal/unmarshal Python data structures."""

    extension = "yaml"

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a value into a YAML object, encoded into binary format using utf-8."""
        import yaml

        try:

            yaml.dump(
                value,
                io.TextIOWrapper(writer, encoding="utf-8"),
                Dumper=yaml.SafeDumper,
            )
        except yaml.YAMLError as e:
            raise SerializationError(e)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize a utf-8-encoded yaml object into the value it represents."""
        import yaml

        try:
            return yaml.load(reader, Loader=yaml.SafeLoader)
        except yaml.YAMLError as e:
            raise DeserializationError(e)
