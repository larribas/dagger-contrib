"""Implementation of a YAML serializer (https://yaml.org/spec/)."""

import io
from typing import Any, BinaryIO, Optional

from dagger import DeserializationError, SerializationError


class AsYAML:
    """Serializer implementation that uses YAML to marshal/unmarshal Python data structures."""

    extension = "yaml"

    def __init__(
        self,
        indent: Optional[int] = None,
    ):
        """
        Initialize a YAML serializer.

        Parameters
        ----------
        indent: int, optional
            Set the indentation level for YAML keys.
        """
        self._indent = indent

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a value into a YAML object, encoded into binary format using utf-8."""
        import yaml

        try:

            yaml.safe_dump(
                value,
                io.TextIOWrapper(writer, encoding="utf-8"),
                indent=self._indent,
            )
        except yaml.YAMLError as e:
            raise SerializationError(e)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize a utf-8-encoded yaml object into the value it represents."""
        import yaml

        try:
            return yaml.safe_load(reader)
        except yaml.YAMLError as e:
            raise DeserializationError(e)
