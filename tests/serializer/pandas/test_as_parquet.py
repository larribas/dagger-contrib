import io
import os
import tempfile

import pytest
from dagger import SerializationError, Serializer

from dagger_contrib.serializer.pandas.as_parquet import AsParquet


def test__conforms_to_protocol():
    assert isinstance(AsParquet(), Serializer)


def test_serialization_and_deserialization_are_symmetric(star_wars_dataframe):
    compression_modes = [
        None,
        "snappy",
        "gzip",
        "brotli",
    ]

    with tempfile.TemporaryDirectory() as tmp:
        filename = os.path.join(tmp, "value.parquet")

        for compression in compression_modes:
            serializer = AsParquet(compression=compression)

            with open(filename, "wb") as writer:
                serializer.serialize(star_wars_dataframe, writer)

            with open(filename, "rb") as reader:
                deserialized_df = serializer.deserialize(reader)

            assert star_wars_dataframe.equals(deserialized_df)


def test_serialize_invalid_values():
    serializer = AsParquet()
    invalid_values = [
        None,
        2,
        "not a data frame",
        ["not", "a", "dataframe"],
        {"not": ["a", "dataframe"]},
    ]

    for value in invalid_values:
        with pytest.raises(SerializationError) as e:
            serializer.serialize(value, io.BytesIO())

            assert (
                str(e.value)
                == f"The pandas.AsParquet serializer only works with values of type pd.DataFrame. You are trying to serialize a value of type '{type(value).__name__}'"
            )


def test_deserialize_empty_file():
    serializer = AsParquet()
    with pytest.raises(Exception):
        serializer.deserialize(io.BytesIO(b""))
