import gzip
import io
import os
import tempfile

import pytest
from dagger import DeserializationError, SerializationError, Serializer

from dagger_contrib.serializer.pandas.as_csv import AsCSV


def test__conforms_to_protocol():
    assert isinstance(AsCSV(), Serializer)


def test_serialization_and_deserialization_are_symmetric(star_wars_dataframe):
    compression_modes = [
        None,
        "gzip",
        "zip",
        "xz",
        "bz2",
        "infer",
    ]

    with tempfile.TemporaryDirectory() as tmp:
        for compression in compression_modes:
            serializer = AsCSV(compression=compression)
            filename = os.path.join(tmp, f"file.{serializer.extension}")

            with open(filename, "wb") as writer:
                serializer.serialize(star_wars_dataframe, writer)

            with open(filename, "rb") as reader:
                deserialized_df = serializer.deserialize(reader)

            assert star_wars_dataframe.equals(deserialized_df)


def test_serialize_invalid_values():
    serializer = AsCSV()
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
                == f"The pandas.AsCSV serializer only works with values of type pd.DataFrame. You are trying to serialize a value of type '{type(value).__name__}'"
            )


def test_deserialize_empty_file():
    serializer = AsCSV()
    with pytest.raises(DeserializationError):
        serializer.deserialize(io.BytesIO(b""))


def test_deserialize_uncompressed_file_with_compression():
    serializer = AsCSV(compression="gzip")
    with pytest.raises(gzip.BadGzipFile):
        serializer.deserialize(io.BytesIO(b"a,b"))


def test_deserialize_compressed_file_without_compression(star_wars_dataframe):
    with tempfile.TemporaryDirectory() as tmp:
        filename = os.path.join(tmp, "value.csv.gz")

        with open(filename, "wb") as writer:
            AsCSV(compression="gzip").serialize(star_wars_dataframe, writer)

        with open(filename, "rb") as reader:
            with pytest.raises(DeserializationError) as e:
                AsCSV().deserialize(reader)

            assert str(e.value).startswith(
                "We could not deserialize the CSV artifact. This may be happening because the file was originally serialized with a particular compression mode, but you're trying to deserialize it with compression=None. The original error is:"
            )


def test_extension_depends_on_compression():
    cases = [
        (None, "csv"),
        ("gzip", "csv.gz"),
        ("zip", "csv.zip"),
        ("xz", "csv.xz"),
        ("bz2", "csv.bz2"),
        ("infer", "csv"),
        ("something_else", "csv"),
    ]

    for compression, expected_extension in cases:
        assert AsCSV(compression=compression).extension == expected_extension
