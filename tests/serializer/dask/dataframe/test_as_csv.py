import gzip
import io
import os
import tempfile

import pytest
from dagger import DeserializationError, SerializationError, Serializer

from dagger_contrib.serializer.dask.dataframe.as_csv import AsCSV
from dagger_contrib.serializer.path.as_tar import AsTar


def test__conforms_to_protocol():
    with tempfile.TemporaryDirectory() as tmp:
        assert isinstance(AsCSV(path_serializer=AsTar(output_dir=tmp)), Serializer)


def test_serialization_and_deserialization_are_symmetric(df_with_multiple_partitions):
    compression_modes = [
        None,
        "gzip",
        "xz",
        "bz2",
    ]

    for compression in compression_modes:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = os.path.join(tmp, "tar_output_dir")
            path_serializer = AsTar(output_dir=output_dir)

            serializer = AsCSV(
                path_serializer=path_serializer,
                csv_compression=compression,
            )

            filename = os.path.join(tmp, f"file.{serializer.extension}")
            with open(filename, "wb") as writer:
                serializer.serialize(df_with_multiple_partitions, writer)

            with open(filename, "rb") as reader:
                deserialized_df = serializer.deserialize(reader)

            sum_of_original_df = df_with_multiple_partitions.sum().sum().compute()
            sum_of_deserialized_df = deserialized_df.sum().sum().compute()

            assert sum_of_original_df == sum_of_deserialized_df


def test_serialize_invalid_values():
    invalid_values = [
        None,
        2,
        "not a data frame",
        ["not", "a", "dataframe"],
        {"not": ["a", "dataframe"]},
    ]

    for value in invalid_values:

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = os.path.join(tmp, "tar_output_dir")
            path_serializer = AsTar(output_dir=output_dir)
            serializer = AsCSV(path_serializer=path_serializer)

            with pytest.raises(SerializationError) as e:
                serializer.serialize(value, io.BytesIO())

            assert (
                str(e.value)
                == f"This serializer only works with values of type dask.dataframe.DataFrame. You are trying to serialize a value of type '{type(value).__name__}'"
            )


def test_deserialize_invalid_value():
    with tempfile.TemporaryDirectory() as tmp:
        # The original content, backed by the file system
        csv_dir = os.path.join(tmp, "csv")
        os.mkdir(csv_dir)
        with open(os.path.join(csv_dir, "df-0.csv"), "w") as f:
            f.write("")  # Empty

        # The serializer produces a tar file
        output_dir = os.path.join(tmp, "output_dir")
        os.mkdir(output_dir)
        path_serializer = AsTar(output_dir=output_dir)
        serialized_tar = os.path.join(tmp, "serialized_tar.tar")
        with open(serialized_tar, "wb") as writer:
            path_serializer.serialize(csv_dir, writer)

        # We try to read it using the CSV serializer
        serializer = AsCSV(path_serializer=path_serializer)
        with open(serialized_tar, "rb") as reader:
            with pytest.raises(DeserializationError):
                serializer.deserialize(reader)


def test_extension_delegates_to_path_serializer():
    class CustomSerializer:
        extension = "custom.ext"

        def serialize(self, value, writer):
            pass

        def deserialize(self, reader):
            pass

    serializer = AsCSV(path_serializer=CustomSerializer())
    assert serializer.extension == "custom.ext"
