import io
import os
import tempfile

import pytest
from dagger import DeserializationError, Serializer

from dagger_contrib.serializer.path.as_tar import AsTar

SUPPORTED_COMPRESSION_MODES = [
    None,
    "gzip",
    "xz",
    "bz2",
]


def test__conforms_to_protocol():
    with tempfile.TemporaryDirectory() as tmp:
        assert isinstance(AsTar(output_dir=tmp), Serializer)


def test_serialization_and_deserialization_are_symmetric_for_a_single_file():
    original_content = "original content"

    for compression in SUPPORTED_COMPRESSION_MODES:
        with tempfile.TemporaryDirectory() as tmp:
            # The original content, backed by the file system
            original_file = os.path.join(tmp, "original")
            with open(original_file, "w") as f:
                f.write(original_content)

            output_dir = os.path.join(tmp, "output_dir")
            os.mkdir(output_dir)
            serializer = AsTar(output_dir=output_dir, compression=compression)

            # The serializer produces a tar file
            serialized_tar = os.path.join(tmp, f"serialized_tar.{serializer.extension}")
            with open(serialized_tar, "wb") as writer:
                serializer.serialize(original_file, writer)

            # And it can read it back
            with open(serialized_tar, "rb") as reader:
                deserialized_file = serializer.deserialize(reader)

            # Retrieving a value equivalent to the original one (a filename pointing to the original content)
            assert deserialized_file.startswith(output_dir)
            with open(deserialized_file, "r") as f:
                assert f.read() == original_content


def test_serialization_and_deserialization_are_symmetric_for_a_directory():
    for compression in SUPPORTED_COMPRESSION_MODES:
        with tempfile.TemporaryDirectory() as tmp:
            # The original content, backed by the file system
            original_dir = os.path.join(tmp, "original_dir")
            original_subdir = os.path.join(original_dir, "subdir")
            os.makedirs(original_subdir)
            original_filenames = [
                "a",
                os.path.join("subdir", "a"),
                os.path.join("subdir", "b"),
            ]
            for filename in original_filenames:
                with open(os.path.join(original_dir, filename), "w") as f:
                    f.write(filename)

            output_dir = os.path.join(tmp, "output_dir")
            os.mkdir(output_dir)
            serializer = AsTar(output_dir=output_dir, compression=compression)

            # The serializer produces a tar file
            serialized_tar = os.path.join(tmp, f"serialized_tar.{serializer.extension}")
            with open(serialized_tar, "wb") as writer:
                serializer.serialize(original_dir, writer)

            # And it can read it back
            with open(serialized_tar, "rb") as reader:
                deserialized_dir = serializer.deserialize(reader)

            # Retrieving a value equivalent to the original one (a directory
            # containing files with the original structure and contents)
            assert deserialized_dir.startswith(output_dir)
            structure = {
                root: (set(dirs), set(files))
                for root, dirs, files in os.walk(deserialized_dir)
            }
            assert structure == {
                os.path.join(output_dir, "original_dir"): ({"subdir"}, {"a"}),
                os.path.join(output_dir, "original_dir", "subdir"): (set(), {"a", "b"}),
            }
            for filename in original_filenames:
                with open(os.path.join(deserialized_dir, filename), "r") as f:
                    assert f.read() == filename


def test_deserialize_invalid_tar_file():
    invalid_values = [
        b"",
        b"123",
    ]

    for value in invalid_values:
        for compression in SUPPORTED_COMPRESSION_MODES:
            with tempfile.TemporaryDirectory() as tmp:
                serializer = AsTar(output_dir=tmp, compression=compression)

                with pytest.raises(DeserializationError):
                    serializer.deserialize(io.BytesIO(value))


def test_extension_depends_on_compression():
    cases = [
        (None, "tar"),
        ("gzip", "tar.gz"),
        ("xz", "tar.xz"),
        ("bz2", "tar.bz2"),
    ]

    for compression, expected_extension in cases:
        with tempfile.TemporaryDirectory() as tmp:
            assert (
                AsTar(output_dir=tmp, compression=compression).extension
                == expected_extension
            )


def test_extension_fails_when_its_not_supported():
    with pytest.raises(AssertionError):
        with tempfile.TemporaryDirectory() as tmp:
            AsTar(output_dir=tmp, compression="unsupported")
