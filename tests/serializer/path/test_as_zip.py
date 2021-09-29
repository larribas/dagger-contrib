import io
import os
import tempfile

import pytest
from dagger import DeserializationError, Serializer

from dagger_contrib.serializer.path.as_zip import AsZip, _find_base_dir

SUPPORTED_COMPRESSION_MODES = [
    "stored",
    "deflated",
    "lzma",
    "bz2",
]


def test__conforms_to_protocol():
    with tempfile.TemporaryDirectory() as tmp:
        assert isinstance(AsZip(output_dir=tmp), Serializer)


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
            serializer = AsZip(output_dir=output_dir, compression=compression)

            # The serializer produces a zip file
            serialized_zip = os.path.join(tmp, f"serialized.{serializer.extension}")
            with open(serialized_zip, "wb") as writer:
                serializer.serialize(original_file, writer)

            # And it can read it back
            with open(serialized_zip, "rb") as reader:
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
            serializer = AsZip(output_dir=output_dir, compression=compression)

            # The serializer produces a zip file
            serialized_zip = os.path.join(tmp, f"serialized.{serializer.extension}")
            with open(serialized_zip, "wb") as writer:
                serializer.serialize(original_dir, writer)

            # And it can read it back
            with open(serialized_zip, "rb") as reader:
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


def test_deserialize_invalid_zip_file():
    invalid_values = [
        b"",
        b"123",
    ]

    for value in invalid_values:
        for compression in SUPPORTED_COMPRESSION_MODES:
            with tempfile.TemporaryDirectory() as tmp:
                serializer = AsZip(output_dir=tmp, compression=compression)

                with pytest.raises(DeserializationError):
                    serializer.deserialize(io.BytesIO(value))


def test_extension_depends_on_compression():
    cases = [
        ("deflated", "zip.zlib"),
        ("stored", "zip"),
        ("bz2", "zip.bz2"),
        ("lzma", "zip.lzma"),
    ]

    for compression, expected_extension in cases:
        with tempfile.TemporaryDirectory() as tmp:
            assert (
                AsZip(output_dir=tmp, compression=compression).extension
                == expected_extension
            )


def test_extension_fails_when_compression_is_not_supported():
    with pytest.raises(AssertionError):
        with tempfile.TemporaryDirectory() as tmp:
            AsZip(output_dir=tmp, compression="unsupported")


def test_find_base_dir():
    cases = [
        {
            "paths": ["a"],
            "expected_result": "a",
        },
        {
            "paths": ["a", "b"],
            "expected_result": "",
        },
        {
            "paths": [
                os.path.join("subdir", "sub", "a"),
                os.path.join("subdir", "sub", "b"),
            ],
            "expected_result": "subdir",
        },
        {
            "paths": [
                os.path.join("subdir", "sub", "a"),
                os.path.join("subdir", "sub", "b"),
                os.path.join("subdir", "x"),
            ],
            "expected_result": "subdir",
        },
    ]

    for case in cases:
        assert _find_base_dir(case["paths"]) == case["expected_result"]
