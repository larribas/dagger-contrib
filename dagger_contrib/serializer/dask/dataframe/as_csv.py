"""Serialize DataFrames as CSVs."""

import os
import tempfile
from typing import Any, BinaryIO, Optional

from dagger import DeserializationError, SerializationError, Serializer


class AsCSV:
    """
    Serializer implementation that uses CSV to serialize Dask DataFrames.

    See Also
    --------
    - https://docs.dask.org/en/latest/generated/dask.dataframe.DataFrame.to_csv.html#dask.dataframe.DataFrame.to_csv
    - https://docs.dask.org/en/latest/generated/dask.dataframe.read_csv.html#dask.dataframe.read_csv
    """

    GLOB_PATTERN = "df-*.csv"

    # Some compression algorithms do not support breaking apart files
    # Official message:
    # > Please ensure that each individual file can fit in memory and
    # > use the keyword ``blocksize=None to remove this message
    BLOCKSIZE_BY_COMPRESSION = {
        "gzip": None,
        "bz2": None,
        "xz": None,
    }

    def __init__(
        self,
        path_serializer: Serializer,
        csv_compression: Optional[str] = None,
    ):
        """
        Initialize a serializer that serializes DataFrame values as CSVs.

        Parameters
        ----------
        path_serializer: Serializer
            A Serializer implementation that works with path names pointing to a file or a directory in the local filesystem.
            Any serializer in dagger_contrib.path.* should be compatible (e.g. AsTar)

        csv_compression: str, optional
            The compression mode to use for each CSV file, which may be one of the following values: {"gzip", "bz2", "xz", None}
        """
        self._csv_compression = csv_compression
        self._path_serializer = path_serializer

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a Dask DataFrame as a series of CSV files packaged and compressed by the provided path serializer."""
        from dask.dataframe import DataFrame

        if not isinstance(value, DataFrame):
            raise SerializationError(
                f"This serializer only works with values of type dask.dataframe.DataFrame. You are trying to serialize a value of type '{type(value).__name__}'"
            )

        with tempfile.TemporaryDirectory() as tmp:
            value.to_csv(
                os.path.join(tmp, self.GLOB_PATTERN),
                compression=self._csv_compression,
            )
            self._path_serializer.serialize(tmp, writer)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize the content of 'reader' into a Dask DataFrame backed by a series of CSV files."""
        from dask.dataframe import read_csv
        from pandas.errors import EmptyDataError

        path = self._path_serializer.deserialize(reader)

        try:
            return read_csv(
                os.path.join(path, self.GLOB_PATTERN),
                compression=self._csv_compression,
                blocksize=self.BLOCKSIZE_BY_COMPRESSION.get(
                    self._csv_compression or "", "default"
                ),
            ).set_index("Unnamed: 0")
        except EmptyDataError as e:
            raise DeserializationError(e)
        except UnicodeDecodeError as e:
            raise DeserializationError(
                f"We could not deserialize the CSV artifact. This may be happening because the file was originally serialized with a particular compression mode, but you're trying to deserialize it with compression=None. The original error is: {str(e)}"
            ) from e

    @property
    def extension(self) -> str:
        """Extension to use for files generated by this serializer."""
        return self._path_serializer.extension
