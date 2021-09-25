"""Serialize DataFrames as CSVs."""

import os
import tempfile
from typing import Any, BinaryIO, Optional

from dagger import DeserializationError, SerializationError, Serializer


class AsParquet:
    """
    Serializer implementation that uses Parquet to serialize Dask DataFrames.

    See Also
    --------
    - https://docs.dask.org/en/latest/generated/dask.dataframe.DataFrame.to_parquet.html#dask.dataframe.DataFrame.to_parquet
    - https://docs.dask.org/en/latest/generated/dask.dataframe.read_parquet.html#dask.dataframe.read_parquet
    """

    def __init__(
        self,
        path_serializer: Serializer,
        engine: str = "auto",
        compression: Optional[str] = "snappy",
    ):
        """
        Initialize a serializer that serializes DataFrame values as CSVs.

        Parameters
        ----------
        path_serializer: Serializer
            A Serializer implementation that works with path names pointing to a file or a directory in the local filesystem.
            Any serializer in dagger_contrib.path.* should be compatible (e.g. AsTar)

        engine: str, default="auto"
            Parquet library to use.

        compression: str, optional, default="snappy"
            The compression mode, which may be one of the following values: {"snappy", "gzip", "brotli", None}
        """
        self._path_serializer = path_serializer
        self._engine = engine
        self._compression = compression

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a Dask DataFrame as Parquet file directory packaged and compressed by the provided path serializer."""
        from dask.dataframe import DataFrame

        if not isinstance(value, DataFrame):
            raise SerializationError(
                f"This serializer only works with values of type dask.dataframe.DataFrame. You are trying to serialize a value of type '{type(value).__name__}'"
            )

        with tempfile.TemporaryDirectory() as tmp:
            value.to_parquet(
                os.path.join(tmp),
                engine=self._engine,
                compression=self._compression,
            )
            self._path_serializer.serialize(tmp, writer)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize the content of 'reader' into a Dask DataFrame backed by a series of CSV files."""
        from dask.dataframe import read_parquet
        from pandas.errors import EmptyDataError

        path = self._path_serializer.deserialize(reader)

        try:
            return read_parquet(
                path,
                engine=self._engine,
            )
        except EmptyDataError as e:
            raise DeserializationError(e)

    @property
    def extension(self) -> str:
        """Extension to use for files generated by this serializer."""
        return self._path_serializer.extension
