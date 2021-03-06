"""Serialize DataFrames as Parquet files (https://parquet.apache.org/)."""

from typing import Any, BinaryIO, Optional

from dagger import SerializationError


class AsParquet:
    """
    Serializer implementation that uses Parquet to serialize Pandas DataFrames.

    See Also
    --------
    - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html
    - https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_parquet.html
    - https://parquet.apache.org/
    """

    EXTENSIONS_BY_COMPRESSION = {
        "gzip": "parquet.gz",
        "snappy": "parquet.snappy",
        "brotli": "parquet.br",
    }

    def __init__(
        self,
        engine: str = "auto",
        compression: Optional[str] = "snappy",
    ):
        """
        Initialize a serializer that serializes DataFrame values using the Parquet format.

        Parameters
        ----------
        engine: str, default="auto"
            Parquet library to use.

        compression: str, optional, default="snappy"
            The compression mode, which may be one of the following values: {"snappy", "gzip", "brotli", None}
        """
        self._engine = engine
        self._compression = compression

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a Pandas DataFrame as a CSV file."""
        import pandas as pd

        if not isinstance(value, pd.DataFrame):
            raise SerializationError(
                f"This serializer only works with values of type pd.DataFrame. You are trying to serialize a value of type '{type(value).__name__}'"
            )

        value.to_parquet(writer, engine=self._engine, compression=self._compression)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize a Parquet file into a DataFrame object."""
        import pandas as pd

        return pd.read_parquet(reader, engine=self._engine)

    @property
    def extension(self) -> str:
        """Extension to use for files generated by this serializer."""
        return self.EXTENSIONS_BY_COMPRESSION.get(self._compression or "", "parquet")
