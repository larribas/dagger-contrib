"""Serialize DataFrames as CSVs."""

from typing import Any, BinaryIO, Optional

from dagger import DeserializationError, SerializationError


class AsCSV:
    """
    Serializer implementation that uses CSV to serialize Pandas DataFrames.

    See Also
    --------
    - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    - https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
    """

    extension = "csv"

    def __init__(
        self,
        compression: Optional[str] = None,
    ):
        """
        Initialize a serializer that serializes DataFrame values as CSVs.

        Parameters
        ----------
        compression: str, optional
            The compression mode, which may be one of the following values: {"gzip", "bz2", "zip", "xz", None}
        """
        self._compression = compression

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a Pandas DataFrame as a CSV file."""
        import pandas as pd

        if not isinstance(value, pd.DataFrame):
            raise SerializationError(
                f"The pandas.AsCSV serializer only works with values of type pd.DataFrame. You are trying to serialize a value of type '{type(value).__name__}'"
            )

        value.to_csv(writer, compression=self._compression)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize a CSV into a DataFrame object."""
        import pandas as pd

        try:
            return pd.read_csv(reader, index_col=0, compression=self._compression)
        except pd.errors.EmptyDataError as e:
            raise DeserializationError(e)
        except UnicodeDecodeError as e:
            raise DeserializationError(
                f"We could not deserialize the CSV artifact. This may be happening because the file was originally serialized with a particular compression mode, but you're trying to deserialize it with compression=None. The original error is: {str(e)}"
            ) from e
