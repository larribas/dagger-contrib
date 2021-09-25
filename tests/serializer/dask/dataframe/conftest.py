"""DataFrame fixtures."""

import numpy as np
import pandas as pd
import pytest
from dask.dataframe import from_pandas


@pytest.fixture
def df_with_multiple_partitions(records=10000):
    """Return a DataFrame with information about a few Star Wars movies."""
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(records, 4)), columns=list("ABCD")
    )
    return from_pandas(df, npartitions=5).reset_index()
