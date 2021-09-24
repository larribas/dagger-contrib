"""DataFrame fixtures."""

import pandas as pd
import pytest


@pytest.fixture
def star_wars_dataframe():
    """Return a DataFrame with information about a few Star Wars movies."""
    return pd.DataFrame(
        {
            "Title": [
                "Star Wars: Episode IV – A New Hope",
                "Star Wars: Episode V – The Empire Strikes Back",
                "Star Wars: Episode VI – Return of the Jedi",
            ],
            "Released": [
                "1977-05-25",
                "1980-05-21",
                "1983-05-25",
            ],
            "Director": [
                "George Lucas",
                "Irvin Kershner",
                "Richard Marquand",
            ],
            "RunningTime": [
                121,
                124,
                132,
            ],
        }
    )
