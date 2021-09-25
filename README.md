# Dagger Contrib

This repository contains extensions and experiments using the [`py-dagger` library](https://github.com/larribas/dagger)


![Python Versions Supported](https://img.shields.io/badge/python-3.8+-blue.svg)
[![Latest PyPI version](https://badge.fury.io/py/py-dagger-contrib.svg)](https://badge.fury.io/py/py-dagger-contrib)
[![Test Coverage (Codecov)](https://codecov.io/gh/larribas/dagger-contrib/branch/main/graph/badge.svg?token=fKU68xYUm8)](https://codecov.io/gh/larribas/dagger-contrib)
![Continuous Integration](https://github.com/larribas/dagger-contrib/actions/workflows/continuous-integration.yaml/badge.svg)


---

## Extensions

- `dagger_contrib.serializer`
    * `AsYAML` - Serializes primitive data types using [YAML](https://yaml.org/spec/).
    * `path` - Serializes local files or directories given their path name.
        - `AsTar` - As tarfiles with optional compression.
    * `pandas.dataframe` - Serializes [Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
        - `AsCSV` - As CSV files.
        - `AsParquet` - As Parquet files.
    * `dask.dataframe` - Serializes [Dask DataFrames](https://docs.dask.org/en/latest/dataframe.html).
        - `AsCSV` - As a directory containing multiple partitioned CSV files.
        - `AsParquet` - As a directory containing multiple partitioned Parquet files.


## Installation

_Dagger Contrib_ is published to the Python Package Index (PyPI) under the name `py-dagger-contrib`. To install it, you can simply run:

```
pip install py-dagger-contrib
```

### Extras

Many of the packages require extra dependencies. You can install those on your own, or via

```
pip install py-dagger-contrib[pandas]
```

Where `pandas` could also be `dask`, `yaml` or `all`.
