[![Tests](https://github.com/emorice/galp/actions/workflows/test.yml/badge.svg)](https://github.com/emorice/galp/actions/workflows/test.yml)
![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Femorice%2Fgalp%2Fmaster%2Fpyproject.toml)

> [!NOTE]
> Still experimental. Users are invited to try out and test the software, report bugs and
> give feedback, but features may be added or dropped and breaking interface and caching changes may still
> occur at any time.

# Incremental distributed python runner

Designed for data science pipelines. Write your code by wrapping select functions with `@galp.step`.
Run with `galp.run(..., store='/path/to/storage')` to execute your code in a distributed manner, with
each step's result being cached in the storage directory.

Draws inspiration from multiprocessing, joblib, dask, snakemake, nextflow, yet different from each of these.
