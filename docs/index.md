# lab-etl

[![PyPI - Version](https://img.shields.io/pypi/v/lab-etl.svg)](https://pypi.org/project/lab-etl)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/lab-etl.svg)](https://pypi.org/project/lab-etl)

This repository contains the codebase for the ETL scripts for loading laboratory instrument data files into our database. In particular, data files from a variety of formats are converted to Apache Parquet files which provides a standardized interface for access and enforces schema. Of notable importance is the inclusion of metadata in these files. Metadata is extracted from the original test files and stored as JSON-like metadata within the Parquet files in either file-wide or column-specific, as appropriate. Depending on the type of file (from which type of instrument) the keys will be standardized for common fields. Additional metadata that may be intrument-specific will be stored as additional metadata but is not guaranteed to be standardized in any meaningful way. However, the names of these fields may be slightly altered to provide clarity to the user as to what they might represent.

Development currently focuses on files and instruments of interest to FSRI's Materials Properties Laboratory but as we integrate with external stakeholders, or have the time, additional instruments and filetypes will be added. Feel free to reach out if you have a particular need for some capability or submit a PR.

---

## Table of Contents

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install lab-etl
```

## License

`lab-etl` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
