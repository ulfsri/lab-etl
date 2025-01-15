import csv
import json

import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.parser import parse
from pyarrow import csv as pacsv

from labetl.util import detect_encoding, get_hash, set_metadata


def load_mcc_data(path: str) -> pa.Table:
    """Load an MCC file into a pyarrow.Table with metadata.

    Args:
        path (str): Path to the MCC file.

    Returns:
        pyarrow.Table: Table containing data and metadata from the MCC file.
    """
    # Determine file encoding using python-magic
    encoding = detect_encoding(path)

    # Find header information
    i, header, delimiter = find_mcc_header(path, encoding)

    # Split header into column names and units
    cols, units = split_mcc_header(header)

    # Configure options for reading CSV
    read_opts = pacsv.ReadOptions(encoding=encoding, column_names=cols, skip_rows=i + 2)
    parse_opts = pacsv.ParseOptions(delimiter=delimiter)

    # Read CSV data into an Arrow Table
    table = pacsv.read_csv(path, read_options=read_opts, parse_options=parse_opts)

    # Define column metadata
    col_meta = {col: {"unit": unit} for col, unit in zip(cols, units)}

    # Retrieve metadata from the MCC file
    tbl_meta = get_mcc_metadata(path, encoding, i)

    # Store metadata in the table
    table = set_metadata(
        table, col_meta=col_meta, tbl_meta={"file_metadata": tbl_meta, "type": "MCC"}
    )

    return table


def get_mcc_metadata(
    path: str, encoding: str, header_end: int
) -> dict[str, str | float | dict[str, str | float]]:
    """
    Get the metadata of an MCC file.

    Args:
        path (str): The path to the MCC file.
        encoding (str): The encoding of the file.
        header_end (int): The index of the last line of the header in the file.

    Returns:
        dict[str, str | float | dict[str, str | float]]: A dictionary with the metadata of the MCC file.
    """
    metadata: dict[str, str | float | dict[str, str | float]] = {}

    # Generate file hash for metadata
    file_hash = get_hash(path)

    # Read file and extract metadata
    with open(path, "r", encoding=encoding) as file:
        lines = file.readlines()
        for i, line in enumerate(lines):
            if i > header_end - 1:
                break
            key, value = line.split(":", 1)
            key = key.strip().lower().replace(" ", "_")
            value = value.strip(", \n\t")

            meta_val: str | float | dict[str, str | float]

            # Attempt to convert value to int or float
            try:
                meta_val = int(value)
            except ValueError:
                try:
                    meta_val = float(value)
                except ValueError:
                    try:
                        meta_val = {"date": parse(value).isoformat()}
                    except ValueError:
                        meta_val = value

            # Unit handling
            unit_mapping = {
                "(mg)": "mg",
                "(c/s)": "°C/s",
                "(c)": "°C",
                "(s)": "s",
                "(cc/min)": "ml/min",
            }
            for unit_suffix, unit in unit_mapping.items():
                if key.endswith(unit_suffix):
                    key = key[: -len(unit_suffix)]
                    meta_val = {"value": meta_val, "unit": unit}
                    break

            # Specific key adjustments
            if "calibration_file" in key:
                meta_val = {"file": meta_val}
            elif "t_correction_coefficients" in key:
                metadata.setdefault("temperature_calibration", {}).update(
                    {
                        "coefficients": [
                            float(x) for x in meta_val.replace("\t", ",").split(",")
                        ]
                    }
                )
                continue

            key_mapping = {
                "sample_weight": "sample_mass",
                "combustor_temp": "combustor_temperature",
                "calibration_file": "temperature_calibration",
            }
            key = key_mapping.get(key, key)
            metadata[key.strip(" _")] = meta_val

    # Add file hash to metadata
    metadata["file_hash"] = {
        "file": path.split("/")[-1],
        "method": "BLAKE2b",
        "hash": file_hash,
    }

    return metadata


def find_mcc_header(path: str, encoding: str) -> tuple[int, list[str], str]:
    """
    Find the header of the MCC file.

    Args:
        path (str): The path to the MCC file.
        encoding (str): The encoding of the file.

    Returns:
        Tuple[int, List[str], str]: A tuple with the index of the last line of the header,
            the header itself, and the delimiter used in the file.
    """
    try:
        with open(path, "r", encoding=encoding) as file:
            sample = file.read()
            delimiter = csv.Sniffer().sniff(sample).delimiter
            file.seek(0)
            reader = csv.reader(file, delimiter=delimiter)
            for i, line in enumerate(reader):
                if not line:  # skip empty lines
                    continue
                if line[0].startswith("*"):  # column names start with *
                    header = next(reader)
                    return i, header, delimiter
    except Exception as e:
        raise ValueError(f"An error occurred while reading the file: {e}")

    raise ValueError("Header not found in the MCC file.")


def split_mcc_header(header: list[str]) -> tuple[list[str], list[str | None]]:
    """
    Split the header into column names and units.

    Args:
        header (list[str]): The header for columns of the MCC file.

    Returns:
        tuple[list[str], list[str | None]]: A tuple with two lists. The first list
            contains the column names and the second list contains the units. If
            no unit is present for a column, the corresponding element in the list
            is None.
    """
    mapping = {"C": "°C", "/m": "1/m", "sec": "s", "cc/min": "ml/min", "C/s": "°C/s"}

    def split_col_unit(col: str) -> tuple[str, str | None]:
        if " (" in col:
            col_name, unit = col.split(" (", 1)
            col_name = col_name.strip().lower().replace(" ", "_")
            unit = unit.rstrip(")").strip()
            unit = mapping.get(unit, unit)
            return col_name, unit
        else:
            col_name = col.strip().lower().replace(" ", "_")
            return col_name, None

    cols, units = zip(*[split_col_unit(col) for col in header])
    return list(cols), list(units)


if __name__ == "__main__":
    path = "tests/test_files/MCC/Hemp_Sheet_MCC_30K_min_220112_R1.txt"
    df = load_mcc_data(path)
    metadata = {
        k.decode("utf-8"): json.loads(v.decode("utf-8"))
        if isinstance(v, bytes) and v.startswith(b"{")
        else v.decode("utf-8")
        for k, v in df.schema.metadata.items()
    }
    pq.write_table(
        df,
        "tests/test_files/MCC/Hemp_Sheet_MCC_30K_min_220112_R1.parquet",
        compression="snappy",
    )
