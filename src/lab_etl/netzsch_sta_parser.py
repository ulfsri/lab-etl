import csv
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv as pacsv
import json
from dateutil.parser import parse
import re
import hashlib
from lab_etl.util import set_metadata, detect_encoding

UNITS = (
    "/°C",
    "/°F",
    "/K",
    "/s",
    "/min",
    "/h",
    "/m",
    "/µV",
    "/mV",
    "/V",
    "/mA",
    "/A",
    "/mbar",
    "/mg",
)


def load_sta_data(path: str) -> pa.Table:
    """Load a STA file and store metadata in the PyArrow table.

    Args:
        path (str): The path to the STA file.

    Returns:
        pyarrow.Table: The table with the data from the STA file and metadata.
    """
    try:
        # Determine file encoding
        encoding = detect_encoding(path)

        # Find the header of the file
        i, header, delimiter = find_sta_header(path, encoding)

        # Split the header into column names and units
        cols, units = split_sta_header(header)

        # Retrieve STA metadata
        sta_meta = get_sta_metadata(path, encoding, i)

        # Read the data from the file
        read_opts = pacsv.ReadOptions(
            encoding=encoding, column_names=cols, skip_rows=i + 1
        )
        parse_opts = pacsv.ParseOptions(delimiter=delimiter)
        data = pacsv.read_csv(path, read_options=read_opts, parse_options=parse_opts)

        # Store units in the column metadata
        col_meta = {col: {"unit": unit} for col, unit in zip(cols, units)}

        # Store the metadata of the file in the table metadata
        tbl_meta = sta_meta

        # Store metadata in the table
        data_meta = set_metadata(data, col_meta=col_meta, tbl_meta=tbl_meta)

        return data_meta

    except Exception as e:
        raise RuntimeError(f"An error occurred while loading the STA data: {e}")


def get_sta_metadata(
    path: str, encoding: str, header_end: int
) -> dict[str, str | float | dict[str, str | float]]:
    """
    Get the metadata of a STA file.

    TODO: Need to deal with range. See validation files as well because it has a different format.

    Args:
        path (str): The path to the STA file.
        encoding (str): The encoding of the file.
        header_end (int): The index of the last line of the header in the file.

    Returns:
        Dict[str, Union[str, float, Dict[str, Union[str, float]]]]: A dictionary with the metadata of the STA file.
    """
    metadata: dict[str, str | float | dict[str, str | float]] = {}

    # Hash the original file to store in metadata
    metadata["file_hash"] = generate_file_hash(path)

    with open(path, "r", encoding=encoding) as file:
        lines = file.readlines()
        for i, line in enumerate(lines):
            if i > header_end - 1:
                break
            if line.startswith("#"):
                key, value = process_metadata_line(line)
                metadata[key] = value

    return metadata


def generate_file_hash(path: str) -> dict[str, str]:
    """Generate a hash for the file at the given path.

    Args:
        path (str): The path to the file.

    Returns:
        dict[str, str]: A dictionary with the file name, hash method, and hash value.
    """
    with open(path, "rb") as file:
        file_hash = hashlib.blake2b(file.read()).hexdigest()
    return {"file": path.split("/")[-1], "method": "BLAKE2b", "hash": file_hash}


def process_metadata_line(line: str) -> tuple:
    """Process a line of metadata from a STA file.

    Args:
        line (str): A line of metadata from the STA file.

    Returns:
        tuple: A tuple with the key and value of the metadata.
    """
    key, value = line[1:].split(":", 1)
    key = key.strip()
    key = map_key(key)
    value = value.strip(", \n")
    meta_val = parse_metadata_value(key, value)

    # Remove units on keys
    if any(units in key for units in UNITS):
        key = key.split("/")[0]
        key = key.strip(" _")
    key = key.replace(" ", "_").lower()
    return key, meta_val


def map_key(key: str) -> str:
    """Map the key to a standard format."""
    key_mapping = {
        "exporttype": "export_type",
        "remark": "comment",
        "type_of_crucible": "crucible_type",
        "tempcal": "temperature_calibration",
        "sensitivity": "sensitivity_calibration",
        "corr._file": "correction_file",
        "ftype": "file_type",
        "mtype": "measurement_type",
        "corr._code": "correction_code",
        "exo": "exothermic",
        "separator": "delimiter",
        "date/time": "date_performed",
    }
    return key_mapping.get(key.lower().replace(" ", "_"), key)


def parse_metadata_value(key: str, value: str) -> str | float | dict[str, str | float]:
    """Parse the value of the metadata.

    Args:
        key (str): The key of the metadata.
        value (str): The value of the metadata.

    Returns:
        str | float | dict[str, str | float]: The parsed value of the metadata.
    """
    if "mfc" in key.lower() and value:
        return parse_mfc_value(value)
    if "crucible_type" in key.lower() and value:
        return parse_crucible_value(value)
    if "seg." in key.lower() and value:
        return parse_segment_value(key, value)
    if any(units in key for units in UNITS):
        return parse_unit_value(key, value)

    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            try:
                if key.lower() == "date_performed":
                    return parse_date(value, None, fuzzy=True)
                return parse_date(value, "date")
            except ValueError:
                return value


def parse_mfc_value(value: str) -> dict[str, str | float]:
    """Parse the value of a mass flow controller.

    Args:
        value (str): The value of the mass flow controller.

    Returns:
        dict[str, str | float]: A dictionary with the gas, range, and unit of the mass flow controller.
    """
    temp = value.replace(",", " ").split(" ")
    gas = temp[0]
    range_val = float(temp[1])
    unit = temp[2]
    return {"gas": gas, "range": range_val, "unit": unit}


def parse_crucible_value(value: str) -> dict[str, str | float]:
    """Parse the value of a crucible.

    Args:
        value (str): The value of the crucible.

    Returns:
        dict[str, str | float]: A dictionary with the material, volume, and extra information of the crucible.
    """
    temp = value.replace(",", " ").split(" ")
    material = temp[0]
    vol = float(temp[1])
    vol_unit = temp[2]
    extra = " ".join(temp[4:])
    return {
        "material": material,
        "volume": {"value": vol, "unit": vol_unit},
        "extra": extra,
    }


def parse_segment_value(key: str, value: str) -> dict[str, str | float]:
    """Parse the value of a segment.

    Args:
        key (str): The key of the metadata.
        value (str): The value of the metadata.

    Returns:
        dict[str, str | float]: A dictionary with the start temperature, end temperature, and heating rate or time of the segment.
    """
    key = key.replace("seg.", "segment")
    temps = re.split(r"/.*/", value)
    numeric = "0123456789-."
    numbers = []
    units = []
    for s in temps:
        for i, c in enumerate(s):
            if c not in numeric:
                break
        numbers.append(s[:i])
        units.append(s[i:].lstrip())
    time_or_hr = "/".join(re.split(r"/", value)[1:-1])
    if re.search(r"[0-9]*\.[0-9]+\(.*\)", time_or_hr):
        hr_units = time_or_hr.replace("(", " ").replace(")", " ").strip().split(" ")
        return {
            "start_temperature": {"value": float(numbers[0]), "unit": units[0]},
            "end_temperature": {"value": float(numbers[1]), "unit": units[1]},
            "heating_rate": {"value": float(hr_units[0]), "unit": hr_units[1]},
        }
    if re.search(r"[0-9]+:[0-9]+", time_or_hr):
        return {
            "start_temperature": {"value": float(numbers[0]), "unit": units[0]},
            "end_temperature": {"value": float(numbers[1]), "unit": units[1]},
            "time": time_or_hr,
        }
    return {}


def parse_unit_value(key: str, value: str) -> dict[str, str | float]:
    """Parse the value of a metadata with units.

    Args:
        key (str): The key of the metadata.
        value (str): The value of the metadata.

    Returns:
        dict[str, str | float]: A dictionary with the value and unit of the metadata.
    """
    temp = value.replace(",", " ").split(" ")
    value = float(temp[0])
    unit = key.split("/")[-1]
    return {"value": value, "unit": unit}


def parse_date(value: str, key: str, fuzzy: bool = False) -> dict[str, str] | str:
    """Parse the date value of the metadata.

    Args:
        value (str): The value of the metadata.
        key (str): The key of the metadata.
        fuzzy (bool): Whether to allow fuzzy parsing. Default is False.

    Returns:
        dict[str, str] | str: A dictionary with the date value of the metadata.
    """
    if key:
        return {key: parse(value, fuzzy=fuzzy).isoformat()}
    return parse(value, fuzzy=fuzzy).isoformat()


def find_sta_header(path: str, encoding: str = "utf-8") -> tuple[int, list[str], str]:
    """Find the header of the STA file.

    Args:
        path (str): The path to the STA file.
        encoding (str): The encoding of the file. Default is 'utf-8'.

    Returns:
        tuple: A tuple with the index of the last line of the header,
               the header itself, and the delimiter used in the file.
    """
    try:
        with open(path, "r", encoding=encoding) as file:
            sample = file.read()
            file.seek(0)
            delimiter = csv.Sniffer().sniff(sample).delimiter
            reader = csv.reader(file, delimiter=delimiter)

            for i, line in enumerate(reader):
                if not line:  # skip empty lines
                    continue
                if line[0].startswith("##"):  # column names start with ##
                    line[0] = line[0][2:]  # cut-off the comment characters
                    header = line
                    return (i, header, delimiter)

        raise ValueError("Header with '##' not found in the file.")

    except Exception as e:
        raise RuntimeError(f"An error occurred while processing the file: {e}")


def split_sta_header(header: list[str]) -> tuple[list[str], list[str | None]]:
    """Split the header into column names and units.

    Args:
        header (list[str]): The header for columns of the STA file.

    Returns:
        tuple[list[str], list[str | None]]: A tuple with two lists. The first list
            contains the column names and the second list contains the units. If
            no unit is present for a column, the corresponding element in the list
            is None.
    """
    cols = []
    units = []
    mapping = {"temp.": "temperature", "sensit.": "sensitivity"}

    for col in header:
        if "/" in col:
            col_name, unit = col.split("/", 1)  # Split at the first instance of "/"
            cols.append(col_name.strip().lower().replace(" ", "_"))
            units.append(unit.strip(" ()"))
        else:
            cols.append(col.strip().lower().replace(" ", "_"))
            units.append(None)

    # Apply mapping and remove content in parentheses
    cols = [
        mapping.get(re.sub(r"\([^)]*\)", "", col), re.sub(r"\([^)]*\)", "", col))
        for col in cols
    ]

    return cols, units


if __name__ == "__main__":
    # path = "tests/test_files/STA/DF_FILED_VAL_STA_N2_10K_240211_R1.csv"
    path = "tests/test_files/STA/DF_FILED_DES_STA_N2_10K_231028_R1.csv"
    df = load_sta_data(path)
    metadata = {
        k.decode("utf-8"): json.loads(v.decode("utf-8"))
        if isinstance(v, bytes) and v.startswith(b"{")
        else v.decode("utf-8")
        for k, v in df.schema.metadata.items()
    }
    pq.write_table(
        df,
        "tests/test_files/STA/DF_FILED_DES_STA_N2_10K_231028_R1.parquet",
        compression="snappy",
    )
