import csv
import magic
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv as pacsv
import json
from dateutil.parser import parse
import re
import hashlib
from lab_etl.util import set_metadata


def load_sta_data(path: str) -> pa.Table:
    """Load a STA file and store metadata in the pyarrow table.

    Args:
        path (str): The path to the STA file.

    Returns:
        pyarrow.Table: The table with the data from the STA file and metadata.
    """
    f = magic.Magic(mime_encoding=True)
    encoding = f.from_file(path)  # get the encoding of the file
    i, header, delimiter = find_sta_header(
        path, encoding
    )  # find the header of the file
    cols, units = split_sta_header(
        header
    )  # split the header into column names and units
    sta_meta = get_sta_metadata(path, encoding, i)

    read_opts = pacsv.ReadOptions(encoding=encoding, column_names=cols, skip_rows=i + 1)
    parse_opts = pacsv.ParseOptions(delimiter=delimiter)
    data = pacsv.read_csv(
        path, read_options=read_opts, parse_options=parse_opts
    )  # read the data from the file

    col_meta = {
        col: {"unit": unit} for col, unit in zip(cols, units)
    }  # store units in the column metadata
    tbl_meta = sta_meta  # store the metadata of the file in the table metadata
    data_meta = set_metadata(
        data, col_meta=col_meta, tbl_meta=tbl_meta
    )  # store metadata in the table
    return data_meta


def get_sta_metadata(
    path: str, encoding: str, header_end: int
) -> dict[str, str | float | dict[str, str | float]]:
    """Get the metadata of a STA file.

    TODO: Need to deal with range. See validation files as well because it has a different format.

    Args:
        path (str): The path to the STA file.
        encoding (str): The encoding of the file.
        header_loc (int): The index of the last line of the header in the file.

    Returns:
        dict[str, str]: A dictionary with the metadata of the STA file.
    """
    metadata: dict[str, str | float | dict[str, str | float]] = {}
    with open(path, "rb") as c:
        hash = hashlib.blake2b(c.read()).hexdigest()  # hash the original file to store in metadata
    with open(path, "r", encoding=encoding) as c:
        lines = c.readlines()
        for i, line in enumerate(lines):
            if i > header_end - 1:
                break
            if line.startswith("#"):
                key, value = line[1:].split(":", 1)
                key = (
                    key.strip().lower().replace(" ", "_")
                )  # convert key to lowercase and replace spaces with underscores (snake_case)
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
                }
                key = key_mapping.get(key, key)
                value = value.strip(", \n")
                if "mfc" in key and value:
                    temp = value.replace(",", " ").split(" ")
                    gas = temp[0]
                    range = float(temp[1])
                    unit = temp[2]
                    meta_val = {"gas": gas, "range": range, "unit": unit}
                elif (
                    "crucible_type" in key and value
                ):  # TODO: check if this is valid and will work for all possible crucible types
                    temp = value.replace(",", " ").split(" ")
                    material = temp[0]
                    vol = float(temp[1])
                    vol_unit = temp[2]
                    extra = " ".join(temp[4:])
                    meta_val = {
                        "material": material,
                        "volume": {"value": vol, "unit": vol_unit},
                        "extra": extra,
                    }
                elif "seg." in key and value:
                    key = key.replace("seg.", "segment")
                    temps = re.split(r"/.*/", value)
                    numeric = (
                        "0123456789-."  # list of numeric strings that we will accept
                    )
                    numbers = []
                    units = []
                    for s in temps:  # split the string into numbers and units
                        for i, c in enumerate(s):
                            if c not in numeric:
                                break
                        numbers.append(s[:i])
                        units.append(s[i:].lstrip())
                    time_or_hr = "/".join(re.split(r"/", value)[1:-1])
                    if bool(
                        re.search(r"[0-9]*\.[0-9]+\(.*\)", time_or_hr)
                    ):  # heating rate
                        hr_units = (
                            time_or_hr.replace("(", " ")
                            .replace(")", " ")
                            .strip()
                            .split(" ")
                        )
                        meta_val = {
                            "start_temperature": {
                                "value": float(numbers[0]),
                                "unit": units[0],
                            },
                            "end_temperature": {
                                "value": float(numbers[1]),
                                "unit": units[1],
                            },
                            "heating_rate": {
                                "value": float(hr_units[0]),
                                "unit": hr_units[1],
                            },
                        }
                    elif bool(re.search(r"[0-9]+:[0-9]+", time_or_hr)):  # time
                        meta_val = {
                            "start_temperature": {
                                "value": float(numbers[0]),
                                "unit": units[0],
                            },
                            "end_temperature": {
                                "value": float(numbers[1]),
                                "unit": units[1],
                            },
                            "time": time_or_hr,
                        }
                else:
                    try:
                        meta_val = int(value)
                    except ValueError:
                        try:
                            meta_val = float(value)
                        except ValueError:
                            try:
                                if key == "date/time":
                                    key = "date_performed"
                                    meta_val = (
                                        parse(value, fuzzy=True).isoformat()
                                    )  # DATA/TIME value requires fuzzy to parse
                                else:
                                    meta_val = {
                                        "date": parse(value).isoformat()
                                    }  # Others just true to parse as normal
                            except ValueError:
                                meta_val = value
                                pass
                if key.endswith(
                    "/mg"
                ):  # check if the key ends with "/mg" and add it to value
                    key = key[:-4]  # remove "/mg" from the end of the key
                    meta_val = {
                        "value": meta_val,
                        "unit": "mg",
                    }  # put the value in a tuple with "/mg"
                elif key.endswith(
                    "/µv"
                ):  # check if the key ends with "µL" and add it to value
                    key = key[:-4]  # remove "µL" from the end of the key
                    meta_val = {
                        "value": meta_val,
                        "unit": "µV",
                    }  # put the value in a tuple with "µL"
                metadata[key.strip()] = meta_val
    metadata["file_hash"] = {
        "file": path.split("/")[-1],
        "method": "BLAKE2b",
        "hash": hash,
    }
    return metadata


def find_sta_header(path: str, encoding: str) -> tuple[int, list[str], str]:
    """Find the header of the STA file.

    Args:
        path (str): The path to the STA file.
        encoding (str): The encoding of the file.

    Returns:
        tuple[int, list[str]]: A tuple with the index of the last line of the header
            and the header itself.
    """
    with open(path, "r", encoding=encoding) as c:
        delimiter = csv.Sniffer().sniff(c.read()).delimiter
        c.seek(0)
        reader = csv.reader(c, delimiter=delimiter)
        for i, line in enumerate(reader):
            if line == []:  # skip empty lines
                continue
            if line[0].startswith("##"):  # column names start with ##
                line[0] = line[0][2:]  # cut-off the comment characters
                header = line
                break
    return (i, header, delimiter)


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
    cols: list[str] = []
    units: list[str | None] = []
    mapping = {"Temp.": "Temperature", "Sensit.": "Sensitivity"}
    for col in header:
        if "/" in col:
            col, unit = col.split("/", 1)  # Split at the first instance of "/"
            cols.append(col.strip())
            units.append(unit.strip())
        else:
            cols.append(col.strip())
            units.append(None)
    for i in range(len(cols)):
        if cols[i] in mapping:
            cols[i] = mapping[cols[i]]
        cols[i] = re.sub(
            r"\([^)]*\)", "", cols[i]
        )  # Remove any content in parenthesis, i.e. (subtr.2)
    return (cols, units)


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
