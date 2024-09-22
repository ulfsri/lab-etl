import zipfile
import struct
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
import re
from itertools import tee, zip_longest
import polars as pl
from polars.exceptions import ShapeError
from lab_etl.util import get_hash, set_metadata

END_FIELD = rb"\x01\x00\x00\x00\x02\x00\x01\x00\x00"
TYPE_PREFIX = rb"\x17\xfc\xff\xff"
TYPE_SEPARATOR = rb"\x80\x01"
END_TABLE = rb"\x18\xfc\xff\xff\x03"
TABLE_SEPARATOR = (
    rb"\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x1a\x80\x01\x01\x80\x02\x00\x00"
)
column_map = {
    "8d": "time",
    "8e": "temperature",
    "9c": "dsc",
    "9e": "purge_flow",
    "90": "protective_flow",
    "87": "sample_mass",
    "30": "furnace_temperature",
    "32": "furnace_power",
    "33": "h_foil_temp",
    "34": "uc_module",
    "35": "env_pressure",
    "36": "env_accel_x",
    "37": "env_accel_y",
    "38": "env_accel_z",
}


def load_ngb_data(path: str) -> pa.Table:
    """Load a STA file and store metadata in the PyArrow table.

    Args:
        path (str): The path to the STA file.

    Returns:
        pyarrow.Table: The table with the data from the STA file and metadata.
    """
    meta, data = get_sta_data(path)
    file_hash = get_hash(path)
    meta["file_hash"] = {
        "file": path.split("/")[-1],
        "method": "BLAKE2b",
        "hash": file_hash,
    }
    data = set_metadata(data, tbl_meta={"file_metadata": meta, "type": "STA"})

    return data


def get_sta_data(path: str) -> dict[str, str | float | dict[str, str | float]]:
    def find_matches(table: bytes, patterns: dict[bytes, str]):
        for field_name, pos in patterns.items():
            category, field = pos
            pattern = (
                category
                + rb".+?"
                + field
                + rb".+?"
                + TYPE_PREFIX
                + rb"(.+?)"  # Find the data type
                + TYPE_SEPARATOR
                + rb"(.+?)"  # Find the value
                + END_FIELD
            )
            matches = re.findall(pattern, table, flags=re.DOTALL)
            for match in matches:
                yield field_name, match

    def find_temp_prog(table: bytes):
        CATEGORY = b"\x0c\x2b"
        patterns = {
            "stage_type": rb"\x3f\x08",
            "temperature": rb"\x17\x0e",
            "heating_rate": rb"\x13\x0e",
            "acquisition_rate": rb"\x14\x0e",
            "time": rb"\x15\x0e",
        }
        if CATEGORY in table:
            step_num = table[0:2]
            for field_name, pos in patterns.items():
                pattern = (
                    pos
                    + rb".+?"
                    + TYPE_PREFIX
                    + rb"(.+?)"  # Find the data type
                    + TYPE_SEPARATOR
                    + rb"(.+?)"  # Find the value
                    + END_FIELD
                )
                match = re.search(pattern, table, flags=re.DOTALL)
                if match:
                    yield field_name, step_num, match.groups()

    def find_cal_constants(table: bytes):
        CATEGORY = b"\xf5\x01"
        patterns = {
            "p0": rb"\x4f\x04",
            "p1": rb"\x50\x04",
            "p2": rb"\x51\x04",
            "p3": rb"\x52\x04",
            "p4": rb"\x53\x04",
            "p5": rb"\xc3\x04",
        }
        if CATEGORY in table:
            for field_name, pos in patterns.items():
                pattern = (
                    pos
                    + rb".+?"
                    + TYPE_PREFIX
                    + rb"(.+?)"  # Find the data type
                    + TYPE_SEPARATOR
                    + rb"(.+?)"  # Find the value
                    + END_FIELD
                )
                match = re.search(pattern, table, flags=re.DOTALL)
                if match:
                    yield field_name, match.groups()

    patterns = {  # cateogry, field
        "instrument": (rb"\x75\x17", rb"\x59\x10"),
        "project": (rb"\x72\x17", rb"\x3c\x08"),
        "date_performed": (rb"\x72\x17", rb"\x3e\x08"),
        "lab": (rb"\x72\x17", rb"\x34\x08"),
        "operator": (rb"\x72\x17", rb"\x35\x08"),
        "crucible_type": (rb"\x7e\x17", rb"\x40\x08"),
        "comment": (rb"\x72\x17", rb"\x3d\x08"),
        "furnace_type": (rb"\x7a\x17", rb"\x40\x08"),
        "carrier_type": (rb"\x79\x17", rb"\x40\x08"),
        "sample_id": (rb"\x30\x75", rb"\x98\x08"),
        "sample_name": (rb"\x30\x75", rb"\x40\x08"),
        "sample_mass": (rb"\x30\x75", rb"\x9e\x0c"),
        "crucible_mass": (rb"\x7e\x17", rb"\x9e\x0c"),
        "material": (rb"\x30\x75", rb"\x62\x09"),
    }

    with zipfile.ZipFile(path, "r") as z:
        for file in z.filelist:
            if file.filename == "Streams/stream_1.table":
                with z.open(file.filename) as stream:
                    stream_table = stream.read()

                    # Split into tables
                    indices = [
                        match.start() - 2
                        for match in re.finditer(TABLE_SEPARATOR, stream_table)
                    ]
                    start, end = tee(indices)
                    next(end)
                    stream_table = [
                        stream_table[i:j] for i, j in zip_longest(start, end)
                    ]

                    metadata = {}
                    for table in stream_table:
                        for field_name, value in find_matches(table, patterns):
                            if field_name == "date_performed":
                                time = struct.unpack("<i", value[1])[0]
                                dt = datetime.fromtimestamp(
                                    time, tz=timezone.utc
                                ).isoformat()
                                metadata[field_name] = dt
                            elif value[0] == b"\x1f":
                                metadata[field_name] = (
                                    value[1][4:]
                                    .decode("utf-8", errors="ignore")
                                    .strip()
                                    .replace("\x00", "")
                                )
                            elif value[0] == b"\x05":  # double
                                metadata[field_name] = struct.unpack("<d", value[1])[0]
                            else:
                                metadata[field_name] = value[1]
                        for field_name, step_num, value in find_temp_prog(table):
                            if value[0] == b"\x03":
                                value = struct.unpack("<i", value[1])[0]
                            elif value[0] == b"\x04":
                                value = struct.unpack("<f", value[1])[0]
                            else:
                                value = value[1]

                            metadata.setdefault("temperature_program", {}).update(
                                {
                                    f'step_{step_num.decode("ascii")[0]}': {
                                        **metadata.setdefault(
                                            "temperature_program", {}
                                        ).get(
                                            f'step_{step_num.decode("ascii")[0]}', {}
                                        ),
                                        field_name: value,
                                    }
                                }
                            )
                        for field_name, value in find_cal_constants(table):
                            if value[0] == b"\x04":
                                metadata.setdefault("calibration_constants", {}).update(
                                    {field_name: struct.unpack("<f", value[1])[0]}
                                )

            if file.filename == "Streams/stream_2.table":  # Primary data table
                with z.open(file.filename) as stream:
                    stream_table = stream.read()

                    # Split into tables
                    indices = [
                        match.start() - 2
                        for match in re.finditer(TABLE_SEPARATOR, stream_table)
                    ]
                    start, end = tee(indices)
                    next(end)
                    stream_table = [
                        stream_table[i:j] for i, j in zip_longest(start, end)
                    ]
                    output = []
                    output_polars = pl.DataFrame()
                    for table in stream_table:
                        if table[1:2] == b"\x17":  # header
                            title = table[0:1].hex()
                            title = column_map.get(title, title)
                            if len(output) > 1:
                                try:
                                    output_polars = output_polars.with_columns(
                                        pl.Series(name=title, values=output)
                                    )
                                except ShapeError:
                                    print("error")
                            output = []

                        if table[1:2] == b"\x75":  # data
                            START_DATA = b"\xa0\x01"
                            END_DATA = b"\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x18\xfc\xff\xff\x03\x80\x01"
                            start_data = table.find(START_DATA) + 6
                            data = table[start_data:]
                            end_data = data.find(END_DATA)
                            data = data[:end_data]
                            data_type = table[start_data - 7 : start_data - 6]
                            if data_type == b"\x05":
                                n = 8
                                data_table = [
                                    struct.unpack("<d", data[i : i + n])[0]
                                    for i in range(0, len(data), n)
                                ]
                                output.extend(data_table)
                            elif data_type == b"\x04":
                                n = 4
                                data_table = [
                                    struct.unpack("<f", data[i : i + n])[0]
                                    for i in range(0, len(data), n)
                                ]
                                output.extend(data_table)

            if file.filename == "Streams/stream_3.table":
                with z.open(file.filename) as stream:
                    stream_table = stream.read()

                    # Split into tables
                    indices = [
                        match.start() - 2
                        for match in re.finditer(TABLE_SEPARATOR, stream_table)
                    ]
                    start, end = tee(indices)
                    next(end)
                    stream_table = [
                        stream_table[i:j] for i, j in zip_longest(start, end)
                    ]
                    output = []
                    for table in stream_table:
                        if table[22:25] == b"\x80\x22\x2b":  # header
                            title = table[0:1].hex()
                            title = column_map.get(title, title)
                            output = []
                        if table[1:2] == b"\x75":  # data
                            START_DATA = b"\xa0\x01"
                            END_DATA = b"\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x18\xfc\xff\xff\x03\x80\x01"
                            start_data = table.find(START_DATA) + 6
                            data = table[start_data:]
                            end_data = data.find(END_DATA)
                            data = data[:end_data]
                            data_type = table[start_data - 7 : start_data - 6]
                            if data_type == b"\x05":
                                n = 8
                                data_table = [
                                    struct.unpack("<d", data[i : i + n])[0]
                                    for i in range(0, len(data), n)
                                ]
                                output.extend(data_table)
                            elif data_type == b"\x04":
                                n = 4
                                data_table = [
                                    struct.unpack("<f", data[i : i + n])[0]
                                    for i in range(0, len(data), n)
                                ]
                                output.extend(data_table)
                            try:
                                output_polars = output_polars.with_columns(
                                    pl.Series(name=title, values=output)
                                )
                            except ShapeError:
                                pass
                    output_polars.write_csv("output.csv")
    return metadata, output_polars.to_arrow()


if __name__ == "__main__":
    # path = "tests/test_files/STA/Hyundai_KM8K_Carpet_STA_N2_10K_240711_R3.ngb-ss3"
    path = "tests/test_files/STA/IBHS_Shingle_102-B-5-1_Sample_2_STA_N2_30K_240716_R1.ngb-ss3"
    table = load_ngb_data(path)
    pq.write_table(
        table,
        "tests/test_files/STA/IBHS_Shingle_102-B-5-1_Sample_2_STA_N2_30K_240716_R1.parquet",
        compression="snappy",
    )
