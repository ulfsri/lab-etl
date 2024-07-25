import zipfile
import struct
from datetime import datetime, timezone
import pyarrow as pa
import re
from itertools import tee, zip_longest
import polars as pl
from polars.exceptions import ShapeError

END_FIELD = rb"\x01\x00\x00\x00\x02\x00\x01\x00\x00"
TYPE_PREFIX = rb"\x17\xfc\xff\xff"
TYPE_SEPARATOR = rb"\x80\x01"
END_TABLE = rb"\x18\xfc\xff\xff\x03"
TABLE_SEPARATOR = (
    rb"\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff\x1a\x80\x01\x01\x80\x02\x00\x00"
)
column_map = {
    "8d": "Time",
    "8e": "Temperature",
    "9c": "DSC",
    "9e": "Purge Flow",
    "90": "Protective Flow",
    "87": "Sample Mass",
    "30": "Furnace Temperature",
    "32": "Furnace Power",
    "33": "HFoilTemp",
    "34": "uCModule",
    "35": "Env Pressure",
    "36": "Env Accel (x)",
    "37": "Env Accel (y)",
    "38": "Env Accel (z)",
}


def load_ngb_data(path: str) -> pa.Table:
    """Load a STA file and store metadata in the PyArrow table.

    Args:
        path (str): The path to the STA file.

    Returns:
        pyarrow.Table: The table with the data from the STA file and metadata.
    """
    meta = get_sta_metadata(path)


def get_sta_metadata(path: str) -> dict[str, str | float | dict[str, str | float]]:
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
            "Stage type": rb"\x3f\x08",
            "Temperature": rb"\x17\x0e",
            "Heating rate": rb"\x13\x0e",
            "Pts/min": rb"\x14\x0e",
            "Time": rb"\x15\x0e",
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
            "P0": rb"\x4f\x04",
            "P1": rb"\x50\x04",
            "P2": rb"\x51\x04",
            "P3": rb"\x52\x04",
            "P4": rb"\x53\x04",
            "P5": rb"\xc3\x04",
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
        "Instrument name": (rb"\x75\x17", rb"\x59\x10"),
        "Project name": (rb"\x72\x17", rb"\x3c\x08"),
        "Date/time of test": (rb"\x72\x17", rb"\x3e\x08"),
        "Lab name": (rb"\x72\x17", rb"\x34\x08"),
        "Operator name": (rb"\x72\x17", rb"\x35\x08"),
        "Crucible name": (rb"\x7e\x17", rb"\x40\x08"),
        "Remark": (rb"\x72\x17", rb"\x3d\x08"),
        "Furnace type": (rb"\x7a\x17", rb"\x40\x08"),
        "Carrier type": (rb"\x79\x17", rb"\x40\x08"),
        "Sample ID": (rb"\x30\x75", rb"\x98\x08"),
        "Sample name": (rb"\x30\x75", rb"\x40\x08"),
        "Sample mass": (rb"\x30\x75", rb"\x9e\x0c"),
        "Crucible mass": (rb"\x7e\x17", rb"\x9e\x0c"),
        "Material": (rb"\x30\x75", rb"\x62\x09"),
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

                    for table in stream_table:
                        for field_name, value in find_matches(table, patterns):
                            if field_name == "Date/time of test":
                                time = struct.unpack("<i", value[1])[0]
                                dt = datetime.fromtimestamp(
                                    time, tz=timezone.utc
                                ).strftime("%Y-%m-%d %H:%M:%S")
                                print(field_name + ": ", dt)
                            elif value[0] == b"\x1f":
                                print(
                                    field_name + ": ",
                                    value[1].decode("ascii", errors="ignore").strip(),
                                )
                            elif value[0] == b"\x05":  # double
                                print(
                                    field_name + ": ", struct.unpack("<d", value[1])[0]
                                )
                            else:
                                print(field_name + ": ", value[1])
                        for field_name, step_num, value in find_temp_prog(table):
                            print(f"{step_num}: {field_name}: {value}")
                        for field_name, value in find_cal_constants(table):
                            print(f"{field_name}: {value}")

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


if __name__ == "__main__":
    # path = "tests/test_files/STA/Hyundai_KM8K_Carpet_STA_N2_10K_240711_R3.ngb-ss3"
    path = "tests/test_files/STA/IBHS_Shingle_102-B-5-1_Sample_2_STA_N2_30K_240716_R1.ngb-ss3"
    load_ngb_data(path)
