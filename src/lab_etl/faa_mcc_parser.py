import csv
import magic
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv as pacsv
import json
from dateutil.parser import parse
import hashlib


def load_mcc_data(path: str) -> pa.Table:
    """Load a MCC file and store metadata in the pyarrow table.

    Args:
        path (str): The path to the MCC file.

    Returns:
        pyarrow.Table: The table with the data from the MCC file and metadata.
    """
    f = magic.Magic(mime_encoding=True)
    encoding = f.from_file(path)  # get the encoding of the file
    i, header, delimiter = find_mcc_header(
        path, encoding
    )  # find the header of the file
    cols, units = split_mcc_header(
        header
    )  # split the header into column names and units
    sta_meta = get_mcc_metadata(path, encoding, i)

    read_opts = pacsv.ReadOptions(encoding=encoding, column_names=cols, skip_rows=i + 2)
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


def get_mcc_metadata(
    path: str, encoding: str, header_end: int
) -> dict[str, str | float | dict[str, str | float]]:
    """Get the metadata of a STA file.

    Args:
        path (str): The path to the MCC file.
        encoding (str): The encoding of the file.
        header_loc (int): The index of the last line of the header in the file.

    Returns:
        dict[str, str]: A dictionary with the metadata of the MCC file.
    """
    metadata: dict[str, str | float | dict[str, str | float]] = {}
    with open(path, "rb") as c:
        hash = hashlib.blake2b(c.read()).hexdigest()  # hash the original file to store in metadata
    with open(path, "r", encoding=encoding) as c:
        lines = c.readlines()
        for i, line in enumerate(lines):
            if i > header_end - 1:
                break
            key, value = line[:].split(":", 1)
            key = (
                key.strip().lower().replace(" ", "_")
            )  # convert key to lowercase and replace spaces with underscores (snake_case)
            value = value.strip(", \n\t")
            try:
                meta_val = int(value)
            except ValueError:
                try:
                    meta_val = float(value)
                except ValueError:
                    try:
                        meta_val = {
                            "date": parse(value).isoformat()
                        }  # Others just true to parse as normal
                    except ValueError:
                        meta_val = value
                        pass
            if key.endswith(
                "(mg)"
            ):  # check if the key ends with "(mg)" and add it to value
                key = key[:-5]  # remove "(mg)" from the end of the key
                meta_val = {
                    "value": meta_val,
                    "unit": "mg",
                }  # put the value in a tuple with "/mg"
            elif key.endswith(
                "(c/s)"
            ):  # check if the key ends with "(C/s)" and add it to value
                key = key[:-6]  # remove "(C/s)" from the end of the key
                meta_val = {
                    "value": meta_val,
                    "unit": "°C/s",
                }  # put the value in a tuple with "C/s"
            elif key.endswith(
                "(c)"
            ):  # check if the key ends with "(C)" and add it to value
                key = key[:-4]  # remove "(C)" from the end of the key
                meta_val = {
                    "value": meta_val,
                    "unit": "°C",
                }  # put the value in a tuple with "C"
            elif key.endswith(
                "(s)"
            ):  # check if the key ends with "(s)" and add it to value
                key = key[:-4]  # remove "(s)" from the end of the key
                meta_val = {
                    "value": meta_val,
                    "unit": "s",
                }  # put the value in a tuple with "s"
            elif key.endswith(
                "(cc/min)"
            ):  # check if the key ends with "(cc/min)" and add it to value
                key = key[:-8]
                meta_val = {
                    "value": meta_val,
                    "unit": "ml/min",
                }  # put the value in a tuple with "cc/min"
            elif "calibration_file" in key:
                meta_val = {"file": meta_val}
            elif "t_correction_coefficients" in key:
                metadata["temperature_calibration"].update(
                    {
                        "coefficients": [
                            float(i) for i in meta_val.replace("\t", ",").split(",")
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
    metadata["file_hash"] = {
        "file": path.split("/")[-1],
        "method": "BLAKE2b",
        "hash": hash,
    }
    return metadata


def find_mcc_header(path: str, encoding: str) -> tuple[int, list[str], str]:
    """Find the header of the MCC file.

    Args:
        path (str): The path to the MCC file.
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
            if line[0].startswith("*"):  # column names start with ##
                header = next(reader)
                break
    return (i, header, delimiter)


def split_mcc_header(header: list[str]) -> tuple[list[str], list[str | None]]:
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
    for col in header:
        if " (" in col:
            col, unit = col.split(" (", 1)  # Split at the first instance of "/"
            cols.append(col.strip())
            units.append(unit[:-1].strip())
        else:
            cols.append(col.strip())
            units.append(None)
    return (cols, units)


def set_metadata(tbl, col_meta={}, tbl_meta={}) -> pa.Table:
    """Store table- and column-level metadata as json-encoded byte strings.

    Provided by: https://stackoverflow.com/a/69553667/25195764

    Table-level metadata is stored in the table's schema.
    Column-level metadata is stored in the table columns' fields.

    To update the metadata, first new fields are created for all columns.
    Next a schema is created using the new fields and updated table metadata.
    Finally a new table is created by replacing the old one's schema, but
    without copying any data.

    Args:
        tbl (pyarrow.Table): The table to store metadata in
        col_meta: A json-serializable dictionary with column metadata in the form
            {
                'column_1': {'some': 'data', 'value': 1},
                'column_2': {'more': 'stuff', 'values': [1,2,3]}
            }
        tbl_meta: A json-serializable dictionary with table-level metadata.

    Returns:
        pyarrow.Table: The table with updated metadata
    """
    # Create updated column fields with new metadata
    if col_meta or tbl_meta:
        fields = []
        for col in tbl.schema.names:
            if col in col_meta:
                # Get updated column metadata
                metadata = tbl.field(col).metadata or {}
                for k, v in col_meta[col].items():
                    if isinstance(v, bytes):
                        metadata[k] = v
                    elif isinstance(v, str):
                        metadata[k] = v.encode("utf-8")
                    else:
                        metadata[k] = json.dumps(v).encode("utf-8")
                # Update field with updated metadata
                fields.append(tbl.field(col).with_metadata(metadata))
            else:
                fields.append(tbl.field(col))

        # Get updated table metadata
        tbl_metadata = tbl.schema.metadata or {}
        for k, v in tbl_meta.items():
            if isinstance(v, bytes):
                tbl_metadata[k] = v
            elif isinstance(v, str):
                tbl_metadata[k] = v.encode("utf-8")
            else:
                tbl_metadata[k] = json.dumps(v).encode("utf-8")

        # Create new schema with updated field metadata and updated table metadata
        schema = pa.schema(fields, metadata=tbl_metadata)

        # With updated schema build new table (shouldn't copy data)
        # tbl = pa.Table.from_batches(tbl.to_batches(), schema)
        tbl = tbl.cast(schema)

    return tbl


if __name__ == "__main__":
    # path = "02_Data/STA/Douglas Fir Validation/FILED/DF_FILED_VAL_STA_N2_10K_240211_R1.csv"
    path = "tests/test_files/MCC/Hemp_Sheet_MCC_30K_min_220112_R1.txt"
    df = load_mcc_data(path)
    metadata = {
        k.decode("utf-8"): json.loads(v.decode("utf-8"))
        if isinstance(v, bytes) and v.startswith(b"{")
        else v.decode("utf-8")
        for k, v in df.schema.metadata.items()
    }
    pq.write_table(
        df, "tests/test_files/MCC/Hemp_Sheet_MCC_30K_min_220112_R1.parquet", compression="snappy"
    )
