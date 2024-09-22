from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from lab_etl.util import get_hash, set_metadata


def load_cone_data(path: str) -> pa.Table:
    """Load a Cone file and store metadata in the pyarrow table.

    Args:
        path (str): The path to the Cone file.

    Returns:
        pyarrow.Table: The table with the data from the Cone file and metadata.
    """
    mapping = {
        "Stack TC": "stack_temperature",
        "Smoke TC": "smoke_temperature",
        "Exh Press": "exhaust_pressure",
        "Ext Coeff": "extinction_coefficient",
        "Flame Verif": "flame_verification",
        "Smoke Comp": "smoke_laser_compensation",
        "Smoke Meas": "smoke_laser_measurement",
    }

    # Read Excel data using Polars
    try:
        df = pl.read_excel(
            path, engine="calamine", sheet_id=2, read_options={"skip_rows": 4}
        )
    except Exception as e:
        raise ValueError(f"Error reading Excel file at {path}: {str(e)}")

    # Get units and metadata
    units = get_cone_units(path)
    meta = get_cone_metadata(path)

    # Drop 'Names' column if it exists
    if "Names" in df.columns:
        df = df.drop("Names")
    if "Ext Coeff" in df.columns:
        df = df.drop("Ext Coeff")

    # Rename columns based on the mapping
    df = df.rename(
        {col: mapping.get(col, col).lower().replace(" ", "_") for col in df.columns}
    )

    # Convert Polars DataFrame to PyArrow Table
    table = df.to_arrow()

    # Add metadata to the PyArrow Table
    table_meta = set_metadata(
        table, col_meta=units, tbl_meta={"file_metadata": meta, "type": "Cone"}
    )

    return table_meta


def get_cone_units(path: str) -> dict:
    """Get the units from a Cone file using Polars.

    Args:
        path (str): The path to the Cone file.

    Returns:
        dict: A dictionary with the units of the columns.
    """
    mapping = {"C": "°C", "/m": "1/m", "sec": "s"}
    k_mapping = {
        "Stack TC": "stack_temperature",
        "Smoke TC": "smoke_temperature",
        "Exh Press": "exhaust_pressure",
        "Ext Coeff": "extinction_coefficient",
        "Flame Verif": "flame_verification",
        "Smoke Comp": "smoke_laser_compensation",
        "Smoke Meas": "smoke_laser_measurement",
    }

    try:
        # Read Excel file using Polars
        units = pl.read_excel(
            path,
            engine="calamine",
            sheet_id=2,
            read_options={"n_rows": 1, "skip_rows": 3},
        )
        units_dict = units.to_dicts()[0]
    except Exception as e:
        raise ValueError(f"Error reading Excel file at {path}: {str(e)}")

    # Process units dictionary
    if "Names" in units_dict:
        del units_dict["Names"]

    units_result = {}

    for k, v in units_dict.items():
        if v is not None:
            standardized_key = k_mapping.get(k, k).lower().replace(" ", "_")
            unit = mapping.get(v, v)
            units_result[standardized_key] = {"unit": unit}

    return units_result


def get_cone_metadata(path: str) -> dict:
    """Get the metadata from a Cone file.

    Args:
        path (str): The path to the Cone file.

    Returns:
        dict: A dictionary with the metadata of the file.
    """
    mapping = {
        "test_ident": "test_id",
        "surf_area": "surface_area",
        "specimen_mass": "sample_mass",
        "pre_test_cmt": "comment",
        "post_test_cmt": "comment",
    }

    # Get file metadata
    file_hash = get_hash(path)

    # Read Excel file using Polars
    try:
        meta = pl.read_excel(
            path, engine="calamine", sheet_id=1, read_options={"header_row": None}
        )
    except Exception as e:
        raise ValueError(f"Error reading Excel file at {path}: {str(e)}")

    meta_dict: dict[str, Any] = {}

    # Process each row in the DataFrame
    for row in meta.iter_rows():
        if len(row) < 2:
            continue

        key = row[0].strip().lower().replace(" ", "_")
        value = row[1].strip()

        if key in mapping:
            key = mapping[key]

        # Convert value to int or float if possible
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass

        # Aggregate values into dictionary
        if key in meta_dict:
            if not isinstance(meta_dict[key], list):
                meta_dict[key] = [meta_dict[key]]
            meta_dict[key].append(value)
        else:
            meta_dict[key] = value

    # Add hash to metadata
    meta_dict["file_hash"] = {
        "file": path.split("/")[-1],
        "method": "BLAKE2b",
        "hash": file_hash,
    }
    return meta_dict


if __name__ == "__main__":
    path = "tests/test_files/Cone/Asphalt_Shingle_Cone_HF25_220415_R1.XLSM"
    table = load_cone_data(path)
    pq.write_table(
        table, "tests/test_files/Cone/Asphalt_Shingle_Cone_HF25_220415_R1.parquet"
    )
    # print(table)
