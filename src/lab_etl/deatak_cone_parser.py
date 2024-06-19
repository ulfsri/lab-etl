import pyarrow as pa
import pyarrow.parquet as pq
from lab_etl.util import set_metadata
import polars as pl


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
    }
    df = pl.read_excel(
        path, engine="calamine", sheet_id=2, read_options={"skip_rows": 5}
    )
    units = get_cone_units(path)
    meta = get_cone_metadata(path)
    df = df.drop("Names")
    df = df.rename(
        {col: mapping.get(col, col).lower().replace(" ", "_") for col in df.columns}
    )
    table = df.to_arrow()
    table_meta = set_metadata(table, col_meta=units, tbl_meta=meta)
    return table_meta


def get_cone_units(path: str):
    """Get the units from a Cone file.

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
    }
    units = pl.read_excel(
        path, engine="calamine", sheet_id=2, read_options={"n_rows": 1, "skip_rows": 3}
    )
    units = units.to_dicts()[0]
    del units["Names"]
    units = {
        k_mapping.get(k, k).lower().replace(" ", "_"): {"unit": mapping.get(v, v)}
        for k, v in units.items()
        if v is not None
    }
    return units


def get_cone_metadata(path: str):
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
    meta = pl.read_excel(
        path, engine="calamine", sheet_id=1, read_options={"header_row": None}
    )
    meta_dict = {}
    for row in meta.iter_rows():
        key = row[0].strip().lower().replace(" ", "_")
        value = row[1].strip()
        if key in mapping:
            key = mapping[key]
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass
        if key in meta_dict:
            meta_dict[key] = [meta_dict[key], value]
        else:
            meta_dict[key] = value
    return meta_dict


if __name__ == "__main__":
    path = "tests/test_files/Cone/Asphalt_Shingle_Cone_HF25_220415_R1.XLSM"
    table = load_cone_data(path)
    pq.write_table(
        table, "tests/test_files/Cone/Asphalt_Shingle_Cone_HF25_220415_R1.parquet"
    )
