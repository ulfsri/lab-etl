from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from brukeropus import read_opus
from brukeropus.file import OPUSFile, get_param_label

from lab_etl.util import get_hash, set_metadata


def load_ftir_data(file_path: str) -> pa.Table:
    """Loads FTIR data from an OPUS file and returns it as a pa.Table.

    Args:
        file_path (str): The path to the OPUS file.

    Returns:
        pa.Table: The FTIR data as a pa.Table with included metadata.
    """
    opus_file = read_opus(file_path)
    if bool(opus_file):
        # Get FTIR data as a pa.Table
        table = get_ftir_data(opus_file)

        # Define column metadata
        col_meta = {
            "wavelength": {"unit": "µm"},
            "reflectance": {"unit": "a.u."},
            "absorbance": {"unit": "a.u."},
            "transmittance": {"unit": "a.u."},
            "reference_spectrum": {"unit": "a.u."},
            "sample_spectrum": {"unit": "a.u."},
            "sample_phase": {"unit": "a.u."},
        }

        # Get table metadata
        tbl_meta = get_ftir_meta(opus_file)

        # Set metadata to the table
        table = set_metadata(
            table,
            col_meta=col_meta,
            tbl_meta={"file_metadata": tbl_meta, "type": "FTIR"},
        )
        return table
    else:
        raise ValueError("Not a valid OPUS file")


def get_ftir_data(file: OPUSFile) -> pa.Table:
    """Retrieves FTIR data from the given OPUSFile object and returns it as a pa.Table.

    Args:
        file (OPUSFile): The OPUSFile object containing the FTIR data.

    Returns:
        pa.Table: The FTIR data as a pa.Table.
    """

    def construct_schema_and_data(
        main_key: str, label: str
    ) -> tuple[list[np.ndarray], list[pa.field]]:
        """Constructs schema and data arrays based on the main data key and label."""
        data = [
            np.array(getattr(file, main_key).wl, dtype=np.float64),
            np.array(getattr(file, main_key).y, dtype=np.float64),
        ]
        schema_list = [
            pa.field("wavelength", pa.float64()),
            pa.field(label.lower().replace(" ", "_"), pa.float64()),
        ]

        for key in file.all_data_keys:
            if key != main_key:
                x = np.array(getattr(file, key).wl, dtype=np.float64)
                y = np.array(getattr(file, key).y, dtype=np.float64)
                y_new = np.interp(getattr(file, main_key).wl, x, y)
                data.append(np.array(y_new, dtype=np.float64))
                schema_list.append(
                    pa.field(
                        getattr(file, key).label.lower().replace(" ", "_"), pa.float64()
                    )
                )

        return data, schema_list

    for main_key in ("r", "a", "t"):
        if main_key in file.all_data_keys:
            label = getattr(file, main_key).label
            data, schema_list = construct_schema_and_data(main_key, label)
            schema = pa.schema(schema_list)
            return pa.Table.from_arrays(data, schema=schema)

    raise ValueError("No valid data keys found in the OPUS file")


def get_ftir_meta(file: OPUSFile) -> dict[Any, Any]:
    """
    Retrieves the metadata from the given OPUSFile object and returns it as a dictionary.

    Args:
        file (OPUSFile): The OPUSFile object from which to retrieve the metadata.

    Returns:
        dict[str, str | dict[Any, Any]]: A dictionary containing the metadata.

    """
    meta = {}

    # Get file hash
    hash = get_hash(file.filepath)

    # Extract parameters with formatted keys
    def format_key(key):
        return get_param_label(key).lower().replace(" ", "_")

    params = {format_key(key): value for key, value in file.params.items()}
    rf_params = {format_key(key): value for key, value in file.rf_params.items()}

    # Extract labels
    labels = {
        key.key: key.label.lower().replace(" ", "_") for key in file.iter_all_data()
    }

    # Update metadata dictionary
    meta.update(
        {
            "data_labels": labels,
            "parameters": params,
            "reference_parameters": rf_params,
            "file_hash": {
                "file": file.filepath.split("/")[-1],
                "method": "BLAKE2b",
                "hash": hash,
            },
        }
    )

    # Determine data performed date
    for data_type in ("r", "a", "t"):
        if data_type in labels:
            meta["data_performed"] = getattr(file, data_type).datetime.isoformat()
            break

    return meta


if __name__ == "__main__":
    # path = "tests/test_files/FTIR/Upper_Fiber_Cement_Board_3.0"
    path = "tests/test_files/FTIR/Bmore_Jacket_CSTM_Stripe_ATR_240517_R2.0"
    # path = (
    #     "tests/test_files/FTIR/Natural_Nylon_Sheet_Extruded_0.125_Trans_IS_R1_221212.0"
    # )
    table = load_ftir_data(path)
    pq.write_table(
        table,
        "tests/test_files/FTIR/Bmore_Jacket_CSTM_Stripe_ATR_240517_R2.parquet",
        compression="snappy",
    )
