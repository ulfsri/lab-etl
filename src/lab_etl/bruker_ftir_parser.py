from brukeropus import read_opus
from brukeropus.file import OPUSFile
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any
import numpy as np
from brukeropus.file import get_param_label
from lab_etl.util import set_metadata


def load_ftir_data(file_path: str) -> pa.Table:
    """Loads FTIR data from an OPUS file and returns it as a pa.Table.

    Args:
        file_path (str): The path to the OPUS file.

    Returns:
        pa.Table: The FTIR data as a pa.Table with included metadata.
    """
    opus_file = read_opus(file_path)
    if bool(opus_file):
        table = get_ftir_data(opus_file)
        col_meta = {
            "Wavelength": {"unit": "µm"},
            "Reflectance": {"unit": "a.u."},
            "Absorbance": {"unit": "a.u."},
            "Transmittance": {"unit": "a.u."},
            "Reference Spectrum": {"unit": "a.u."},
            "Sample Spectrum": {"unit": "a.u."},
            "Sample Phase": {"unit": "a.u."},
        }
        tbl_meta = get_ftir_meta(opus_file)
        table = set_metadata(table, col_meta=col_meta, tbl_meta=tbl_meta)
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
    data = []
    schema_list = []
    if "r" in file.all_data_keys:
        data = [
            np.float64(file.r.wl),  # Wavelength
            np.float64(file.r.y),  # Reflectance
        ]
        schema_list = [
            pa.field("Wavelength", pa.float64()),
            pa.field(file.r.label, pa.float64()),
        ]
        for key in file.all_data_keys:
            if key != "r":
                x = np.float64(getattr(file, key).wl)
                y = np.float64(getattr(file, key).y)
                y_new = np.interp(file.r.wl, x, y)
                data.append(y_new)
                schema_list.append(pa.field(getattr(file, key).label, pa.float64()))
        schema = pa.schema(schema_list)
        return pa.Table.from_arrays(data, schema=schema)
    elif "a" in file.all_data_keys:
        data = [
            np.float64(file.a.wl),  # Wavelength
            np.float64(file.a.y),  # Absorbance
        ]
        schema_list = [
            pa.field("Wavelength", pa.float64()),
            pa.field(file.a.label, pa.float64()),
        ]
        for key in file.all_data_keys:
            if key != "a":
                x = np.float64(getattr(file, key).wl)
                y = np.float64(getattr(file, key).y)
                y_new = np.interp(file.a.wl, x, y)
                data.append(y_new)
                schema_list.append(pa.field(getattr(file, key).label, pa.float64()))
        schema = pa.schema(schema_list)
        return pa.Table.from_arrays(data, schema=schema)
    elif "t" in file.all_data_keys:
        data = [np.float64(file.t.wl), np.float64(file.t.y)]
        schema_list = [
            pa.field("Wavelength", pa.float64()),
            pa.field(file.t.label, pa.float64()),
        ]
        for key in file.all_data_keys:
            if key != "t":
                x = np.float64(getattr(file, key).wl)
                y = np.float64(getattr(file, key).y)
                y_new = np.interp(file.t.wl, x, y)
                data.append(y_new)
                schema_list.append(pa.field(getattr(file, key).label, pa.float64()))
        schema = pa.schema(schema_list)
        return pa.Table.from_arrays(data, schema=schema)


def get_ftir_meta(file: OPUSFile) -> dict[Any, Any]:
    """
    Retrieves the metadata from the given OPUSFile object and returns it as a dictionary.

    Args:
        file (OPUSFile): The OPUSFile object from which to retrieve the metadata.

    Returns:
        dict[str, str | dict[Any, Any]]: A dictionary containing the metadata.

    """
    meta = {}
    params = {
        get_param_label(key).lower().replace(" ", "_"): value
        for key, value in file.params.items()
    }
    rf_params = {
        get_param_label(key).lower().replace(" ", "_"): value
        for key, value in file.rf_params.items()
    }
    labels = {key.key: key.label for key in file.iter_all_data()}
    meta.update({"data_labels": labels})
    if "r" in labels:
        meta.update({"data_performed": file.r.datetime.isoformat()})
    elif "a" in labels:
        meta.update({"data_performed": file.a.datetime.isoformat()})
    elif "t" in labels:
        meta.update({"data_performed": file.t.datetime.isoformat()})
    meta.update({"parameters": params})
    meta.update({"reference_parameters": rf_params})
    return meta


if __name__ == "__main__":
    path = "tests/test_files/FTIR/Upper_Fiber_Cement_Board_3.0"
    # path = "tests/test_files/FTIR/Bmore_Jacket_CSTM_Stripe_ATR_240517_R2.0"
    # path = (
    #     "tests/test_files/FTIR/Natural_Nylon_Sheet_Extruded_0.125_Trans_IS_R1_221212.0"
    # )
    table = load_ftir_data(path)
    print(table)
    pq.write_table(
        table,
        "tests/test_files/FTIR/Natural_Nylon_Sheet_Extruded_0.125_Trans_IS_R1_221212.parquet",
        compression="snappy",
    )
