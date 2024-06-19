from brukeropus import read_opus
from brukeropus.file import OPUSFile
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any
import numpy as np
import json
from brukeropus.file import get_param_label


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
    if "r" in file.all_data_keys:
        data = [
            np.float64(file.r.wl),  # Wavelength
            np.float64(file.r.y),  # Reflectance
        ]
        schema = pa.schema(
            [pa.field("Wavelength", pa.float64()), pa.field(file.r.label, pa.float64())]
        )
        return pa.Table.from_arrays(data, schema=schema)
    elif "a" in file.all_data_keys:
        data = [
            np.float64(file.a.wl),  # Wavelength
            np.float64(file.a.y),  # Absorbance
        ]
        schema = pa.schema(
            [pa.field("Wavelength", pa.float64()), pa.field(file.a.label, pa.float64())]
        )
        return pa.Table.from_arrays(data, schema=schema)
    elif "t" in file.all_data_keys:
        data = [np.float64(file.t.wl), np.float64(file.t.y)]
        schema = pa.schema(
            [pa.field("Wavelength", pa.float64()), pa.field(file.t.label, pa.float64())]
        )
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
    # path = "tests/test_files/FTIR/Upper_Fiber_Cement_Board_3.0"
    # path = "tests/test_files/FTIR/Bmore_Jacket_CSTM_Stripe_ATR_240517_R2.0"
    path = (
        "tests/test_files/FTIR/Natural_Nylon_Sheet_Extruded_0.125_Trans_IS_R1_221212.0"
    )
    table = load_ftir_data(path)
    pq.write_table(
        table,
        "tests/test_files/FTIR/Natural_Nylon_Sheet_Extruded_0.125_Trans_IS_R1_221212.parquet",
        compression="snappy",
    )
    # opus_file = read_opus(path)  # Returns an OPUSFile class
    # opus_file.print_parameters()  # Pretty prints all metadata in the file to the console
    # print(opus_file.data_keys)  # Returns a list of all data keys in the file

    # # General parameter metadata
    # dict(opus_file.params)

    # # Reference parameter metadata
    # dict(opus_file.rf_params)

    # # Data
    # opus_file.all_data_keys # Returns a list of all data keys in the file: ['rf', 'r', 'sm']
    # opus_file.r.label # Returns the label of the reflectance spectrum
    # opus_file.r.x # Returns the x-axis data for the reflectance spectrum, in whatever units it was saved in, can be queried with opus_file.r.dxu
    # opus_file.r.wl # Returns the x-axis data for the reflectance spectrum as wavelength (um)
    # opus_file.r.y # Returns the y-axis data for the reflectance spectrum
    # opus_file.r.datetime # Returns the date and time of the measurement

    # opus_file.iter_all_data() # Returns a generator that yields all data in the file, i.e. iterates through all data keys

    # sm: Single-channel sample spectra
    # rf: Single-channel reference spectra
    # igsm: Sample interferogram
    # igrf: Reference interferogram
    # phsm: Sample phase
    # phrf: Reference phase
    # a: Absorbance
    # t: Transmittance
    # r: Reflectance
    # km: Kubelka-Munk
    # tr: Trace (Intensity over Time)
    # gcig: gc File (Series of Interferograms)
    # gcsc: gc File (Series of Spectra)
    # ra: Raman
    # e: Emission
    # pw: Power
    # logr: log(Reflectance)
    # atr: ATR
    # pas: Photoacoustic

    # Conversions between parameter names can be found in
    # from brukeropus.file.constants import PARAM_LABELS
