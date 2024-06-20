import pyarrow as pa
import pyarrow.parquet as pq
from lab_etl.util import set_metadata, detect_encoding, get_hash
import re
from datetime import datetime as dt
from typing import Any
import numpy as np


def load_hfm_data(path):
    encoding = detect_encoding(path)
    metadata = get_hfm_metadata(path, encoding)
    data = extract_hfm_data(metadata)
    table = set_metadata(data, tbl_meta=metadata)
    return table


def get_hfm_metadata(path: str, encoding: str = "utf-16le"):
    type = "conductivity"  # assume it's thermal conductivity unless we find otherwise
    metadata: dict[str, str | float | dict[str, str | float]] = {}

    # Get file hash
    hash = get_hash(path)

    with open(path, "r", encoding=encoding) as c:
        lines = c.readlines()

        for i, line in enumerate(lines):
            line = line.strip()
            if (
                "date_performed" not in metadata
            ):  # if the date_performed is not already in metadata
                try:
                    datetime = dt.strptime(
                        line, "%A, %B %d, %Y, Time %H:%M"
                    )  # try to parse the date
                    metadata.update(
                        {"date_performed": datetime.isoformat()}
                    )  # if successful, store the date in metadata as the date_performed
                except ValueError:
                    pass
            if line.startswith("Sample Name: "):
                metadata.update({"sample_id": line.split(":")[1].strip()})
            elif line.startswith("Run Mode"):
                type = line.split(":")[1].strip().lower().replace(" ", "_")
                if type == "specific_heat":
                    type = "volumetric_heat_capacity"
            elif line.startswith("Transducer Heat Capacity Coefficients"):
                if "calibration" not in metadata:
                    metadata.update({"calibration": {}})
                sub_line = line.split(":")[1].strip()
                A = float(re.findall(r"\d+\.\d+", sub_line)[0])
                B = float(re.findall(r"\d+\.\d+", sub_line)[1])
                metadata["calibration"].update(
                    {"heat_capacity_coefficients": {"A": A, "B": B}}
                )
            elif line.startswith("Thickness: "):
                sub_line = line.split(":")[1].strip()
                value = float(re.findall(r"\d+\.\d+", sub_line)[0])
                unit = re.findall("[a-zA-Z]+", sub_line)[0]
                metadata.update({"thickness": {"value": value, "unit": unit}})
            elif line.startswith("Rear Left :"):
                sub_line = line.split(":")[1].strip()
                if "thickness" not in metadata:
                    metadata.update({"thickness": {}})
                value = float(re.findall(r"\d+\.\d+", sub_line)[0])
                unit = re.findall("[a-zA-Z]+", sub_line)[0]
                metadata["thickness"].update(
                    {"rear_left": {"value": value, "unit": unit}}
                )
                sub_line = line.split(":")[2].strip()
                value = float(re.findall(r"\d+\.\d+", sub_line)[0])
                unit = re.findall("[a-zA-Z]+", sub_line)[0]
                metadata["thickness"].update(
                    {"rear_right": {"value": value, "unit": unit}}
                )
            elif line.startswith("Front Left:"):
                sub_line = line.split(":")[1].strip()
                if "thickness" not in metadata:
                    metadata.update({"thickness": {}})
                value = float(re.findall(r"\d+\.\d+", sub_line)[0])
                unit = re.findall("[a-zA-Z]+", sub_line)[0]
                metadata["thickness"].update(
                    {"front_left": {"value": value, "unit": unit}}
                )
                sub_line = line.split(":")[2].strip()
                value = float(re.findall(r"\d+\.\d+", sub_line)[0])
                unit = re.findall("[a-zA-Z]+", sub_line)[0]
                metadata["thickness"].update(
                    {"front_right": {"value": value, "unit": unit}}
                )
            elif (
                line.startswith("[")
                and line.endswith("]")
                and not any(c in line[1:-1] for c in ["[", "]"])
            ):
                if "comment" not in metadata:
                    metadata.update({"comment": line.strip("[]").strip()})
                else:
                    metadata.update(
                        {"comment": [metadata["comment"], line.strip("[]").strip()]}
                    )
            elif line.startswith("Thickness obtained"):
                if "thickness" not in metadata:
                    metadata.update({"thickness": {}})
                metadata["thickness"].update(
                    {"obtained": line.split(":")[1].strip("from ")}
                )
            elif line.startswith("Calibration used"):
                if "calibration" not in metadata:
                    metadata.update({"calibration": {}})
                metadata["calibration"].update({"type": line.split(":")[1].strip()})
            elif line.startswith("Calibration File Id"):
                if "calibration" not in metadata:
                    metadata.update({"calibration": {}})
                metadata["calibration"].update({"file": line.split(":")[1].strip()})
            elif line.startswith("Number of transducer per plate"):
                metadata.update(
                    {"number_of_transducers": int(line.split(":")[1].strip())}
                )
            elif line.startswith("Number of Setpoints"):
                metadata.update(
                    {"number_of_setpoints": int(line.split(":")[1].strip())}
                )
                if type == "conductivity":
                    offset = 1
                elif type == "volumetric_heat_capacity":
                    offset = 0
                for j in range(i + 1, i + offset + metadata["number_of_setpoints"]):
                    if "setpoints" not in metadata:
                        metadata.update({"setpoints": {}})
                    metadata["setpoints"].update({f"setpoint_{j-i}": {}})
            elif line.startswith("Setpoint No."):
                setpoint = int(line.split(".")[1].strip())
                for j in range(1, 19):
                    if (
                        "date_performed"
                        not in metadata["setpoints"][f"setpoint_{setpoint}"]
                    ):
                        try:
                            datetime = dt.strptime(
                                lines[i - 2].strip(), "%A, %B %d, %Y, Time %H:%M"
                            )
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"date_performed": datetime.isoformat()}
                            )
                        except ValueError:
                            pass
                    if lines[i + j].strip().startswith("Setpoint Upper:"):
                        value = re.findall(
                            r"\d+\.\d+", lines[i + j].split(":")[1].strip()
                        )[0]
                        unit = re.findall(
                            "[^\x00-\x7f]+[a-zA-Z]+", lines[i + j].split(":")[1].strip()
                        )[0]
                        if (
                            "setpoint_temperature"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"setpoint_temperature": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "setpoint_temperature"
                        ].update({"upper": {"value": float(value), "unit": unit}})
                    elif lines[i + j].strip().startswith("Setpoint Lower:"):
                        value = re.findall(
                            r"\d+\.\d+", lines[i + j].split(":")[1].strip()
                        )[0]
                        unit = re.findall(
                            "[^\x00-\x7f]+[a-zA-Z]+", lines[i + j].split(":")[1].strip()
                        )[0]
                        if (
                            "setpoint_temperature"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"setpoint_temperature": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "setpoint_temperature"
                        ].update({"lower": {"value": float(value), "unit": unit}})
                    elif lines[i + j].strip().startswith("Temperature Upper"):
                        value = re.findall(
                            r"\d+\.\d+", lines[i + j].split(":")[1].strip()
                        )[0]
                        unit = re.findall(
                            "[^\x00-\x7f]+[a-zA-Z]+", lines[i + j].split(":")[1].strip()
                        )[0]
                        if (
                            "temperature"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"temperature": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "temperature"
                        ].update({"upper": {"value": float(value), "unit": unit}})
                    elif lines[i + j].strip().startswith("Temperature Lower"):
                        value = re.findall(
                            r"\d+\.\d+", lines[i + j].split(":")[1].strip()
                        )[0]
                        unit = re.findall(
                            "[^\x00-\x7f]+[a-zA-Z]+", lines[i + j].split(":")[1].strip()
                        )[0]
                        if (
                            "temperature"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"temperature": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "temperature"
                        ].update({"lower": {"value": float(value), "unit": unit}})
                    elif lines[i + j].strip().startswith("CalibFactor  Upper"):
                        if (
                            "calibration"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"calibration": {}}
                            )
                        unit = "µV/W"
                        value = float(lines[i + j].split(":")[1].strip())
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "calibration"
                        ].update({"upper": {"value": value, "unit": unit}})
                    elif lines[i + j].strip().startswith("CalibFactor  Lower"):
                        if (
                            "calibration"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"calibration": {}}
                            )
                        unit = "µV/W"
                        value = float(lines[i + j].split(":")[1].strip())
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "calibration"
                        ].update({"lower": {"value": value, "unit": unit}})
                    elif lines[i + j].strip().startswith("Results Upper"):
                        if (
                            "results"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"results": {}}
                            )
                        value = float(
                            re.findall(r"\d+\.\d+", lines[i + j].split(":")[1].strip())[
                                0
                            ]
                        )
                        unit = re.findall(
                            "[a-zA-Z]/[a-zA-Z]+", lines[i + j].split(":")[1].strip()
                        )[0]
                        metadata["setpoints"][f"setpoint_{setpoint}"]["results"].update(
                            {"upper": {"value": value, "unit": unit}}
                        )
                    elif lines[i + j].strip().startswith("Results Lower"):
                        if (
                            "results"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"results": {}}
                            )
                        value = float(
                            re.findall(r"\d+\.\d+", lines[i + j].split(":")[1].strip())[
                                0
                            ]
                        )
                        unit = re.findall(
                            "[a-zA-Z]/[a-zA-Z]+", lines[i + j].split(":")[1].strip()
                        )[0]
                        metadata["setpoints"][f"setpoint_{setpoint}"]["results"].update(
                            {"lower": {"value": value, "unit": unit}}
                        )
                    elif lines[i + j].strip().startswith("Temperature Equilibrium"):
                        if (
                            "thermal_equilibrium"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"thermal_equilibrium": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "thermal_equilibrium"
                        ].update(
                            {"temperature": float(lines[i + j].split(":")[1].strip())}
                        )
                    elif lines[i + j].strip().startswith("Between Block HFM Equil."):
                        if (
                            "thermal_equilibrium"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"thermal_equilibrium": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "thermal_equilibrium"
                        ].update(
                            {"between_block": float(lines[i + j].split(":")[1].strip())}
                        )
                    elif lines[i + j].strip().startswith("HFM Percent Change"):
                        if (
                            "thermal_equilibrium"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"thermal_equilibrium": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "thermal_equilibrium"
                        ].update(
                            {
                                "percent_change": float(
                                    lines[i + j].split(":")[1].strip()
                                )
                            }
                        )
                    elif lines[i + j].strip().startswith("Min Number of Blocks"):
                        if (
                            "thermal_equilibrium"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"thermal_equilibrium": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "thermal_equilibrium"
                        ].update(
                            {
                                "min_number_of_blocks": float(
                                    lines[i + j].split(":")[1].strip()
                                )
                            }
                        )
                    elif lines[i + j].strip().startswith("Calculation Blocks"):
                        if (
                            "thermal_equilibrium"
                            not in metadata["setpoints"][f"setpoint_{setpoint}"]
                        ):
                            metadata["setpoints"][f"setpoint_{setpoint}"].update(
                                {"thermal_equilibrium": {}}
                            )
                        metadata["setpoints"][f"setpoint_{setpoint}"][
                            "thermal_equilibrium"
                        ].update(
                            {
                                "calculation_blocks": float(
                                    lines[i + j].split(":")[1].strip()
                                )
                            }
                        )
                    elif lines[i + j].strip().startswith("Temperature Average"):
                        value = re.findall(
                            r"\d+\.\d+", lines[i + j].split(":")[1].strip()
                        )[0]
                        unit = re.findall(
                            "[^\x00-\x7f]+[a-zA-Z]+", lines[i + j].split(":")[1].strip()
                        )[0]
                        metadata["setpoints"][f"setpoint_{setpoint}"].update(
                            {
                                "temperature_average": {
                                    "value": float(value),
                                    "unit": unit,
                                }
                            }
                        )
                    elif lines[i + j].strip().startswith("Specific Heat"):
                        sub_line = lines[i + j].split(":")[1].strip()
                        value = re.findall(r"\d+", sub_line)[0]
                        unit = sub_line.replace(value, "").strip()
                        metadata["setpoints"][f"setpoint_{setpoint}"].update(
                            {
                                "volumetric_heat_capacity": {
                                    "value": float(value),
                                    "unit": unit,
                                }
                            }
                        )
    metadata.update({"type": type})
    metadata["file_hash"] = {
        "file": path.split("/")[-1],
        "method": "BLAKE2b",
        "hash": hash,
    }
    return metadata


def extract_hfm_data(meta: dict[Any, Any]) -> pa.Table:
    """Extract HFM data and return it as a PyArrow Table with metadata.

    Args:
        meta (dict): The metadata dictionary containing HFM data.

    Returns:
        pyarrow.Table: The PyArrow table with the extracted data and metadata.
    """
    data = []
    units = []
    col_units = {}

    if meta["type"] == "conductivity":
        schema = pa.schema(
            [
                pa.field("setpoint", pa.int32()),
                pa.field("upper_temperature", pa.float64()),
                pa.field("lower_temperature", pa.float64()),
                pa.field("upper_conductivity", pa.float64()),
                pa.field("lower_conductivity", pa.float64()),
            ]
        )
        for key, value in meta["setpoints"].items():
            setpoint = int(key.split("_")[1])
            upper_temp = value["temperature"]["upper"]["value"]
            upper_temp_unit = value["temperature"]["upper"]["unit"]
            lower_temp = value["temperature"]["lower"]["value"]
            lower_temp_unit = value["temperature"]["lower"]["unit"]
            upper_cond = value["results"]["upper"]["value"]
            upper_cond_unit = value["results"]["upper"]["unit"]
            lower_cond = value["results"]["lower"]["value"]
            lower_cond_unit = value["results"]["lower"]["unit"]
            data.append([setpoint, upper_temp, lower_temp, upper_cond, lower_cond])
            units = [upper_temp_unit, lower_temp_unit, upper_cond_unit, lower_cond_unit]
        col_units = {
            "upper_temperature": {"units": units[0]},
            "lower_temperature": {"units": units[1]},
            "upper_conductivity": {"units": units[2]},
            "lower_conductivity": {"units": units[3]},
        }
    elif meta["type"] == "volumetric_heat_capacity":
        schema = pa.schema(
            [
                pa.field("setpoint", pa.int32()),
                pa.field("average_temperature", pa.float64()),
                pa.field("volumetric_heat_capacity", pa.float64()),
            ]
        )
        for key, value in meta["setpoints"].items():
            setpoint = int(key.split("_")[1])
            average_temp = value["temperature_average"]["value"]
            average_temp_unit = value["temperature_average"]["unit"]
            specific_heat = value["volumetric_heat_capacity"]["value"]
            specific_heat_unit = value["volumetric_heat_capacity"]["unit"]
            data.append([setpoint, average_temp, specific_heat])
            units = [average_temp_unit, specific_heat_unit]
        col_units = {
            "average_temperature": {"units": units[0]},
            "volumetric_heat_capacity": {"units": units[1]},
        }

    # Transpose data to match schema
    trans_data = np.transpose(data)
    arrays = [pa.array(trans_data[i]) for i in range(len(trans_data))]

    # Create PyArrow table from arrays and schema
    table = pa.Table.from_arrays(arrays, schema=schema)

    # Add metadata to the table
    table = set_metadata(table, col_meta=col_units)

    return table


if __name__ == "__main__":
    path = "tests/test_files/HFM/Black_PMMA_HFM_Dry_conductivity_211115_R1.tst"
    df = load_hfm_data(path)
    pq.write_table(
        df, "tests/test_files/HFM/Black_PMMA_HFM_Dry_conductivity_211115_R1.parquet"
    )
    path = "tests/test_files/HFM/Black_PMMA_HFM_Dry_heatcapacity_211117_R3.tst"
    df = load_hfm_data(path)
    pq.write_table(
        df,
        "tests/test_files/HFM/Black_PMMA_HFM_Dry_heatcapacity_211117_R3.parquet",
    )
