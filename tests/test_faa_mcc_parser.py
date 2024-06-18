import unittest
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os
from lab_etl.faa_mcc_parser import (
    load_mcc_data,
    get_mcc_metadata,
    find_mcc_header,
    split_mcc_header,
    set_metadata,
)

class TestParseMCC(unittest.TestCase):
    def setUp(self):
        self.test_files_dir = "tests/test_files/MCC"
        self.csv_file_path = os.path.join(self.test_files_dir, "Hemp_Sheet_MCC_30K_min_220112_R1.txt")
        self.parquet_file_path = os.path.join(self.test_files_dir, "Hemp_Sheet_MCC_30K_min_220112_R1.parquet")

    def tearDown(self):
        if os.path.exists(self.parquet_file_path):
            os.remove(self.parquet_file_path)

    def test_load_mcc_data(self):
        result = load_mcc_data(self.csv_file_path)
        self.assertIsInstance(result, pa.Table)
        expected_columns = ["Time", "Temperature", "N2 flow rate", "O2 flow rate", "Flow Rate", "Oxygen", "HRR", "Heating rate"]  # Replace with actual column names
        self.assertListEqual(result.column_names, expected_columns)
        expected_num_rows = 2584
        self.assertEqual(result.num_rows, expected_num_rows)

    def test_get_mcc_metadata(self):
        metadata = get_mcc_metadata(self.csv_file_path, "us-ascii", 9)  # Replace with actual encoding and header end index
        self.assertIsInstance(metadata, dict)
        expected_keys = ["sample_id", "sample_mass", "heating_rate", "combustor_temperature", "n2_flow_rate", "o2_flow_rate", "temperature_calibration", "time_shift", "file_hash"]  # Replace with actual expected keys
        self.assertListEqual(sorted(metadata.keys()), sorted(expected_keys))
        self.assertEqual(metadata["file_hash"]['hash'], "894746aebd128d33e8f24b068795787762ef0ef2cb0edcceb8eeb75a059ba9daf989ffd14de6297aa32b9957f0c3a671f316003c1bc2fa8359318d173ff9d828")

    def test_find_mcc_header(self):
        header_end, header, delimiter = find_mcc_header(self.csv_file_path, "us-ascii")
        expected_header_end = 9
        self.assertEqual(header_end, expected_header_end)
        expected_header = ["Time (s)", "Temperature (C)", "N2 flow rate (cc/min)", "O2 flow rate (cc/min)", "Flow Rate (cc/min)", "Oxygen (%)", "HRR (W/g)", "Heating rate (C/s)"]  # Replace with actual expected header
        self.assertListEqual(header, expected_header)
        expected_delimiter = "\t"
        self.assertEqual(delimiter, expected_delimiter)

    def test_split_mcc_header(self):
        header = ["Time (s)", "Temperature (C)", "N2 flow rate (cc/min)", "O2 flow rate (cc/min)", "Flow Rate (cc/min)", "Oxygen (%)", "HRR (W/g)", "Heating rate (C/s)"]
        cols, units = split_mcc_header(header)
        expected_cols = ["Time", "Temperature", "N2 flow rate", "O2 flow rate", "Flow Rate", "Oxygen", "HRR", "Heating rate"]
        self.assertListEqual(cols, expected_cols)
        expected_units = ["s", "C", "cc/min", "cc/min", "cc/min", "%", "W/g", "C/s"]
        self.assertListEqual(units, expected_units)

    def test_set_metadata(self):
        data = [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ]
        schema = pa.schema([
            pa.field("A", pa.int64()),
            pa.field("B", pa.int64()),
            pa.field("C", pa.int64())
        ])
        table = pa.Table.from_arrays(data, schema=schema)
        col_meta = {
            "A": {"description": "Column A"},
            "B": {"description": "Column B"},
            "C": {"description": "Column C"}
        }
        tbl_meta = {
            "table_description": "Sample Table"
        }
        updated_table = set_metadata(table, col_meta=col_meta, tbl_meta=tbl_meta)
        for col in updated_table.schema.names:
            if col in col_meta:
                decoded = {
                    k.decode("utf-8"): json.loads(v.decode("utf-8"))
                    if isinstance(v, bytes) and v.startswith(b"{")
                    else v.decode("utf-8")
                    for k, v in updated_table.field(col).metadata.items()
                }
                self.assertEqual(decoded, col_meta[col])
        decoded = {
            k.decode("utf-8"): json.loads(v.decode("utf-8"))
            if isinstance(v, bytes) and v.startswith(b"{")
            else v.decode("utf-8")
            for k, v in updated_table.schema.metadata.items()
        }
        self.assertEqual(decoded, tbl_meta)

    def test_parquet_file_written(self):
        table = load_mcc_data(self.csv_file_path)
        pq.write_table(table, self.parquet_file_path, compression="snappy")
        self.assertTrue(os.path.exists(self.parquet_file_path))

    # def test_metadata_written_to_parquet(self):
    #     table = load_mcc_data(self.csv_file_path)
    #     column_names = table.column_names
    #     for col in column_names:
    #         table = set_metadata(table, col_meta={col: {"unit": "kg"}})
    #     pq.write_table(table, self.parquet_file_path, compression="snappy")
    #     table_read = pq.read_table(self.parquet_file_path)
    #     print(table_read.field("HRR").metadata.items())
    #     for col in column_names:
    #         print({k.decode("utf-8") : v.decode("utf-8") for k, v in table_read.field(col).metadata.items()})
    #         col_meta = {k.decode("utf-8") : v.decode("utf-8") for k, v in table_read.field(col).metadata.items()}
    #         self.assertEqual(col_meta["unit"], "kg")


if __name__ == "__main__":
    unittest.main()