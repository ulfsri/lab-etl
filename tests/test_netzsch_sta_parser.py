import unittest
import os
import pyarrow as pa
import json
import pyarrow.parquet as pq
from lab_etl.netzsch_sta_parser import (
    load_sta_data,
    get_sta_metadata,
    find_sta_header,
    split_sta_header,
    set_metadata,
)


class TestParseSTA(unittest.TestCase):
    def setUp(self):
        self.test_files_dir = "tests/test_files/STA"
        self.csv_file_path = os.path.join(self.test_files_dir, "DF_FILED_VAL_STA_N2_10K_240211_R1.csv")
        self.parquet_file_path = os.path.join(self.test_files_dir, "DF_FILED_VAL_STA_N2_10K_240211_R1.parquet")

    def tearDown(self):
        if os.path.exists(self.parquet_file_path):
            os.remove(self.parquet_file_path)

    def test_load_sta_data(self):
        table = load_sta_data(self.csv_file_path)
        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.num_rows, 1094)
        self.assertEqual(table.num_columns, 7)
        self.assertEqual(table.column_names, ["Temperature", "Time", "Mass", "DSC", "DTG", "Sensitivity", "Segment"])
        self.assertEqual(table.schema.field("Temperature").type, pa.float64())
        self.assertEqual(table.schema.field("Time").type, pa.float64())
        self.assertEqual(table.schema.field("Mass").type, pa.float64())
        self.assertEqual(table.schema.field("DSC").type, pa.float64())
        self.assertEqual(table.schema.field("DTG").type, pa.float64())
        self.assertEqual(table.schema.field("Sensitivity").type, pa.float64())
        self.assertEqual(table.schema.field("Segment").type, pa.int64())

    def test_get_sta_metadata(self):
        metadata = get_sta_metadata(self.csv_file_path, encoding="iso-8859-1", header_end=45)
        self.assertIsInstance(metadata, dict)
        self.assertIn("export_type", metadata)
        self.assertIn("segment", metadata)
        self.assertIn("sample_mass", metadata)


    def test_find_sta_header(self):
        header_end, columns, delimiter = find_sta_header(self.csv_file_path, encoding="iso-8859-1")
        self.assertIsInstance(header_end, int)
        self.assertIsInstance(columns, list)
        self.assertIsInstance(delimiter, str)
        self.assertEqual(header_end, 45)
        self.assertEqual(delimiter, ",")

    def test_split_sta_header(self):
        header = ["column1 /mg", "column2 /C", "column3 /ml/min"]
        column_names, column_units = split_sta_header(header)
        self.assertIsInstance(column_names, list)
        self.assertIsInstance(column_units, list)
        self.assertEqual(len(column_names), len(column_units))
        self.assertEqual(column_names, ["column1", "column2", "column3"])
        self.assertEqual(column_units, ["mg", "C", "ml/min"])

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
        self.assertIsInstance(updated_table, pa.Table)
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
        table = load_sta_data(self.csv_file_path)
        pq.write_table(table, self.parquet_file_path, compression="snappy")
        self.assertTrue(os.path.exists(self.parquet_file_path))

    # def test_metadata_written_to_parquet(self):
    #     table = load_sta_data(self.csv_file_path)
    #     table = set_metadata(table, col_meta={"column1": {"unit": "kg"}, "column2": {"unit": "m"}})
    #     pq.write_table(table, self.parquet_file_path, compression="snappy")
    #     table_read = pq.read_table(self.parquet_file_path)
    #     metadata = table_read.schema.metadata
    #     self.assertIn(b"column1", metadata)
    #     self.assertIn(b"column2", metadata)

if __name__ == "__main__":
    unittest.main()
