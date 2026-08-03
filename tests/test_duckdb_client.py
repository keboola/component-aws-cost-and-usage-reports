import csv
import os
import tempfile
import unittest

from duckdb_client import DuckDBClient


class TestDuckDBClient(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def _write_csv(self, name, rows):
        path = os.path.join(self.tmp_dir, name)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        return path

    def _read_csv(self, path):
        with open(path, encoding="utf-8") as f:
            return list(csv.reader(f))

    def test_case_differing_columns_kept_distinct(self):
        """Source columns differing only in letter case must not be collapsed (SUPPORT-17124)."""
        header = ["identity/LineItemId", "resourceTags/user:Team", "resourceTags/user:team"]
        chunk1 = self._write_csv("chunk1.csv", [header, ["li1", "platform", "pod-a"], ["li2", "data", "pod-b"]])
        chunk2 = self._write_csv("chunk2.csv", [header, ["li3", "ml", "pod-c"]])

        # Normalized, case-insensitively unique names as produced by component.py
        column_names = ["identity__LineItemId", "resourceTags__user_Team", "resourceTags__user_team_1"]

        client = DuckDBClient()
        try:
            client.create_table("report", [{"name": c, "type": "TEXT"} for c in column_names])
            client.load_csv_file("report", column_names, chunk1)
            client.load_csv_file("report", column_names, chunk2)
            out_path = os.path.join(self.tmp_dir, "out.csv")
            client.export_to_csv("report", out_path, column_names)
        finally:
            client.close()

        rows = self._read_csv(out_path)
        self.assertEqual(rows[0], column_names)
        data = {r[0]: r[1:] for r in rows[1:]}
        # The two case-differing tags keep their own, distinct values.
        self.assertEqual(data["li1"], ["platform", "pod-a"])
        self.assertEqual(data["li2"], ["data", "pod-b"])
        self.assertEqual(data["li3"], ["ml", "pod-c"])

    def test_missing_columns_filled_with_null(self):
        """Columns absent from some files (or all files) are filled with NULL."""
        chunk_full = self._write_csv(
            "full.csv",
            [["identity/LineItemId", "resourceTags/user:Team"], ["li1", "platform"]],
        )
        chunk_partial = self._write_csv("partial.csv", [["identity/LineItemId"], ["li2"]])

        client = DuckDBClient()
        try:
            client.create_table("report", [])
            client.load_csv_file("report", ["identity__LineItemId", "resourceTags__user_Team"], chunk_full)
            client.load_csv_file("report", ["identity__LineItemId"], chunk_partial)
            out_path = os.path.join(self.tmp_dir, "out.csv")
            # "extra_col" is present in no file and must be NULL-filled.
            header = ["identity__LineItemId", "resourceTags__user_Team", "extra_col"]
            client.export_to_csv("report", out_path, header)
        finally:
            client.close()

        rows = self._read_csv(out_path)
        self.assertEqual(rows[0], ["identity__LineItemId", "resourceTags__user_Team", "extra_col"])
        data = {r[0]: r[1:] for r in rows[1:]}
        self.assertEqual(data["li1"], ["platform", ""])
        self.assertEqual(data["li2"], ["", ""])

    def test_file_shorter_than_names_does_not_abort(self):
        """A chunk with fewer physical columns than its manifest must not abort the export.

        Both chunks share the same (manifest-derived) column_names, so they land in one
        read group; the short chunk's missing trailing column is NULL-padded instead of
        raising an error (SUPPORT-17124 follow-up)."""
        column_names = ["identity__LineItemId", "resourceTags__user_Team", "resourceTags__user_team_1"]
        chunk_full = self._write_csv(
            "full.csv",
            [
                ["identity/LineItemId", "resourceTags/user:Team", "resourceTags/user:team"],
                ["li1", "platform", "pod-a"],
            ],
        )
        # Physically only two columns although the manifest declares three.
        chunk_short = self._write_csv(
            "short.csv",
            [["identity/LineItemId", "resourceTags/user:Team"], ["li2", "data"]],
        )

        client = DuckDBClient()
        try:
            client.create_table("report", [{"name": c, "type": "TEXT"} for c in column_names])
            client.load_csv_file("report", column_names, chunk_full)
            client.load_csv_file("report", column_names, chunk_short)
            out_path = os.path.join(self.tmp_dir, "out.csv")
            client.export_to_csv("report", out_path, column_names)
        finally:
            client.close()

        rows = self._read_csv(out_path)
        self.assertEqual(rows[0], column_names)
        data = {r[0]: r[1:] for r in rows[1:]}
        self.assertEqual(data["li1"], ["platform", "pod-a"])
        # Missing third column padded with NULL (empty CSV field).
        self.assertEqual(data["li2"], ["data", ""])


if __name__ == "__main__":
    unittest.main()
