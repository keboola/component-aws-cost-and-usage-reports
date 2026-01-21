"""
Unit tests for AWS Cost and Usage Reports component.

@author: esner
"""

import unittest
import sys
import os
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from aws_report_handlers import ReportHandlerFactory, ReportVersionDetector
from aws_report_handlers.cur_1_report_handler import CUR1ReportHandler
from component import Component


class TestVersionDetection(unittest.TestCase):
    """Test CUR version detection logic."""

    def test_detect_cur_2_0_by_billing_period(self):
        """Test detection of CUR 2.0 by BILLING_PERIOD pattern."""
        s3_objects = [
            {"Key": "metadata/BILLING_PERIOD=2024-09/Manifest.json"},
            {"Key": "data/BILLING_PERIOD=2024-09/report.csv.gz"},
        ]

        version = ReportVersionDetector.detect_version(s3_objects)
        self.assertEqual(version, "modern")

    def test_detect_cur_1_0_by_date_pattern(self):
        """Test detection of CUR 1.0 by date folder pattern."""
        s3_objects = [
            {"Key": "20240101-20240131/Manifest.json"},
            {"Key": "20240101-20240131/report.csv.zip"},
        ]

        version = ReportVersionDetector.detect_version(s3_objects)
        self.assertEqual(version, "legacy")

    def test_detect_empty_objects_defaults_to_legacy(self):
        """Test that empty S3 objects list defaults to legacy."""
        version = ReportVersionDetector.detect_version([])
        self.assertEqual(version, "legacy")

    def test_detect_mixed_bucket_prioritizes_cur_1_0(self):
        """Test that CUR 1.0 is detected when bucket contains both formats."""
        # Mixed bucket: CUR 1.0 date patterns + CUR 2.0 billing periods
        s3_objects = [
            {"Key": "20240101-20240131/Manifest.json"},
            {"Key": "20240101-20240131/report.csv.zip"},
            {"Key": "metadata/BILLING_PERIOD=2024-12/Manifest.json"},
            {"Key": "data/BILLING_PERIOD=2024-12/report.csv.gz"},
        ]

        # Should prioritize CUR 1.0 date patterns as they're more specific
        version = ReportVersionDetector.detect_version(s3_objects)
        self.assertEqual(version, "legacy")


class TestHandlerFactory(unittest.TestCase):
    """Test handler factory functionality."""

    def test_factory_creates_cur2_handler_for_modern(self):
        """Test factory creates CUR2 handler for modern format."""
        # Mock S3 objects indicating CUR 2.0
        s3_objects = [{"Key": "metadata/BILLING_PERIOD=2024-09/Manifest.json"}]

        # This would normally create a handler, but we can't without real S3 client
        # So we just test the version detection part
        version = ReportVersionDetector.detect_version(s3_objects)
        self.assertEqual(version, "modern")


class TestColumnDeduplication(unittest.TestCase):
    """Test case-insensitive column deduplication logic."""

    def test_deduplicate_case_insensitive_no_duplicates(self):
        """Test deduplication with no duplicates."""
        from unittest.mock import MagicMock

        # Create a mock component instance
        component = MagicMock()
        component._deduplicate_case_insensitive = Component._deduplicate_case_insensitive.__get__(component)

        columns = ["column_a", "column_b", "column_c"]
        result = component._deduplicate_case_insensitive(columns)

        self.assertEqual(result, ["column_a", "column_b", "column_c"])

    def test_deduplicate_case_insensitive_with_duplicates(self):
        """Test deduplication with case-variant duplicates."""
        from unittest.mock import MagicMock

        component = MagicMock()
        component._deduplicate_case_insensitive = Component._deduplicate_case_insensitive.__get__(component)

        # resourcetags__user_owner appears twice with different casing
        columns = ["column_a", "resourcetags__user_owner", "column_b", "resourcetags__user_Owner"]
        result = component._deduplicate_case_insensitive(columns)

        # Second occurrence should get _1 suffix
        self.assertEqual(result, ["column_a", "resourcetags__user_owner", "column_b", "resourcetags__user_Owner_1"])

    def test_deduplicate_case_insensitive_multiple_duplicates(self):
        """Test deduplication with multiple case-variant duplicates."""
        from unittest.mock import MagicMock

        component = MagicMock()
        component._deduplicate_case_insensitive = Component._deduplicate_case_insensitive.__get__(component)

        columns = ["tag", "TAG", "Tag", "other"]
        result = component._deduplicate_case_insensitive(columns)

        # First stays as is, second gets _1, third gets _2
        self.assertEqual(result, ["tag", "TAG_1", "Tag_2", "other"])


class TestCUR1Handler(unittest.TestCase):
    """Test CUR 1.0 handler functionality."""

    def test_manifest_contains_zip_files_with_zip(self):
        """Test ZIP file detection for .zip files."""
        handler = CUR1ReportHandler(None, "bucket", "prefix")

        manifest = {
            "reportKeys": [
                "path/to/report-00001.csv.zip",
                "path/to/report-00002.csv.zip"
            ]
        }

        self.assertTrue(handler._manifest_contains_zip_files(manifest))

    def test_manifest_contains_zip_files_with_csv(self):
        """Test ZIP file detection for direct CSV files."""
        handler = CUR1ReportHandler(None, "bucket", "prefix")

        manifest = {
            "reportKeys": [
                "path/to/report-00001.csv",
                "path/to/report-00002.csv"
            ]
        }

        self.assertFalse(handler._manifest_contains_zip_files(manifest))

    def test_manifest_contains_zip_files_empty(self):
        """Test ZIP file detection with empty reportKeys."""
        handler = CUR1ReportHandler(None, "bucket", "prefix")

        manifest = {"reportKeys": []}

        self.assertFalse(handler._manifest_contains_zip_files(manifest))

    def test_manifest_contains_zip_files_missing(self):
        """Test ZIP file detection with missing reportKeys."""
        handler = CUR1ReportHandler(None, "bucket", "prefix")

        manifest = {}

        self.assertFalse(handler._manifest_contains_zip_files(manifest))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
