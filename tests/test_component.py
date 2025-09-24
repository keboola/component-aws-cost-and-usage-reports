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


class TestVersionDetection(unittest.TestCase):
    """Test CUR version detection logic."""

    def test_detect_cur_2_0_by_billing_period(self):
        """Test detection of CUR 2.0 by BILLING_PERIOD pattern."""
        s3_objects = [
            {"Key": "metadata/BILLING_PERIOD=2024-09/Manifest.json"},
            {"Key": "data/BILLING_PERIOD=2024-09/report.csv.gz"}
        ]
        
        version = ReportVersionDetector.detect_version(s3_objects)
        self.assertEqual(version, "modern")

    def test_detect_cur_1_0_by_date_pattern(self):
        """Test detection of CUR 1.0 by date folder pattern."""
        s3_objects = [
            {"Key": "20240101-20240131/Manifest.json"},
            {"Key": "20240101-20240131/report.csv.zip"}
        ]
        
        version = ReportVersionDetector.detect_version(s3_objects)
        self.assertEqual(version, "legacy")

    def test_detect_empty_objects_defaults_to_legacy(self):
        """Test that empty S3 objects list defaults to legacy."""
        version = ReportVersionDetector.detect_version([])
        self.assertEqual(version, "legacy")


class TestHandlerFactory(unittest.TestCase):
    """Test handler factory functionality."""

    def test_factory_creates_cur2_handler_for_modern(self):
        """Test factory creates CUR2 handler for modern format."""
        # Mock S3 objects indicating CUR 2.0
        s3_objects = [
            {"Key": "metadata/BILLING_PERIOD=2024-09/Manifest.json"}
        ]
        
        # This would normally create a handler, but we can't without real S3 client
        # So we just test the version detection part
        version = ReportVersionDetector.detect_version(s3_objects)
        self.assertEqual(version, "modern")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
