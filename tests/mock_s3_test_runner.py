#!/usr/bin/env python3
"""
Mock S3 Test Runner for AWS Cost and Usage Reports Component

Tests both CUR 1.0 and CUR 2.0 formats using local mock data.
"""

import json
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import MagicMock, Mock

# Add src directory to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / "src"))

from aws_report_handlers import ReportHandlerFactory
from configuration import Configuration


class MockS3Client:
    """Mock S3 client that reads from local filesystem."""

    def __init__(self, mock_data_path: str):
        self.mock_data_path = Path(mock_data_path)
        self.region_name = "us-east-1"

    def list_objects_v2(self, Bucket: str, Prefix: str = "", **kwargs):
        """Simulate S3 list_objects_v2 operation."""
        bucket_path = self.mock_data_path / Bucket
        if not bucket_path.exists():
            return {"Contents": []}

        contents = []
        for file_path in bucket_path.rglob("*"):
            if file_path.is_file():
                # Convert local path to S3-like key
                relative_path = file_path.relative_to(bucket_path)
                key = str(relative_path).replace(os.sep, "/")

                # More flexible prefix matching for mock data
                if not Prefix or Prefix in key or key.startswith(Prefix):
                    contents.append(
                        {
                            "Key": key,
                            "LastModified": datetime.fromtimestamp(
                                file_path.stat().st_mtime
                            ),
                            "Size": file_path.stat().st_size,
                        }
                    )

        return {"Contents": contents}

    def get_object(self, Bucket: str, Key: str):
        """Simulate S3 get_object operation."""
        file_path = self.mock_data_path / Bucket / Key
        if not file_path.exists():
            raise Exception(f"NoSuchKey: {Key}")

        class MockBody:
            def __init__(self, content: bytes):
                self.content = content

            def read(self):
                return self.content

        with open(file_path, "rb") as f:
            content = f.read()

        return {"Body": MockBody(content)}


def test_cur_format(
    test_name: str, mock_data_path: str, config_path: str, expected_output_path: str
):
    """Test a specific CUR format with mock data."""
    print(f"\n🧪 Testing {test_name}")
    print("=" * 50)

    try:
        # Load test configuration
        with open(config_path, "r") as f:
            config_data = json.load(f)

        # Create mock S3 client
        mock_s3_client = MockS3Client(mock_data_path)
        bucket = config_data["parameters"]["aws_parameters"]["s3_bucket"]
        prefix = config_data["parameters"]["report_path_prefix"]

        print(f"📁 Mock S3 bucket: {bucket}")
        print(f"🔍 Report prefix: {prefix}")

        # List S3 objects
        response = mock_s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        s3_objects = response.get("Contents", [])

        print(f"📄 Found {len(s3_objects)} mock S3 objects:")
        for obj in s3_objects:
            print(f"  • {obj['Key']}")

        if not s3_objects:
            print("❌ No mock objects found")
            return False

        # Create handler using factory
        handler = ReportHandlerFactory.create_handler(
            s3_client=mock_s3_client, 
            bucket=bucket, 
            report_prefix=prefix,
            s3_objects=s3_objects
        )
        print(f"🏗️ Created handler: {type(handler).__name__}")

        # Retrieve manifests
        report_name = prefix
        manifests = handler.retrieve_manifests(s3_objects, report_name)
        print(f"📋 Retrieved {len(manifests)} manifests")

        # Debug: show what handler is looking for
        if len(manifests) == 0 and s3_objects:
            print("🔍 Debug - Handler search criteria:")
            print(f"  • Report name: {report_name}")
            print(f"  • Expected manifest pattern: {report_name}-Manifest.json")
            print("  • S3 object keys:")
            for obj in s3_objects:
                print(f"    - {obj['Key']}")
            if hasattr(handler, "retrieve_manifests"):
                print("  • Handler type has retrieve_manifests method")

        if not manifests:
            print("❌ No manifests retrieved")
            return False

        # Show manifest info
        for i, manifest in enumerate(manifests):
            print(f"  📄 Manifest {i + 1}:")
            print(f"    • Period: {manifest.get('period', 'unknown')}")
            print(
                f"    • Format version: {manifest.get('format_version', manifest.get('cur_version', 'unknown'))}"
            )
            print(f"    • Columns: {len(manifest.get('columns', []))}")

        # Test column normalization
        sample_manifest = manifests[0]
        normalized_columns = handler.normalize_columns(sample_manifest)
        print(f"📝 Normalized columns: {len(normalized_columns)}")
        print(f"  First 5: {normalized_columns[:5]}")

        # Test CSV pattern generation
        csv_patterns = handler.get_csv_patterns(manifests)
        print(f"📁 Generated {len(csv_patterns)} CSV patterns:")
        for pattern in csv_patterns:
            print(f"  • {pattern}")

        # Validate patterns
        valid_patterns = []
        for pattern in csv_patterns:
            if pattern.startswith("s3://"):
                print(f"✅ Valid S3 pattern: {os.path.basename(pattern)}")
                valid_patterns.append(pattern)
            elif os.path.exists(pattern):
                print(f"✅ Valid local file: {os.path.basename(pattern)}")
                valid_patterns.append(pattern)
            else:
                print(f"❌ Invalid pattern: {pattern}")

        if valid_patterns:
            print(
                f"✅ {test_name} test PASSED - {len(valid_patterns)} valid patterns generated"
            )
            return True
        else:
            print(f"❌ {test_name} test FAILED - no valid patterns")
            return False

    except Exception as e:
        print(f"❌ {test_name} test ERROR: {e}")
        logging.error(f"Test {test_name} failed", exc_info=True)
        return False


def main():
    """Run all mock S3 tests."""
    print("🚀 AWS Cost and Usage Reports Mock S3 Tests")
    print("=" * 60)

    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Test paths
    base_path = Path(__file__).parent
    mock_data_path = base_path / "mock_data"

    test_results = []

    # Test CUR 1.0
    cur1_result = test_cur_format(
        test_name="CUR 1.0 (Legacy ZIP)",
        mock_data_path=str(mock_data_path / "cur_1_0" / "s3_structure"),
        config_path=str(base_path / "functional_tests" / "cur_1_0" / "config.json"),
        expected_output_path=str(
            base_path / "functional_tests" / "cur_1_0" / "expected_output.csv"
        ),
    )
    test_results.append(("CUR 1.0", cur1_result))

    # Test CUR 2.0
    cur2_result = test_cur_format(
        test_name="CUR 2.0 (Modern GZIP)",
        mock_data_path=str(mock_data_path / "cur_2_0" / "s3_structure"),
        config_path=str(base_path / "functional_tests" / "cur_2_0" / "config.json"),
        expected_output_path=str(
            base_path / "functional_tests" / "cur_2_0" / "expected_output.csv"
        ),
    )
    test_results.append(("CUR 2.0", cur2_result))

    # Summary
    print("\n📊 Test Results Summary")
    print("=" * 30)
    passed = 0
    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{len(test_results)} tests passed")

    if passed == len(test_results):
        print("🎉 All tests passed! Component supports both CUR formats.")
        return True
    else:
        print("⚠️  Some tests failed. Check the logs above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
