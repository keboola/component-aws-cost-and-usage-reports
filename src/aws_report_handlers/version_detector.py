"""
Version detector for AWS Cost and Usage Reports.
"""

import logging
import re
from typing import Any


class ReportVersionDetector:
    """Detects AWS Cost and Usage Report version from S3 structure."""

    @staticmethod
    def detect_version(s3_objects: list[dict]) -> str:
        """
        Detect CUR report version from S3 object paths.

        Args:
            s3_objects: List of S3 objects from list_objects_v2

        Returns:
            str: "legacy" for CUR 1.0 or "modern" for CUR 2.0
        """
        if not s3_objects:
            logging.warning("No S3 objects provided for version detection")
            return "legacy"
        logging.info(f"[VERSION DETECTION] Analyzing {len(s3_objects)} S3 objects")
        sample_keys = [obj["Key"] for obj in s3_objects[:5]]
        logging.info(f"[VERSION DETECTION] Sample keys: {sample_keys}")
        has_billing_period = any("BILLING_PERIOD=" in obj["Key"] for obj in s3_objects)
        logging.info(f"[VERSION DETECTION] has_billing_period (CUR 2.0 indicator): {has_billing_period}")
        if has_billing_period:
            billing_period_keys = [obj["Key"] for obj in s3_objects if "BILLING_PERIOD=" in obj["Key"]][:3]
            logging.info(f"[VERSION DETECTION] BILLING_PERIOD keys: {billing_period_keys}")
            logging.info("Detected modern report format (CUR 2.0) - found BILLING_PERIOD= partitioning")
            return "modern"
        has_date_pattern = any(re.search(r"\d{8}-\d{8}", obj["Key"]) for obj in s3_objects)
        logging.info(f"[VERSION DETECTION] has_date_pattern (CUR 1.0 indicator): {has_date_pattern}")
        if has_date_pattern:
            date_pattern_keys = [obj["Key"] for obj in s3_objects if re.search(r"\d{8}-\d{8}", obj["Key"])][:3]
            logging.info(f"[VERSION DETECTION] Date pattern keys: {date_pattern_keys}")
            logging.info("Detected legacy report format (CUR 1.0) - found date patterns")
            return "legacy"
        has_metadata_folder = any("/metadata/" in obj["Key"] for obj in s3_objects)
        logging.info(f"[VERSION DETECTION] has_metadata_folder (CUR 2.0 indicator): {has_metadata_folder}")
        if has_metadata_folder:
            metadata_keys = [obj["Key"] for obj in s3_objects if "/metadata/" in obj["Key"]][:3]
            logging.info(f"[VERSION DETECTION] Metadata folder keys: {metadata_keys}")
            logging.info("Detected modern report format (CUR 2.0) - found metadata folder")
            return "modern"
        has_zip_files = any(obj["Key"].endswith(".csv.zip") for obj in s3_objects)
        logging.info(f"[VERSION DETECTION] has_zip_files (CUR 1.0 indicator): {has_zip_files}")
        if has_zip_files:
            zip_keys = [obj["Key"] for obj in s3_objects if obj["Key"].endswith(".csv.zip")][:3]
            logging.info(f"[VERSION DETECTION] ZIP file keys: {zip_keys}")
            logging.info("Detected legacy report format (CUR 1.0) - found ZIP files")
            return "legacy"
        logging.info("[VERSION DETECTION] No specific indicators found, defaulting to legacy (CUR 1.0)")
        return "legacy"

    @staticmethod
    def get_version_details(s3_objects: list[dict]) -> dict[str, Any]:
        """
        Get detailed information about detected CUR version.

        Args:
            s3_objects: List of S3 objects from list_objects_v2

        Returns:
            Dictionary with version details
        """
        version_type = ReportVersionDetector.detect_version(s3_objects)

        details = {
            "version": version_type,
            "total_objects": len(s3_objects),
            "sample_paths": [obj["Key"] for obj in s3_objects[:3]],  # First 3 paths
        }

        if version_type == "modern":
            # Extract billing periods
            billing_periods = set()
            for obj in s3_objects:
                match = re.search(r"BILLING_PERIOD=([^/]+)", obj["Key"])
                if match:
                    billing_periods.add(match.group(1))
            details["billing_periods"] = sorted(list(billing_periods))

            # Check for GZIP files
            details["has_gzip"] = any(obj["Key"].endswith(".csv.gz") for obj in s3_objects)
            details["has_metadata_folder"] = any("/metadata/" in obj["Key"] for obj in s3_objects)

        else:  # legacy
            # Extract date patterns
            date_patterns = set()
            for obj in s3_objects:
                match = re.search(r"(\d{8}-\d{8})", obj["Key"])
                if match:
                    date_patterns.add(match.group(1))
            details["date_patterns"] = sorted(list(date_patterns))

            # Check for ZIP files
            details["has_zip"] = any(obj["Key"].endswith(".csv.zip") for obj in s3_objects)

        return details
