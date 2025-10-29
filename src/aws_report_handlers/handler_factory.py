"""
Factory for creating appropriate report format handlers.
"""

import logging

import boto3

from .base_handler import BaseReportHandler
from .cur_1_report_handler import CUR1ReportHandler
from .cur_2_report_handler import CUR2ReportHandler
from .version_detector import ReportVersionDetector


class ReportHandlerFactory:
    """Factory for creating appropriate report format handlers."""

    @staticmethod
    def create_handler(
        s3_client: boto3.client,
        bucket: str,
        report_prefix: str,
        s3_objects: list[dict] | None = None,
    ) -> BaseReportHandler:
        """
        Create handler based on detected report format.

        Args:
            s3_client: S3 client for operations
            bucket: S3 bucket name
            report_prefix: Report prefix/path
            s3_objects: Optional S3 objects for format detection (will fetch if not provided)

        Returns:
            Appropriate handler instance (CUR1ReportHandler or CUR2ReportHandler)
        """
        # If no S3 objects provided, create a temporary handler to fetch them
        if s3_objects is None:
            temp_handler = CUR1ReportHandler(s3_client, bucket, report_prefix)
            s3_objects = list(temp_handler.get_s3_objects())

        version_type = ReportVersionDetector.detect_version(s3_objects)

        if version_type == "modern":
            logging.info("Creating CUR 2.0 report handler")
            handler = CUR2ReportHandler(s3_client, bucket, report_prefix)
        else:
            logging.info("Creating CUR 1.0 report handler")
            handler = CUR1ReportHandler(s3_client, bucket, report_prefix)

        return handler

    @staticmethod
    def get_supported_formats() -> list[str]:
        """
        Get list of supported report formats.

        Returns:
            List of supported format names
        """
        return ["legacy", "modern"]

    @staticmethod
    def get_format_info() -> dict[str, str]:
        """
        Get information about supported formats.

        Returns:
            Dictionary mapping format names to descriptions
        """
        return {
            "legacy": "CUR 1.0 format - ZIP files, date-based folders (YYYYMMDD-YYYYMMDD)",
            "modern": "CUR 2.0 format - GZIP files, billing period partitions (BILLING_PERIOD=YYYY-MM)",
        }
