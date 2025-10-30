"""
Factory for creating appropriate report format handlers.
"""

import logging

import boto3

from .base_handler import BaseReportHandler
from .cur_1_report_handler import CUR1ReportHandler
from .cur_2_report_handler import CUR2ReportHandler


class ReportHandlerFactory:
    """Factory for creating appropriate report format handlers."""

    @staticmethod
    def create_handler(
        s3_client: boto3.client,
        bucket: str,
        report_prefix: str,
        s3_objects: list[dict] | None = None,
        version: str | None = None,
    ) -> BaseReportHandler:
        """
        Create handler based on specified version or detected report format.

        Args:
            s3_client: S3 client for operations
            bucket: S3 bucket name
            report_prefix: Report prefix/path
            s3_objects: Optional S3 objects for format detection (will fetch if not provided)
            version: Optional version override ('cur2' for CUR 2.0, None defaults to CUR 1.0)

        Returns:
            Appropriate handler instance (CUR1ReportHandler or CUR2ReportHandler)
        """
        # If version is explicitly specified, use it
        if version == "cur2":
            logging.info("Creating CUR 2.0 report handler (version explicitly set)")
            handler = CUR2ReportHandler(s3_client, bucket, report_prefix)
        elif version in (None, "", "cur1"):  # Default to CUR 1.0 when no version specified
            logging.info("Creating CUR 1.0 report handler (default version)")
            handler = CUR1ReportHandler(s3_client, bucket, report_prefix)
        else:
            # Invalid version parameter
            raise ValueError(f"Invalid version parameter: {version}. Use 'cur2' or None.")

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
