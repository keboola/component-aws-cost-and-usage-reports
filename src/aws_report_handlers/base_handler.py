"""
Base abstract class for AWS Cost and Usage Report handlers.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError


class BaseReportHandler(ABC):
    """Abstract base class for Cost and Usage Report handlers."""

    def __init__(self, s3_client: boto3.client, bucket: str, report_prefix: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self.report_prefix = self._cleanup_report_prefix(report_prefix)

    def _cleanup_report_prefix(self, prefix: str) -> str:
        """Clean and normalize the report prefix."""
        if prefix.endswith("/"):
            prefix = prefix[:-1]
        if not prefix.endswith("*"):
            prefix = prefix + "*"
        return prefix

    def get_report_name(self) -> str:
        """Extract clean report name from the configured prefix."""
        return self.report_prefix.split("/")[-1].replace("*", "")

    def get_s3_objects(self, since_dt=None, until_dt=None):
        """Get S3 objects matching the prefix and optional date filters."""
        prefix = self.report_prefix
        if prefix.endswith("*"):
            is_wildcard = True
            prefix = prefix[:-1]
        else:
            is_wildcard = False

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
        except ClientError as error:
            logging.error(f"Error occurred while listing S3 objects: {error}")
            raise

        params = dict(
            Bucket=self.bucket,
            Prefix=prefix,
            PaginationConfig={"MaxItems": 100000, "PageSize": 1000},
        )
        pages = paginator.paginate(**params)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if since_dt and obj["LastModified"] <= since_dt:
                    continue
                if until_dt and obj["LastModified"] > until_dt:
                    continue
                if (is_wildcard and key.startswith(prefix)) or key == prefix:
                    yield obj

    @abstractmethod
    def retrieve_manifests(self, s3_objects: list[dict], report_name: str) -> list[dict[str, Any]]:
        """
        Retrieve and parse manifests from S3 objects.

        Args:
            s3_objects: List of S3 objects from list_objects_v2
            report_name: Name of the report to process

        Returns:
            List of manifest dictionaries with metadata
        """
        pass

    @abstractmethod
    def get_csv_patterns(self, manifests: list[dict]) -> list[str]:
        """
        Generate CSV file patterns from manifests.

        Args:
            manifests: List of manifest dictionaries

        Returns:
            List of S3 patterns (s3://bucket/path/*.csv or local paths)
        """
        pass

    @abstractmethod
    def normalize_columns(self, manifest: dict) -> list[str]:
        """
        Normalize column names from manifest.

        Args:
            manifest: Manifest dictionary

        Returns:
            List of normalized column names
        """
        pass

    @abstractmethod
    def filter_by_date_range(self, manifests: list[dict], since_dt: datetime, until_dt: datetime) -> list[dict]:
        """
        Filter manifests by date range.

        Args:
            manifests: List of manifest dictionaries
            since_dt: Start datetime (datetime with timezone)
            until_dt: End datetime (datetime with timezone)

        Returns:
            Filtered list of manifests that overlap with the date range
        """
        pass

    def _read_s3_file_contents(self, key: str) -> bytes:
        """Read file contents from S3."""
        if not self.s3_client or not self.bucket:
            raise ValueError("S3 client and bucket must be set before reading files")

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read()
        except Exception as error:
            raise RuntimeError(f"Failed to read S3 object {key}: {error}")
