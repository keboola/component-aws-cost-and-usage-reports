"""
CUR 2.0 (Modern) report handler implementation.

Handles the new AWS Cost and Usage Report format:
- GZIP compressed files
- BILLING_PERIOD= partitioning structure
- Flat column structure (no categories)
- Manifests in metadata/ subfolder
"""

import gzip
import json
import logging
import os
import re
import tempfile
from calendar import monthrange
from datetime import datetime, date
from typing import Any, Optional

from .base_handler import BaseReportHandler


class CUR2ReportHandler(BaseReportHandler):
    """Handler for CUR 2.0 (modern) format reports."""

    def __init__(self, s3_client, bucket: str, report_prefix: str):
        super().__init__(s3_client, bucket, report_prefix)

    def retrieve_manifests(self, s3_objects: list[dict], report_name: str) -> list[dict[str, Any]]:
        """
        Retrieve and parse CUR 2.0 manifest files from S3 objects.

        CUR 2.0 manifests are located in metadata/BILLING_PERIOD=YYYY-MM/ subfolders.
        """
        manifests = []
        manifest_pattern = f"{report_name}-Manifest.json"

        for s3_object in s3_objects:
            key = s3_object["Key"]

            # CUR 2.0: manifests are in metadata/BILLING_PERIOD=YYYY-MM/
            if "metadata/BILLING_PERIOD=" in key and key.endswith(manifest_pattern):
                try:
                    # Extract billing period from path
                    period_match = re.search(r"BILLING_PERIOD=([^/]+)", key)
                    if not period_match:
                        logging.warning(f"Could not extract billing period from key: {key}")
                        continue

                    period = period_match.group(1)

                    # Download and parse manifest JSON
                    manifest_content = self._read_s3_file_contents(key)
                    manifest = json.loads(manifest_content)

                    # Enrich with metadata for processing
                    manifest["last_modified"] = s3_object["LastModified"]
                    manifest["report_folder"] = self._extract_report_folder(key, period, manifest_pattern)
                    manifest["period"] = period
                    manifest["format_version"] = "2.0"
                    # Ensure assemblyId exists (fallback to reportId or generate one)
                    if "assemblyId" not in manifest:
                        manifest["assemblyId"] = manifest.get(
                            "reportId", f"cur2-{period}-{manifest.get('account', 'unknown')}"
                        )

                    manifests.append(manifest)

                except Exception as e:
                    logging.error(f"Failed to process CUR 2.0 manifest {key}: {e}")
                    continue

        logging.info(f"Found {len(manifests)} CUR 2.0 manifests")
        return manifests

    def get_csv_patterns(self, manifests: list[dict]) -> list[str]:
        """
        Generate CSV file patterns for CUR 2.0 manifests.

        CUR 2.0: Use specific files from manifest reportKeys rather than wildcards.
        """
        patterns = []

        for manifest in manifests:
            base_path = manifest["report_folder"]
            # CUR 2.0 uses 'dataFiles' instead of 'reportKeys'
            data_files = manifest.get("dataFiles", [])
            report_keys = manifest.get("reportKeys", [])  # Fallback for older format
            files_to_process = data_files or report_keys
            logging.info(
                f"Processing manifest: base_path='{base_path}', "
                f"dataFiles={len(data_files)}, reportKeys={len(report_keys)}"
            )

            if files_to_process:
                # Check if files are GZIP - if so, download and extract them locally
                gzip_files = [f for f in files_to_process if f.endswith(".gz")]
                csv_files = [f for f in files_to_process if f.endswith(".csv")]
                # Handle GZIP files - download and extract locally
                for gzip_file in gzip_files:
                    # Extract S3 key from full URL for dataFiles, or use directly for reportKeys
                    if gzip_file.startswith("s3://"):
                        # dataFiles format: s3://bucket/path/file.gz
                        s3_key = gzip_file.replace(f"s3://{self.bucket}/", "")
                    else:
                        # reportKeys format: relative path
                        if base_path:
                            s3_key = f"{base_path}/{gzip_file}"
                        else:
                            s3_key = gzip_file
                    logging.info(f"Attempting to extract GZIP file: {s3_key}")
                    try:
                        extracted_path = self._download_and_extract_gzip(s3_key)
                        patterns.append(extracted_path)
                        logging.info(f"Successfully extracted {s3_key} to {extracted_path}")
                    except Exception as e:
                        logging.error(f"Failed to extract GZIP file {s3_key}: {e}")
                        logging.warning(f"Falling back to S3 pattern for {s3_key} (this may fail with DuckDB)")
                        # Don't add fallback S3 pattern - it will fail anyway
                        # Instead, try alternative approach or skip this file
                        continue
                # Handle direct CSV files
                for csv_file in csv_files:
                    if csv_file.startswith("s3://"):
                        # Use the S3 URL directly
                        patterns.append(csv_file)
                    else:
                        # Build S3 pattern for relative paths
                        if base_path:
                            pattern = f"s3://{self.bucket}/{base_path}/{csv_file}"
                        else:
                            pattern = f"s3://{self.bucket}/{csv_file}"
                        patterns.append(pattern)
            else:
                # Fallback to wildcard pattern if no reportKeys
                period = manifest["period"]
                if base_path:
                    pattern = f"s3://{self.bucket}/{base_path}/data/BILLING_PERIOD={period}/*.csv.gz"
                else:
                    pattern = f"s3://{self.bucket}/data/BILLING_PERIOD={period}/*.csv.gz"
                patterns.append(pattern)

        logging.info(f"Generated {len(patterns)} CSV patterns for CUR 2.0")
        if not patterns:
            logging.error("No CSV patterns could be generated for CUR 2.0 manifests")
            raise ValueError("No valid CSV files found in CUR 2.0 manifests")
        return patterns

    def normalize_columns(self, manifest: dict) -> list[str]:
        """
        Normalize column names for CUR 2.0 format.

        CUR 2.0 uses flat column structure without categories.
        """
        # CUR 2.0: flat column names (no categories)
        manifest_columns = [col["name"] for col in manifest.get("columns", [])]

        return manifest_columns

    def filter_by_date_range(self, manifests: list[dict], since_dt: datetime, until_dt: datetime) -> list[dict]:
        """
        Filter CUR 2.0 manifests by date range.

        A manifest is included if its billing period overlaps with the specified date range.
        For monthly periods (YYYY-MM), the entire month is considered.
        Overlap occurs when: (period_end >= since) AND (period_start <= until)

        Args:
            manifests: List of CUR 2.0 manifest dictionaries
            since_dt: Start of date range (datetime with timezone)
            until_dt: End of date range (datetime with timezone)

        Returns:
            List of manifests whose billing period overlaps with the date range
        """
        if not since_dt or not until_dt:
            return manifests

        filtered_manifests = []
        for manifest in manifests:
            period = manifest.get("period")
            if not period:
                logging.warning(f"Manifest missing period, skipping: {manifest.get('reportId')}")
                continue

            try:
                # Parse period to get start and end dates
                period_start, period_end = self._parse_billing_period_range(period)
                if not period_start or not period_end:
                    logging.warning(f"Could not parse billing period '{period}'")
                    continue

                # Check for overlap: period overlaps if (period_end >= since) AND (period_start <= until)
                if period_end >= since_dt.date() and period_start <= until_dt.date():
                    filtered_manifests.append(manifest)
                    logging.debug(
                        f"Manifest {manifest.get('reportId')} included: "
                        f"period {period} ({period_start} to {period_end}) overlaps with filter range"
                    )

            except (ValueError, TypeError, AttributeError) as e:
                logging.warning(f"Could not parse billing period '{period}': {e}")
                continue

        logging.info(f"Filtered {len(manifests)} manifests to {len(filtered_manifests)} by date range")
        return filtered_manifests

    def _download_and_extract_gzip(self, s3_key: str) -> str:
        """Download and extract a single GZIP file, return path to extracted CSV."""
        try:
            # Download GZIP file content
            gzip_content = self._read_s3_file_contents(s3_key)

            # Create temporary file for GZIP content
            with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as temp_gzip:
                temp_gzip.write(gzip_content)
                temp_gzip_path = temp_gzip.name

            # Extract CSV content from GZIP
            extract_dir = tempfile.mkdtemp()
            csv_filename = os.path.basename(s3_key).replace(".gz", "")
            csv_path = os.path.join(extract_dir, csv_filename)

            with gzip.open(temp_gzip_path, "rb") as gzip_file:
                with open(csv_path, "wb") as csv_file:
                    csv_file.write(gzip_file.read())

            # Cleanup temp GZIP file
            os.unlink(temp_gzip_path)

            logging.debug(f"Successfully extracted GZIP file {s3_key} to {csv_path}")
            return csv_path

        except Exception as e:
            logging.error(f"Failed to extract GZIP file {s3_key}: {e}")
            raise

    def _extract_report_folder(self, manifest_key: str, period: str, manifest_pattern: str) -> str:
        """
        Extract the base report folder from manifest key.

        Removes the metadata/BILLING_PERIOD=YYYY-MM/manifest.json part to get base folder.
        """
        # Remove metadata/BILLING_PERIOD=period/manifest_pattern from the end
        suffix_to_remove = f"/metadata/BILLING_PERIOD={period}/{manifest_pattern}"
        if manifest_key.endswith(suffix_to_remove):
            return manifest_key[: -len(suffix_to_remove)]

        # Fallback: try to extract base path
        parts = manifest_key.split("/")
        if len(parts) >= 3:
            # Assume structure: base/folder/metadata/BILLING_PERIOD=period/manifest
            # Find 'metadata' and take everything before it
            try:
                metadata_index = parts.index("metadata")
                return "/".join(parts[:metadata_index])
            except ValueError:
                pass

        # Last resort: remove last 3 parts (metadata/BILLING_PERIOD=period/manifest)
        return "/".join(parts[:-3]) if len(parts) > 3 else "/".join(parts[:-1])

    def _parse_billing_period(self, period: str) -> Optional[date]:
        """
        Parse CUR 2.0 billing period to start date.

        Supports various formats:
        - YYYY-MM (monthly) -> first day of month
        - YYYY-MM-DD (daily) -> that specific day
        - YYYY (yearly) -> first day of year
        - YYYYWXX (weekly) -> Monday of the specified ISO week

        Args:
            period: Billing period string

        Returns:
            Start date of the period, or None if parsing fails
        """
        try:
            if re.match(r"^\d{4}-\d{2}-\d{2}$", period):
                # Daily: YYYY-MM-DD
                return datetime.strptime(period, "%Y-%m-%d").date()
            elif re.match(r"^\d{4}-\d{2}$", period):
                # Monthly: YYYY-MM (use first day of month)
                return datetime.strptime(period + "-01", "%Y-%m-%d").date()
            elif re.match(r"^\d{4}$", period):
                # Yearly: YYYY (use first day of year)
                return datetime.strptime(period + "-01-01", "%Y-%m-%d").date()
            elif "W" in period:
                # Weekly format: YYYYWXX (e.g., 2025W03)
                week_match = re.match(r"^(\d{4})W(\d{2})$", period)
                if week_match:
                    year = int(week_match.group(1))
                    week = int(week_match.group(2))
                    return date.fromisocalendar(year, week, 1)

        except ValueError as e:
            logging.error(f"Failed to parse billing period '{period}': {e}")

        return None

    def _parse_billing_period_range(self, period: str) -> tuple[Optional[date], Optional[date]]:
        """
        Parse CUR 2.0 billing period to date range (start, end).

        Supports various formats:
        - YYYY-MM (monthly) -> first day to last day of month
        - YYYY-MM-DD (daily) -> that day to that day
        - YYYY (yearly) -> first day to last day of year
        - YYYYWXX (weekly) -> Monday to Sunday of the specified ISO week

        Args:
            period: Billing period string (e.g., "2024-09")

        Returns:
            Tuple of (start_date, end_date), or (None, None) if parsing fails
        """
        try:
            if re.match(r"^\d{4}-\d{2}-\d{2}$", period):
                # Daily: YYYY-MM-DD
                date_obj = datetime.strptime(period, "%Y-%m-%d").date()
                return (date_obj, date_obj)

            elif re.match(r"^\d{4}-\d{2}$", period):
                # Monthly: YYYY-MM
                year, month = map(int, period.split("-"))
                start_date = datetime(year, month, 1).date()

                # Calculate last day of month using monthrange
                last_day = monthrange(year, month)[1]
                end_date = datetime(year, month, last_day).date()

                return (start_date, end_date)

            elif re.match(r"^\d{4}$", period):
                # Yearly: YYYY
                year = int(period)
                start_date = datetime(year, 1, 1).date()
                end_date = datetime(year, 12, 31).date()
                return (start_date, end_date)

            elif "W" in period:
                # Weekly format: YYYYWXX (e.g., 2025W03)
                week_match = re.match(r"^(\d{4})W(\d{2})$", period)
                if week_match:
                    year = int(week_match.group(1))
                    week = int(week_match.group(2))
                    start_date = date.fromisocalendar(year, week, 1)
                    end_date = date.fromisocalendar(year, week, 7)
                    return (start_date, end_date)

        except ValueError as e:
            logging.error(f"Failed to parse billing period range '{period}': {e}")

        return (None, None)
