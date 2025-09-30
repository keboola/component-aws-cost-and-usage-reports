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
from datetime import datetime
from typing import Any

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
                    manifest["report_folder"] = self._extract_report_folder(
                        key, period, manifest_pattern
                    )
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
                        logging.warning(
                            f"Falling back to S3 pattern for {s3_key} (this may fail with DuckDB)"
                        )
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
                    pattern = (
                        f"s3://{self.bucket}/{base_path}/data/BILLING_PERIOD={period}/*.csv.gz"
                    )
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

    def filter_by_date_range(
        self, manifests: list[dict], since_timestamp, until_timestamp
    ) -> list[dict]:
        """
        Filter CUR 2.0 manifests by date range.

        Uses BILLING_PERIOD information from the manifest period.
        """
        if not since_timestamp or not until_timestamp:
            return manifests

        filtered_manifests = []
        for manifest in manifests:
            period = manifest.get("period")
            if not period:
                continue

            try:
                # Parse period (YYYY-MM format) to date
                period_date = self._parse_billing_period(period)
                if not period_date:
                    continue

                # Check if period falls within date range
                if since_timestamp.date() <= period_date <= until_timestamp.date():
                    filtered_manifests.append(manifest)

            except (ValueError, TypeError) as e:
                logging.warning(f"Could not parse billing period '{period}': {e}")
                continue

        logging.info(
            f"Filtered {len(manifests)} manifests to {len(filtered_manifests)} by date range"
        )
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

    def _parse_billing_period(self, period: str) -> datetime.date:
        """
        Parse CUR 2.0 billing period to date.

        Supports various formats:
        - YYYY-MM (monthly)
        - YYYY-MM-DD (daily)
        - YYYY (yearly)
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
                # Weekly format: YYYY-WXX (not fully supported yet)
                logging.warning(f"Weekly billing period format not fully supported: {period}")
                # Try to extract year and approximate
                year_match = re.match(r"^(\d{4})", period)
                if year_match:
                    return datetime.strptime(year_match.group(1) + "-01-01", "%Y-%m-%d").date()

        except ValueError as e:
            logging.error(f"Failed to parse billing period '{period}': {e}")

        return None
