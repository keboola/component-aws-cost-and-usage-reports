"""
CUR 1.0 (Legacy) report handler implementation.

Handles the original AWS Cost and Usage Report format:
- ZIP compressed files
- Date-based folder structure (YYYYMMDD-YYYYMMDD)
- Category/name column structure
- Manifests in report root folder
"""

import json
import logging
import os
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Any

from .base_handler import BaseReportHandler


class CUR1ReportHandler(BaseReportHandler):
    """Handler for CUR 1.0 (legacy) format reports."""

    def __init__(self, s3_client, bucket: str, report_prefix: str):
        super().__init__(s3_client, bucket, report_prefix)

    def retrieve_manifests(
        self, s3_objects: List[Dict], report_name: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieve and parse CUR 1.0 manifest files from S3 objects.

        CUR 1.0 manifests are located in the root of report folders with
        date-based naming (YYYYMMDD-YYYYMMDD).
        """
        manifests = []
        manifest_file_pattern = f"{report_name}-Manifest.json"

        for s3_object in s3_objects:
            object_name = s3_object["Key"].split("/")[-1]
            parent_folder_name = s3_object["Key"].split("/")[-2]

            # Parse billing period from folder name (YYYYMMDD-YYYYMMDD format)
            start_date, end_date = self._parse_billing_period_from_folder_name(
                parent_folder_name
            )

            # Process only root-level manifest files for valid billing periods
            if start_date and object_name == manifest_file_pattern:
                # Download and parse manifest JSON
                manifest_content = self._read_s3_file_contents(s3_object["Key"])
                manifest = json.loads(manifest_content)

                # Enrich with additional metadata for processing
                manifest["last_modified"] = s3_object["LastModified"]
                manifest["report_folder"] = s3_object["Key"].replace(
                    f"/{manifest_file_pattern}", ""
                )
                manifest["period"] = parent_folder_name
                manifest["format_version"] = "1.0"
                # Ensure assemblyId exists (fallback to reportId or generate one)
                if "assemblyId" not in manifest:
                    manifest["assemblyId"] = manifest.get(
                        "reportId", f"cur1-{parent_folder_name}-{manifest.get('account', 'unknown')}"
                    )

                manifests.append(manifest)

        logging.info(f"Found {len(manifests)} CUR 1.0 manifests")
        return manifests

    def get_csv_patterns(self, manifests: List[Dict]) -> List[str]:
        """
        Generate CSV file patterns for CUR 1.0 manifests.

        Handles both direct CSV files and ZIP-compressed files with parallel extraction.
        """
        patterns = []

        # Separate ZIP and CSV manifests for different processing
        csv_manifests = [
            m for m in manifests if not self._manifest_contains_zip_files(m)
        ]
        zip_manifests = [m for m in manifests if self._manifest_contains_zip_files(m)]

        # Process direct CSV patterns
        for manifest in csv_manifests:
            base_path = manifest["report_folder"]
            pattern = f"s3://{self.bucket}/{base_path}/*.csv"
            patterns.append(pattern)

        # Process ZIP files in parallel
        if zip_manifests:
            extracted_paths = self._extract_all_zip_files_parallel(zip_manifests)
            patterns.extend(extracted_paths)

        logging.info(f"Generated {len(patterns)} CSV patterns for CUR 1.0")
        return patterns

    def normalize_columns(self, manifest: Dict) -> List[str]:
        """
        Normalize column names for CUR 1.0 format.

        CUR 1.0 uses category/name structure for columns.
        """
        # Build column names from manifest metadata
        manifest_columns = [
            col["category"] + "/" + col["name"] for col in manifest.get("columns", [])
        ]

        return manifest_columns

    def filter_by_date_range(
        self, manifests: List[Dict], since_timestamp, until_timestamp
    ) -> List[Dict]:
        """
        Filter CUR 1.0 manifests by date range.

        Uses billingPeriod information from manifest.
        """
        if not since_timestamp or not until_timestamp:
            return manifests

        filtered_manifests = []
        for manifest in manifests:
            billing_period = manifest.get("billingPeriod", {})
            if not billing_period:
                continue

            # Extract start date from billing period
            period_start = billing_period.get("start", "").split("T")[0]

            try:
                if (
                    datetime.strftime(until_timestamp, "%Y%m%d")
                    >= period_start
                    >= datetime.strftime(since_timestamp, "%Y%m%d")
                ):
                    filtered_manifests.append(manifest)
            except (ValueError, TypeError):
                logging.warning(
                    f"Could not parse billing period for manifest: {billing_period}"
                )
                continue

        logging.info(
            f"Filtered {len(manifests)} manifests to {len(filtered_manifests)} by date range"
        )
        return filtered_manifests

    def _parse_billing_period_from_folder_name(self, folder_name: str):
        """Parse billing period dates from CUR 1.0 folder name (YYYYMMDD-YYYYMMDD)."""
        try:
            # AWS billing periods are typically in format YYYYMMDD-YYYYMMDD
            date_parts = folder_name.split("-")

            if len(date_parts) != 2:
                return None, None

            start_date = datetime.strptime(date_parts[0], "%Y%m%d")
            end_date = datetime.strptime(date_parts[1], "%Y%m%d")

            return start_date, end_date

        except ValueError:
            # Not a valid date format
            logging.debug(
                f"Could not parse billing period from folder name: {folder_name}"
            )
            return None, None

    def _manifest_contains_zip_files(self, manifest: Dict[str, Any]) -> bool:
        """Check if CUR 1.0 manifest contains ZIP files."""
        return (
            manifest.get("reportKeys")
            and len(manifest["reportKeys"]) > 0
            and manifest["reportKeys"][0].endswith("zip")
        )

    def _extract_all_zip_files_parallel(self, zip_manifests: List[Dict]) -> List[str]:
        """Extract all ZIP files in parallel and return paths to extracted CSV files."""
        logging.info(f"Extracting {len(zip_manifests)} ZIP reports in parallel...")

        zip_tasks = []
        for manifest in zip_manifests:
            for chunk_key in manifest.get("reportKeys", []):
                if chunk_key.endswith(".zip"):
                    # Handle special path syntax for S3 keys
                    key_parts = chunk_key.split("/")
                    if "//" in manifest["report_folder"]:
                        normalized_key = f"{manifest['report_folder']}/{key_parts[-2]}/{key_parts[-1]}"
                    else:
                        normalized_key = chunk_key
                    zip_tasks.append(normalized_key)

        # Extract all ZIP files in parallel
        extracted_csv_paths = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all download/extract tasks
            future_to_key = {
                executor.submit(self._download_and_extract_zip, zip_key): zip_key
                for zip_key in zip_tasks
            }

            # Collect results as they complete
            for future in as_completed(future_to_key):
                zip_key = future_to_key[future]
                try:
                    csv_path = future.result()
                    extracted_csv_paths.append(csv_path)
                    logging.debug(f"Successfully extracted: {zip_key}")
                except Exception as e:
                    logging.error(f"Failed to extract {zip_key}: {e}")

        logging.info(
            f"Successfully extracted {len(extracted_csv_paths)} CSV files from ZIP archives"
        )
        return extracted_csv_paths

    def _download_and_extract_zip(self, s3_key: str) -> str:
        """Download and extract a single ZIP file, return path to extracted CSV."""
        try:
            # Download ZIP file content
            zip_content = self._read_s3_file_contents(s3_key)

            # Create temporary file for ZIP content
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
                temp_zip.write(zip_content)
                temp_zip_path = temp_zip.name

            # Extract CSV files from ZIP
            extract_dir = tempfile.mkdtemp()
            with zipfile.ZipFile(temp_zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

            # Find the extracted CSV file (should be only one per ZIP)
            csv_path = None
            for root, dirs, files in os.walk(extract_dir):
                for file in files:
                    if file.endswith(".csv"):
                        csv_path = os.path.join(root, file)
                        break
                if csv_path:
                    break

            # Cleanup temp ZIP file
            os.unlink(temp_zip_path)

            if not csv_path:
                raise Exception(f"No CSV file found in ZIP: {s3_key}")

            return csv_path

        except Exception as e:
            logging.error(f"Failed to extract ZIP file {s3_key}: {e}")
            raise
