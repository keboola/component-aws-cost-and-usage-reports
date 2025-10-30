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
from typing import Any

from .base_handler import BaseReportHandler


class CUR1ReportHandler(BaseReportHandler):
    """Handler for CUR 1.0 (legacy) format reports."""

    def __init__(self, s3_client, bucket: str, report_prefix: str):
        super().__init__(s3_client, bucket, report_prefix)

    def retrieve_manifests(self, s3_objects: list[dict], report_name: str) -> list[dict[str, Any]]:
        """
        Retrieve and parse CUR 1.0 manifest files from S3 objects.

        CUR 1.0 manifests are located in the root of report folders with
        date-based naming (YYYYMMDD-YYYYMMDD).
        """
        manifests = []
        manifest_file_pattern = f"{report_name}-Manifest.json"

        for s3_object in s3_objects:
            key_parts = s3_object["Key"].split("/")
            object_name = key_parts[-1]

            # Try to parse billing period from folder name (YYYYMMDD-YYYYMMDD format)
            # Used only to identify valid report folders (gate), not to mutate manifest content

            if object_name != manifest_file_pattern:
                continue

            parent_folder_name = key_parts[-2]
            start_date, end_date = self._try_parse_billing_period_from_folder_name(parent_folder_name)

            if not start_date:
                if len(key_parts) >= 3:
                    grandparent_folder_name = key_parts[-3]
                    start_date, end_date = self._try_parse_billing_period_from_folder_name(grandparent_folder_name)

                    if start_date:
                        parent_folder_name = grandparent_folder_name

            # Process only root-level manifest files for valid billing periods
            if start_date:
                # Download and parse manifest JSON
                manifest_content = self._read_s3_file_contents(s3_object["Key"])
                manifest = json.loads(manifest_content)

                # Enrich with additional metadata for processing
                manifest["last_modified"] = s3_object["LastModified"]
                manifest["report_folder"] = s3_object["Key"].replace(f"/{manifest_file_pattern}", "")
                manifest["period"] = parent_folder_name
                manifest["format_version"] = "1.0"
                # Ensure assemblyId exists (fallback to reportId or generate one)
                if "assemblyId" not in manifest:
                    manifest["assemblyId"] = manifest.get(
                        "reportId",
                        f"cur1-{parent_folder_name}-{manifest.get('account', 'unknown')}",
                    )

                manifests.append(manifest)

        logging.info(f"Found {len(manifests)} CUR 1.0 manifests")
        return manifests

    def get_csv_patterns(self, manifests: list[dict]) -> list[str]:
        """
        Generate CSV file patterns for CUR 1.0 manifests.

        Handles both direct CSV files and ZIP-compressed files with parallel extraction.
        """
        patterns = []

        # Separate ZIP and CSV manifests for different processing
        csv_manifests = [m for m in manifests if not self._manifest_contains_zip_files(m)]
        zip_manifests = [m for m in manifests if self._manifest_contains_zip_files(m)]

        # Process direct CSV patterns
        for manifest in csv_manifests:
            report_keys = manifest.get("reportKeys", [])
            for key in report_keys:
                if key.endswith(".csv") or key.endswith(".csv.gz"):
                    patterns.append(f"s3://{self.bucket}/{key}")

        # Process ZIP files in parallel
        if zip_manifests:
            extracted_paths = self._extract_all_zip_files_parallel(zip_manifests)
            patterns.extend(extracted_paths)

        logging.info(f"Generated {len(patterns)} CSV patterns for CUR 1.0")
        return patterns

    def normalize_columns(self, manifest: dict) -> list[str]:
        """
        Normalize column names for CUR 1.0 format.

        CUR 1.0 uses category/name structure for columns.
        """
        # Build column names from manifest metadata
        manifest_columns = [col["category"] + "/" + col["name"] for col in manifest.get("columns", [])]

        return manifest_columns

    def filter_by_date_range(self, manifests: list[dict], since_dt: datetime, until_dt: datetime) -> list[dict]:
        """
        Filter CUR 1.0 manifests by date range.

        A manifest is included if its billing period overlaps with the specified date range.
        Overlap occurs when: (period_end >= since) AND (period_start <= until)

        Args:
            manifests: List of CUR 1.0 manifest dictionaries
            since_dt: Start of date range (datetime with timezone)
            until_dt: End of date range (datetime with timezone)

        Returns:
            List of manifests whose billing period overlaps with the date range
        """
        if not since_dt or not until_dt:
            return manifests

        filtered_manifests = []
        for manifest in manifests:
            billing_period = manifest.get("billingPeriod", {})
            if not billing_period:
                logging.warning(f"Manifest missing billingPeriod, skipping: {manifest.get('reportId')}")
                continue

            try:
                # Parse billing period dates (format: "20240101T000000.000Z")
                period_start_str = billing_period.get("start", "")
                period_end_str = billing_period.get("end", "")

                if not period_start_str or not period_end_str:
                    logging.warning(f"Incomplete billing period in manifest: {billing_period}")
                    continue

                # Parse ISO format dates (e.g., "20240101T000000.000Z")
                period_start = datetime.strptime(period_start_str.split("T")[0], "%Y%m%d")
                period_end = datetime.strptime(period_end_str.split("T")[0], "%Y%m%d")

                # Check for overlap: period overlaps if (period_end >= since) AND (period_start <= until)
                # Convert to date for comparison (ignore time component)
                if period_end.date() >= since_dt.date() and period_start.date() <= until_dt.date():
                    filtered_manifests.append(manifest)
                    logging.debug(
                        f"Manifest {manifest.get('reportId')} included: "
                        f"period {period_start_str} to {period_end_str} overlaps with filter range"
                    )

            except (ValueError, TypeError, AttributeError) as e:
                logging.warning(f"Could not parse billing period for manifest {manifest.get('reportId')}: {e}")
                continue

        logging.info(f"Filtered {len(manifests)} manifests to {len(filtered_manifests)} by date range")
        return filtered_manifests

    def _try_parse_billing_period_from_folder_name(self, folder_name: str) -> tuple[datetime | None, datetime | None]:
        """
        Best-effort parsing of a CUR 1.0 billing period from a folder name.

        Context:
        - CUR 1.0 commonly uses folder names in the format YYYYMMDD-YYYYMMDD.
        - In line with the main-branch behavior, this method is used only as a
          gate to recognize valid period folders when scanning S3.
        - This method does not mutate the manifest; filtering relies solely on
          the billingPeriod present in the manifest JSON.

        Behavior:
        - Attempts to split the folder name into two parts and parse both using
          the "%Y%m%d" format.
        - On failure, returns (None, None) without logging (expected, because most
          folders are not period folders, e.g., "metadata", "data").

        Args:
            folder_name: S3 folder name, e.g., "20240101-20240131".

        Returns:
            (start_date, end_date) or (None, None) if invalid format/content.
        """
        # Quick pattern check - expected format: YYYYMMDD-YYYYMMDD
        periods = folder_name.split("-")
        start_date = None
        end_date = None
        if len(periods) == 2:
            try:
                start_date = datetime.strptime(periods[0], "%Y%m%d")
                end_date = datetime.strptime(periods[1], "%Y%m%d")
            except Exception:
                pass

        return start_date, end_date

    def _manifest_contains_zip_files(self, manifest: dict[str, Any]) -> bool:
        """Check if CUR 1.0 manifest contains ZIP files."""
        return (
            manifest.get("reportKeys") and len(manifest["reportKeys"]) > 0 and manifest["reportKeys"][0].endswith("zip")
        )

    def _extract_all_zip_files_parallel(self, zip_manifests: list[dict]) -> list[str]:
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
            future_to_key = {executor.submit(self._download_and_extract_zip, zip_key): zip_key for zip_key in zip_tasks}

            # Collect results as they complete
            for future in as_completed(future_to_key):
                zip_key = future_to_key[future]
                try:
                    csv_path = future.result()
                    extracted_csv_paths.append(csv_path)
                    logging.debug(f"Successfully extracted: {zip_key}")
                except Exception as e:
                    logging.error(f"Failed to extract {zip_key}: {e}")

        logging.info(f"Successfully extracted {len(extracted_csv_paths)} CSV files from ZIP archives")
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
