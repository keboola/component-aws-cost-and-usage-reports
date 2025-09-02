import json
import logging
import os
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError


class AWSReportManager:
    """Manages AWS S3 operations and Cost and Usage Report manifests."""

    def __init__(self, s3_client: boto3.client, bucket: str, report_prefix: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self.report_prefix = report_prefix
        self._cleanup_report_prefix()

    def _cleanup_report_prefix(self):
        """Clean and normalize the report prefix."""
        if self.report_prefix.endswith("/"):
            self.report_prefix = self.report_prefix[:-1]

        if not self.report_prefix.endswith("*"):
            self.report_prefix = self.report_prefix + "*"

    def get_report_name(self) -> str:
        """Extract clean report name from the configured prefix."""
        return self.report_prefix.split("/")[-1].replace("*", "")

    def get_s3_objects(self, since_timestamp=None, until_timestamp=None):
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

                if since_timestamp and obj["LastModified"] <= since_timestamp:
                    continue
                if until_timestamp and obj["LastModified"] > until_timestamp:
                    continue

                if (is_wildcard and key.startswith(prefix)) or key == prefix:
                    yield obj

    def retrieve_report_manifests(
        self, all_files, report_name: str
    ) -> List[Dict[str, Any]]:
        """Extract and parse report manifest files from S3 objects."""
        manifests = []
        manifest_file_pattern = f"{report_name}-Manifest.json"

        for s3_object in all_files:
            object_name = s3_object["Key"].split("/")[-1]
            parent_folder_name = s3_object["Key"].split("/")[-2]

            # Parse billing period from folder name
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

                manifests.append(manifest)

        return manifests

    def _parse_billing_period_from_folder_name(self, folder_name):
        """Parse billing period dates from AWS report folder name."""
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

    def _read_s3_file_contents(self, key: str) -> bytes:
        """Read file contents from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read()
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                logging.exception("The specified object was not found.")
            elif error.response["Error"]["Code"] == "AccessDenied":
                logging.exception("Permission to access the object from S3 is missing.")
            else:
                logging.exception(str(error))
            raise

    def filter_manifests_by_date_range(
        self, manifests: List[Dict], since_timestamp, until_timestamp
    ):
        """Filter report manifests to only include those within the specified date range."""
        return [
            manifest
            for manifest in manifests
            if datetime.strftime(until_timestamp, "%Y%m%d")
            >= manifest["billingPeriod"]["start"].split("T")[0]
            >= datetime.strftime(since_timestamp, "%Y%m%d")
        ]

    def manifest_contains_zip_files(self, manifest: Dict[str, Any]) -> bool:
        """Check if manifest contains ZIP files."""
        return (
            manifest.get("reportKeys")
            and len(manifest["reportKeys"]) > 0
            and manifest["reportKeys"][0].endswith("zip")
        )

    def get_s3_patterns_from_manifests(self, manifests: List[Dict]) -> List[str]:
        """Build S3 patterns for CSV files from manifests."""
        patterns = []
        for manifest in manifests:
            if not self.manifest_contains_zip_files(manifest):
                base_path = manifest["report_folder"]
                pattern = f"s3://{self.bucket}/{base_path}/*.csv"
                patterns.append(pattern)
        return patterns

    def prepare_all_csv_patterns(self, manifests: List[Dict]) -> List[str]:
        """Prepare all CSV patterns by extracting ZIP files and getting S3 patterns."""
        csv_patterns = []

        # Get direct CSV patterns
        csv_manifests = [
            m for m in manifests if not self.manifest_contains_zip_files(m)
        ]
        csv_patterns.extend(self.get_s3_patterns_from_manifests(csv_manifests))

        # Extract ZIP files and get local CSV patterns
        zip_manifests = [m for m in manifests if self.manifest_contains_zip_files(m)]
        if zip_manifests:
            extracted_paths = self.extract_all_zip_files_parallel(zip_manifests)
            csv_patterns.extend(extracted_paths)

        return csv_patterns

    def extract_all_zip_files_parallel(self, zip_manifests: List[Dict]) -> List[str]:
        """Extract all ZIP files in parallel and return paths to extracted CSV files."""
        logging.info(f"Extracting {len(zip_manifests)} ZIP reports in parallel...")

        zip_tasks = []
        for manifest in zip_manifests:
            for chunk_key in manifest["reportKeys"]:
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
        # Create unique temporary paths
        zip_filename = s3_key.split("/")[-1]
        local_zip_path = os.path.join(
            tempfile.gettempdir(), f"{zip_filename}_{os.getpid()}"
        )

        return self.download_and_unzip(s3_key, local_zip_path)

    def download_and_unzip(self, s3_key: str, local_zip_path: str) -> str:
        """Download ZIP file from S3 and extract the CSV content."""
        # Download ZIP file from S3
        logging.debug(f"Downloading ZIP file from S3: {s3_key}")
        self.s3_client.download_file(self.bucket, s3_key, local_zip_path)

        # Create temporary directory for extraction
        extraction_dir = tempfile.mkdtemp(suffix="_aws_report_extraction")

        # Extract ZIP contents
        with zipfile.ZipFile(local_zip_path, "r") as zip_file:
            zip_file.extractall(extraction_dir)

        # Find the extracted CSV file (should be only one)
        extracted_files = os.listdir(extraction_dir)
        if not extracted_files:
            raise Exception(f"No files found in extracted ZIP: {s3_key}")

        csv_file_path = os.path.join(extraction_dir, extracted_files[0])
        logging.debug(f"Extracted CSV file: {csv_file_path}")

        return csv_file_path
