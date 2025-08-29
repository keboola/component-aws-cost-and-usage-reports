import logging
import os
import sys
from datetime import datetime

import boto3
import pytz
from keboola.component.base import ComponentBase

from configuration import Configuration
from aws_report_manager import AWSReportManager
from duckdb_processor import DuckDBProcessor
from column_normalizer import ColumnNormalizer


class Component(ComponentBase):
    """Main AWS Cost and Usage Reports component."""

    def __init__(self, debug=False):
        super().__init__()

        # Load and validate configuration using Pydantic
        try:
            self.config = Configuration(**self.configuration.parameters)
        except Exception as e:
            logging.error(f"Configuration validation failed: {e}")
            exit(1)

        # Override debug from config
        if self.config.debug:
            debug = True

        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Loading configuration...")

        # Initialize AWS client
        s3_client = boto3.client(
            "s3",
            region_name=self.config.aws_parameters.aws_region,
            aws_access_key_id=self.config.aws_parameters.api_key_id,
            aws_secret_access_key=self.config.aws_parameters.api_key_secret,
        )

        # Initialize specialized managers
        self.aws_manager = AWSReportManager(
            s3_client=s3_client,
            bucket=self.config.aws_parameters.s3_bucket,
            report_prefix=self.config.report_path_prefix,
        )
        self.duckdb_processor = DuckDBProcessor(self.config)
        self.column_normalizer = ColumnNormalizer()

        # Load state
        self.last_state = self.get_state_file()
        self.last_report_id = self.last_state.get("last_report_id")
        self.last_header = self.last_state.get("report_header", [])

        # Runtime state (will be set during execution)
        self.since_timestamp = None
        self.until_timestamp = None
        self.report_name = None
        self.latest_report_id = self.last_report_id

    def run(self):
        """Main execution method - orchestrates the entire AWS Cost and Usage Report extraction process."""
        try:
            # Step 1: Prepare runtime state
            self._prepare_runtime_state()

            # Step 2: Discover and validate available reports
            report_manifests = self._discover_available_reports()

            # Step 3: Process the reports and export data
            self._process_and_export_reports(report_manifests)

            # Step 4: Save final state
            self._save_final_state()

            logging.info(
                f"Extraction finished successfully at {datetime.now().isoformat()}."
            )

        except Exception as e:
            logging.error(f"Report extraction failed: {e}")
            raise e
        finally:
            self.duckdb_processor.cleanup_connection()

    def _prepare_runtime_state(self):
        """Prepare runtime state from configuration."""
        # Parse date range from configuration
        since = self.config.min_date_since or "2000-01-01"
        until = self.config.max_date
        logging.info(f"Date range: {since} to {until}")
        start_date, end_date = self._convert_date_strings(since, until)

        # Convert to UTC timestamps
        self.until_timestamp = pytz.utc.localize(end_date)

        # Determine starting timestamp based on incremental mode
        last_file_timestamp = self.last_state.get("last_file_timestamp")

        if last_file_timestamp and self.config.since_last:
            self.since_timestamp = datetime.fromisoformat(last_file_timestamp)
        else:
            self.since_timestamp = pytz.utc.localize(start_date)

        # Extract report name from prefix
        self.report_name = self.aws_manager.get_report_name()

    def _discover_available_reports(self):
        """Discover and filter available reports from S3 based on configuration."""
        logging.info(
            f"Discovering reports for '{self.report_name}' since {self.since_timestamp}"
        )

        # Get all S3 objects matching our prefix
        all_files = self.aws_manager.get_s3_objects(self.since_timestamp)

        # Extract report manifests
        report_manifests = self.aws_manager.retrieve_report_manifests(
            all_files, self.report_name
        )

        # Filter manifests by date range if not in incremental mode
        if not self.config.since_last:
            report_manifests = self.aws_manager.filter_manifests_by_date_range(
                report_manifests, self.since_timestamp, self.until_timestamp
            )

        # Validate we found reports
        if not report_manifests:
            logging.warning(
                "No reports found for the specified period. Check your prefix setting and date range."
            )
            self.write_state_file(self.last_state)
            exit(0)

        logging.info(f"{len(report_manifests)} reports found and ready for processing.")
        return report_manifests

    def _process_and_export_reports(self, manifests):
        """Process report manifests and export the consolidated data."""
        # Setup DuckDB connection for processing
        self.duckdb_processor.setup_connection()

        # Separate CSV and ZIP manifests for different processing strategies
        csv_manifests = [
            m for m in manifests if not self.aws_manager.manifest_contains_zip_files(m)
        ]
        zip_manifests = [
            m for m in manifests if self.aws_manager.manifest_contains_zip_files(m)
        ]

        # Try efficient bulk loading for CSV files first
        if csv_manifests and not zip_manifests:
            if self._try_bulk_csv_processing(csv_manifests):
                return

        # Fall back to individual file processing
        self._process_reports_individually(manifests)

    def _try_bulk_csv_processing(self, csv_manifests):
        """Attempt to process CSV files using efficient bulk loading."""
        logging.info(
            f"Attempting bulk processing of {len(csv_manifests)} CSV reports..."
        )

        # Generate S3 patterns for bulk loading
        s3_patterns = self.aws_manager.get_s3_patterns_from_manifests(csv_manifests)

        # Try bulk loading
        if not self.duckdb_processor.load_csv_files_bulk(s3_patterns):
            logging.warning(
                "Bulk loading failed, falling back to individual processing"
            )
            return False

        # Update tracking information
        self._update_runtime_state_from_manifests(csv_manifests)

        # Process schema and create unified view
        current_columns = self.duckdb_processor.get_current_columns_from_table()
        final_columns = self.column_normalizer.normalize_and_merge_columns(
            current_columns, self.last_header
        )
        self.last_header = final_columns

        # Export the data
        if self.duckdb_processor.create_unified_view(final_columns, current_columns):
            output_path = os.path.join(self.tables_out_path, f"{self.report_name}.csv")
            self.duckdb_processor.export_data_to_csv("unified_reports", output_path)
            return True
        else:
            logging.error("Failed to create unified view for bulk export")
            return False

    def _process_reports_individually(self, manifests):
        """Process report files individually (legacy approach for ZIP files or bulk fallback)."""
        logging.info("Processing reports individually...")

        # Prepare schema from all manifests
        self.last_header = self.column_normalizer.get_max_header_normalized(
            manifests, self.last_header
        )

        # Create result table with fixed schema
        result_table = f"result_{self.report_name.replace('-', '_')}"
        self.duckdb_processor.create_result_table(result_table, self.last_header)

        # Process each manifest
        for manifest in manifests:
            if (
                self.config.since_last
                and manifest["assemblyId"] == self.latest_report_id
            ):
                logging.warning(
                    f"Report ID {manifest['assemblyId']} already processed, skipping."
                )
                continue

            # Update tracking
            if self.since_timestamp < manifest["last_modified"]:
                self.since_timestamp = manifest["last_modified"]
                self.latest_report_id = manifest["assemblyId"]

            # Process the report chunks
            self._append_report_chunks(result_table, manifest)

        # Export the consolidated data
        output_path = os.path.join(self.tables_out_path, f"{self.report_name}.csv")
        self.duckdb_processor.export_data_to_csv(result_table, output_path)

    def _append_report_chunks(self, result_table: str, manifest):
        """Process and append all chunks from a single report manifest to the result table."""
        logging.info(
            f"Processing report ID {manifest['assemblyId']} for period {manifest['period']} "
            f"with {len(manifest['reportKeys'])} data chunks."
        )

        # Check if this report contains ZIP files
        contains_zip_files = self.aws_manager.manifest_contains_zip_files(manifest)
        if contains_zip_files:
            logging.info("Report contains ZIP files - using local staging approach")

        # Process each data chunk in the report
        for chunk_key in manifest["reportKeys"]:
            self._process_single_chunk(chunk_key, manifest, result_table)

    def _process_single_chunk(self, chunk_key, manifest, result_table):
        """Process a single report chunk (CSV or ZIP file) and append to result table."""
        # Handle special path syntax for S3 keys
        key_parts = chunk_key.split("/")
        if "//" in manifest["report_folder"]:
            normalized_key = (
                f"{manifest['report_folder']}/{key_parts[-2]}/{key_parts[-1]}"
            )
        else:
            normalized_key = chunk_key

        # Determine source path based on file type
        s3_path = f"s3://{self.aws_manager.bucket}/{normalized_key}"

        if s3_path.endswith(".zip"):
            # ZIP files need to be downloaded and extracted locally
            zip_filename = normalized_key.split("/")[-1]
            local_zip_path = f"/tmp/{zip_filename}.zip"
            source_path = self.aws_manager.download_and_unzip(
                normalized_key, local_zip_path
            )
        else:
            # CSV files can be read directly from S3 via DuckDB
            source_path = s3_path

        # Log progress
        chunk_filename = normalized_key.split("/")[-1]
        logging.info(f"Processing chunk: {chunk_filename}")

        # Load chunk data into staging table
        self.duckdb_processor.load_chunk_into_staging_table(source_path)

        # Get columns from staging table
        staging_columns = self.duckdb_processor.get_staging_table_columns()

        # Build column mapping
        column_mapping = self.column_normalizer.build_column_aligned_select(
            self.last_header, staging_columns
        )

        # Insert the aligned data
        self.duckdb_processor.insert_staging_data_to_result(
            result_table, column_mapping
        )

    def _save_final_state(self):
        """Save the final execution state for future incremental runs."""
        output_table = os.path.join(self.tables_out_path, self.report_name)

        # Write table manifest
        self._write_table_manifest(output_table)

        # Write state file
        self.write_state_file(
            {
                "last_file_timestamp": self.since_timestamp.isoformat(),
                "last_report_id": self.latest_report_id,
                "report_header": self.last_header,
            }
        )

        logging.info("Final state saved successfully")

    def _update_runtime_state_from_manifests(self, manifests):
        """Update runtime state with information from multiple manifests."""
        for manifest in manifests:
            if (
                self.config.since_last
                and manifest["assemblyId"] == self.latest_report_id
            ):
                continue
            if self.since_timestamp < manifest["last_modified"]:
                self.since_timestamp = manifest["last_modified"]
                self.latest_report_id = manifest["assemblyId"]

    def _convert_date_strings(self, since, until):
        """Convert date strings to datetime objects."""
        if since == "2000-01-01":
            start_date = datetime(2000, 1, 1)
        else:
            start_date = datetime.strptime(since, "%Y-%m-%d")

        if until == "now":
            end_date = datetime.now()
        else:
            end_date = datetime.strptime(until, "%Y-%m-%d")

        return start_date, end_date

    def _write_table_manifest(self, output_table):
        """Write table manifest with complete column information."""
        table_name = os.path.basename(output_table)
        incremental = self.config.is_incremental
        pkey = self.config.primary_key

        # Create table definition with all discovered columns
        table_def = self.create_out_table_definition(
            name=table_name,
            path=f"{table_name}.csv",
            incremental=incremental,
            primary_key=pkey,
            columns=self.last_header,
        )

        # Write manifest
        self.write_manifest(table_def)


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
