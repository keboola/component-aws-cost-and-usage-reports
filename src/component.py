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
            output_table = self._process_reports_unified_bulk(report_manifests)

            # Step 4: Write table manifest
            self._write_table_manifest(output_table)

            # Step 5: Save final state
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

    def _process_reports_unified_bulk(self, report_manifests) -> str:
        """New unified approach: extract all ZIP files in parallel, then bulk load everything."""
        logging.info(
            f"Processing {len(report_manifests)} reports using unified bulk approach..."
        )

        # Step 1: Prepare all CSV patterns (S3 direct + extracted from ZIP files in parallel)
        all_csv_patterns = self.aws_manager.prepare_all_csv_patterns(report_manifests)

        if not all_csv_patterns:
            logging.warning("No CSV files found to process")
            raise Exception("No CSV files found to process")

        # Step 2: Update runtime state from manifests
        self._update_runtime_state_from_manifests(report_manifests)

        # Step 3: Determine final column set for output table
        final_columns = self.column_normalizer.get_max_header_normalized(
            report_manifests, self.last_header
        )
        self.last_header = final_columns

        # Step 4: Bulk load all CSV files into DuckDB
        # Setup DuckDB connection for processing
        self.duckdb_processor.setup_connection()

        if not self.duckdb_processor.load_csv_files_bulk(all_csv_patterns):
            raise Exception("Failed to load CSV files in bulk")

        # Step 5: Get current columns from loaded data
        current_columns = self.duckdb_processor.get_current_columns_from_table()

        # Step 6: Create unified view and export
        if not self.duckdb_processor.create_unified_view(
            final_columns, current_columns
        ):
            raise Exception("Failed to create unified view")

        # Step 7: Export the data
        output_path = os.path.join(self.tables_out_path, f"{self.report_name}.csv")
        self.duckdb_processor.export_data_to_csv("unified_reports", output_path)

        logging.info(
            f"Successfully processed {len(all_csv_patterns)} files using unified bulk approach"
        )
        return output_path

    def _save_final_state(self):
        """Save the final execution state for future incremental runs."""

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

    @staticmethod
    def _convert_date_strings(since, until):
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
