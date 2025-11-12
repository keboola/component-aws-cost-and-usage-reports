import logging
import os
import sys
from datetime import datetime

import boto3
import pytz
from keboola.component.base import ComponentBase
from keboola.utils.header_normalizer import DictHeaderNormalizer

from aws_report_handlers import ReportHandlerFactory
from configuration import Configuration
from duckdb_client import DuckDB


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

        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Loading configuration...")

        # Initialize AWS client
        self.s3_client = boto3.client(
            "s3",
            region_name=self.config.aws_parameters.aws_region,
            aws_access_key_id=self.config.aws_parameters.api_key_id,
            aws_secret_access_key=self.config.aws_parameters.api_key_secret,
        )

        # Report handler will be initialized in run() after date range is set
        # to enable proper version detection on date-filtered S3 objects
        self.report_handler = None
        self.duckdb_processor = DuckDB(self.config)
        self.column_normalizer = DictHeaderNormalizer(replace_dict={"/": "__"})

        # Load state
        self.last_state = self.get_state_file()
        self.last_report_id = self.last_state.get("last_report_id")
        self.last_header = self.last_state.get("report_header", [])

        # Runtime state (will be set during execution)
        self.since_dt = None
        self.until_dt = None
        self.report_name = None
        self.latest_report_id = self.last_report_id
        self.column_types = {}
        self.export_output_path = None

    def run(self):
        """Main execution method - orchestrates the entire AWS Cost and Usage
        Report extraction process."""
        try:
            # Step 1: Prepare runtime state
            self._prepare_runtime_state()

            # Step 1b: Initialize report handler with date-filtered version detection
            # This ensures we detect CUR version only on relevant date range,
            # avoiding false detection in buckets with mixed CUR 1.0 and 2.0 data
            self.report_handler = ReportHandlerFactory.create_handler(
                s3_client=self.s3_client,
                bucket=self.config.aws_parameters.s3_bucket,
                report_prefix=self.config.report_path_prefix,
                since_dt=self.since_dt,
            )

            # Step 2: Discover and validate available reports
            report_manifests = self._discover_available_reports()

            # Step 3: Process the reports and export data
            final_columns = self._process_reports_unified_bulk(report_manifests)

            # Step 4: Write table manifest
            self._write_table_manifest(self.export_output_path, final_columns)

            # Step 5: Save final state
            self._save_final_state()

            logging.info(f"Extraction finished successfully at {datetime.now().isoformat()}.")

        except Exception as e:
            logging.error(f"Report extraction failed: {e}")
            raise e

    def _prepare_runtime_state(self):
        """Prepare runtime state from configuration."""
        # Get date range from configuration properties
        start_date = self.config.start_datetime
        end_date = self.config.end_datetime
        logging.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        # Convert to UTC timestamps
        self.until_dt = pytz.utc.localize(end_date)

        # Determine starting timestamp based on incremental mode
        last_file_timestamp = self.last_state.get("last_file_timestamp")

        if last_file_timestamp and self.config.since_last:
            self.since_dt = datetime.fromisoformat(last_file_timestamp)
        else:
            self.since_dt = pytz.utc.localize(start_date)

        # Extract report name from prefix
        self.report_name = self.config.report_path_prefix.rstrip("/").rstrip("*").split("/")[-1]

    def _discover_available_reports(self):
        """Discover and filter available reports from S3 based on
        configuration."""
        logging.info(f"Discovering reports for '{self.report_name}' since {self.since_dt}")

        # Get all S3 objects matching our prefix
        all_files = list(self.report_handler.get_s3_objects(self.since_dt))

        # Extract report manifests
        report_manifests = self.report_handler.retrieve_manifests(all_files, self.report_name)

        # Filter manifests by date range if not in incremental mode
        if not self.config.since_last:
            report_manifests = self.report_handler.filter_by_date_range(report_manifests, self.since_dt, self.until_dt)

        # Validate we found reports
        if not report_manifests:
            logging.warning("No reports found for the specified period. Check your prefix setting and date range.")
            self.write_state_file(self.last_state)
            exit(0)

        logging.info(f"{len(report_manifests)} reports found and ready for processing.")
        return report_manifests

    def _process_reports_unified_bulk(self, report_manifests):
        """Process reports using read_csv_auto with union_by_name for efficient processing."""
        logging.info(f"Processing {len(report_manifests)} reports...")

        all_csv_patterns = self.report_handler.get_csv_patterns(report_manifests)
        if not all_csv_patterns:
            logging.warning("No CSV files found to process")
            raise Exception("No CSV files found to process")

        self._update_runtime_state_from_manifests(report_manifests)

        final_columns = self._get_max_header_normalized(report_manifests, self.last_header)
        self.last_header = final_columns

        self.duckdb_processor.setup_connection()

        if not self.duckdb_processor.create_unified_view_from_files(all_csv_patterns, final_columns):
            raise Exception("Failed to create unified view from files")

        self.export_output_path = os.path.join(self.tables_out_path, f"{self.report_name}.csv")
        self.duckdb_processor.export_data_to_csv(self.export_output_path)

        self.duckdb_processor.close()

        logging.info(f"Successfully processed {len(all_csv_patterns)} files")

        return final_columns

    def _save_final_state(self):
        """Save the final execution state for future incremental runs."""

        # Write state file
        self.write_state_file(
            {
                "last_file_timestamp": self.since_dt.isoformat(),
                "last_report_id": self.latest_report_id,
                "report_header": self.last_header,
            }
        )

        logging.info("Final state saved successfully")

    def _update_runtime_state_from_manifests(self, manifests):
        """Update runtime state with information from multiple manifests."""
        for manifest in manifests:
            # Skip if we've already processed this manifest (using assemblyId, reportId, or period as fallback)
            manifest_id = manifest.get("assemblyId", manifest.get("reportId", manifest.get("period", "unknown")))
            if self.config.since_last and manifest_id == self.latest_report_id:
                continue
            if self.since_dt < manifest["last_modified"]:
                self.since_dt = manifest["last_modified"]
                # Use assemblyId if available, otherwise fall back to reportId or period
                self.latest_report_id = manifest.get(
                    "assemblyId", manifest.get("reportId", manifest.get("period", "unknown"))
                )

    def _deduplicate_case_insensitive(self, columns: list[str]) -> list[str]:
        """Deduplicate columns case-insensitively, adding _1, _2 suffixes.

        When AWS resource tags differ only by casing (e.g., 'resourceTags/user:owner'
        vs 'resourceTags/user:Owner'), this method ensures they are deduplicated with
        numeric suffixes to prevent failures in Keboola Storage which is case-insensitive.

        Args:
            columns: List of column names to deduplicate

        Returns:
            List of deduplicated column names where case-variant duplicates
            have _1, _2, etc. suffixes added
        """
        seen: dict[str, tuple[str, int]] = {}  # lowercase -> (original_name, count)
        result: list[str] = []
        for col in columns:
            lower_col = col.lower()
            if lower_col in seen:
                seen[lower_col] = (seen[lower_col][0], seen[lower_col][1] + 1)
                result.append(f"{col}_{seen[lower_col][1]}")
            else:
                seen[lower_col] = (col, 0)
                result.append(col)
        return result

    def _get_manifest_normalized_columns(self, manifest):
        """Extract and normalize column names from manifest metadata."""
        # Get raw column names using the appropriate handler
        raw_columns = self.report_handler.normalize_columns(manifest)

        # Normalize column names using HeaderNormalizer
        normalized_columns = self.column_normalizer.normalize_header(raw_columns)

        # Remove case-insensitive duplicates with suffix indexing
        return sorted(self._deduplicate_case_insensitive(normalized_columns))

    def _get_max_header_normalized(self, manifests, current_header):
        """Build normalized column schema from all manifests."""
        header = current_header.copy()

        for manifest in manifests:
            # Get normalized columns from this manifest
            normalized_columns = self._get_manifest_normalized_columns(manifest)

            # Merge with existing header
            norm_cols = set(normalized_columns)
            if not norm_cols.issubset(set(header)):
                norm_cols.update(set(header))
                header = list(norm_cols)
                header.sort()

        return header

    def _write_table_manifest(self, output_table, final_columns):
        """Write table manifest with complete column information and data
        types."""
        table_name = os.path.basename(output_table)
        incremental = self.config.loading_options.incremental_output_bool
        pkey = self.config.loading_options.pkey

        # Create table definition with schema as list of strings
        # (defaults to STRING type)
        table_def = self.create_out_table_definition(
            name=table_name,
            incremental=incremental,
            primary_key=pkey,
            schema=final_columns,
            has_header=True,
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
