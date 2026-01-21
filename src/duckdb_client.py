import logging

import duckdb

from configuration import Configuration

UNIFIED_REPORTS_VIEW = "unified_reports"


class DuckDB:
    """Handles all DuckDB operations for report data processing."""

    def __init__(self, config: Configuration):
        self.config = config
        self.con = None
        self.db_path = "/tmp/cur.duckdb"

    def setup_connection(self):
        """
        Setup DuckDB connection with S3 credentials.

        Installs httpfs extension for S3 access and configures AWS credentials.
        """
        if self.con:
            return

        logging.info(f"Setting up DuckDB connection with db_path: {self.db_path}")
        self.con = duckdb.connect(self.db_path)

        try:
            version = self.con.execute("SELECT version()").fetchone()[0]
            logging.info(f"DuckDB version: {version}")
        except Exception:
            logging.warning("Could not determine DuckDB version")

        self._apply_connection_settings()
        logging.info("DuckDB connection ready with S3 credentials configured")

    def _apply_connection_settings(self):
        """
        Apply S3 credentials and DuckDB settings to current connection.

        Used both for initial setup and after connection resets.
        Settings based on Martin's Shopify component configuration.
        """
        self.con.execute("SET extension_directory='/tmp/duckdb_extensions';")
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        self.con.execute(f"SET s3_region='{self.config.aws_parameters.aws_region}';")
        self.con.execute(f"SET s3_access_key_id='{self.config.aws_parameters.api_key_id}';")
        self.con.execute(f"SET s3_secret_access_key='{self.config.aws_parameters.api_key_secret}';")

        # DuckDB memory and performance settings
        self.con.execute("SET temp_directory='/tmp/duckdb_temp';")
        self.con.execute("SET preserve_insertion_order=false;")
        self.con.execute("SET threads=1;")
        self.con.execute("SET memory_limit='1536MB';")  # 1.5GB limit (Docker has 2GB total)

    def close(self):
        """
        Close DuckDB connection and release all resources.

        Call this after all processing is complete to free memory
        before output mapping or other downstream operations.
        """
        if self.con:
            logging.info("Closing DuckDB connection to release memory...")
            self.con.close()
            self.con = None
            logging.info("DuckDB connection closed")

    @staticmethod
    def _quote_ident(name: str) -> str:
        """
        Quote SQL identifier with double quotes, escaping embedded quotes.

        Handles column names with special characters (/, spaces, etc.) and embedded quotes.
        Example: 'cost/usage' -> '"cost/usage"', 'name"with"quotes' -> '"name""with""quotes"'
        """
        return '"' + name.replace('"', '""') + '"'

    def _get_read_csv_auto_options(self) -> str:
        """
        Return standardized read_csv_auto options for consistent CSV parsing.

        Options:
        - HEADER=TRUE: First row contains column names
        - ALL_VARCHAR=TRUE: Load all columns as strings to avoid type inference issues
        - NULLSTR: Treat these strings as NULL values
        - filename=true: Add filename column for tracking source files
        - PARALLEL=FALSE: Single-threaded parsing to reduce memory usage (important-comment)
        """
        return """HEADER=TRUE,
                                           ALL_VARCHAR=TRUE,
                                           NULLSTR=['null', 'NULL', 'None'],
                                           filename=true,
                                           PARALLEL=FALSE"""

    def create_unified_view_from_files(self, csv_patterns: list[str], final_columns: list[str]) -> bool:
        """
        Create unified view from CSV files using batch processing to avoid OOM.

        Processes files in batches to limit memory usage, then creates final view.

        Args:
            csv_patterns: List of CSV file paths (S3 URIs or local paths)
            final_columns: List of final column names in KBC format (with __)

        Returns:
            True if successful, False otherwise
        """
        import time
        logging.info(f"Creating unified view from {len(csv_patterns)} CSV files using batched read_csv_auto...")
        start_time = time.time()

        try:
            # Process files in batches to avoid OOM with large file counts
            BATCH_SIZE = 25  # Process 25 files at a time
            options = self._get_read_csv_auto_options()

            # Create base table from first batch
            first_batch = csv_patterns[:BATCH_SIZE]
            file_list = ", ".join([f"'{pattern}'" for pattern in first_batch])

            logging.info(f"Creating base table from first {len(first_batch)} files...")
            self.con.execute(f"""
                CREATE OR REPLACE TABLE raw_unified AS
                SELECT * EXCLUDE (filename)
                FROM read_csv_auto([{file_list}],
                                   {options},
                                   union_by_name=true);
            """)

            # Process remaining files in batches
            remaining_batches = [csv_patterns[i:i + BATCH_SIZE]
                                 for i in range(BATCH_SIZE, len(csv_patterns), BATCH_SIZE)]

            for batch_idx, batch in enumerate(remaining_batches, start=2):
                file_list = ", ".join([f"'{pattern}'" for pattern in batch])
                logging.info(f"Processing batch {batch_idx}/{len(remaining_batches) + 1} ({len(batch)} files)...")

                self.con.execute(f"""
                    INSERT INTO raw_unified
                    SELECT * EXCLUDE (filename)
                    FROM read_csv_auto([{file_list}],
                                       {options},
                                       union_by_name=true);
                """)

            # Build SELECT with column aliases for final view
            select_parts = []
            for final_col in final_columns:
                # Convert from KBC format (col__name) to original (col/name)
                original_col = final_col.replace("__", "/")
                quoted_original = self._quote_ident(original_col)
                quoted_final = self._quote_ident(final_col)

                select_parts.append(
                    f'COALESCE({quoted_original}, NULL) AS {quoted_final}'
                )

            select_sql = ",\n                ".join(select_parts)

            self.con.execute(f"""
                CREATE OR REPLACE VIEW {UNIFIED_REPORTS_VIEW} AS
                SELECT {select_sql}
                FROM raw_unified;
            """)

            elapsed = time.time() - start_time
            logging.info(
                f"Unified view created in {elapsed:.1f}s from {len(csv_patterns)} files "
                f"({len(remaining_batches) + 1} batches) with {len(final_columns)} columns."
            )

            return True

        except Exception as e:
            logging.error(f"Failed to create unified view from files: {e}")
            return False

    def export_data_to_csv(self, output_path: str):
        """
        Export data to CSV file using single COPY statement.

        Exports entire unified view directly to output file in one pass.
        """
        import time
        logging.info(f"Exporting data to {output_path}...")
        start_time = time.time()

        self.con.execute(f"""
            COPY {UNIFIED_REPORTS_VIEW}
            TO '{output_path}'
            (HEADER, DELIMITER ',', FORCE_QUOTE *)
        """)

        elapsed = time.time() - start_time
        logging.info(f"Export completed in {elapsed:.1f}s")
