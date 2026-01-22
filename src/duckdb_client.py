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

    def _normalize_column_name(self, col_name: str) -> str:
        """
        Normalize column name exactly like v1.1.2 did.

        Replaces:
        - / with __
        - All other non-alphanumeric characters with _

        Example: "resourceTags/user:owner" -> "resourceTags__user_owner"
        """
        import re
        normalized = col_name.replace('/', '__')
        normalized = re.sub(r"[^a-zA-Z\d_]", "_", normalized)
        return normalized

    def create_unified_table_from_files(self, csv_patterns: list[str]) -> bool:
        """
        Create unified table with normalized and deduplicated column names (like v1.1.2).

        Process:
        1. UNION ALL files with original column names
        2. Normalize column names (/ -> __, special chars -> _)
        3. Deduplicate case-insensitive with _1, _2 suffixes
        4. Export with normalized names

        Args:
            csv_patterns: List of CSV file paths (S3 URIs or local paths)

        Returns:
            True if successful, False otherwise
        """
        import time
        logging.info(f"Creating unified table from {len(csv_patterns)} CSV files...")
        start_time = time.time()

        try:
            # Process files in batches
            BATCH_SIZE = 10  # 10 files per batch with 4GB memory
            options = self._get_read_csv_auto_options()

            all_batches = [csv_patterns[i:i + BATCH_SIZE]
                           for i in range(0, len(csv_patterns), BATCH_SIZE)]

            # Build UNION ALL query
            logging.info(f"Building UNION ALL query for {len(all_batches)} batches...")
            union_parts = []
            for batch_idx, batch in enumerate(all_batches, start=1):
                file_list = ", ".join([f"'{pattern}'" for pattern in batch])
                logging.info(f"Adding batch {batch_idx}/{len(all_batches)} ({len(batch)} files)...")

                union_parts.append(f"""
                    SELECT * EXCLUDE (filename)
                    FROM read_csv_auto([{file_list}],
                                       {options},
                                       union_by_name=true)
                """)

            logging.info("Creating raw unified table...")
            union_query = "\nUNION ALL BY NAME\n".join(union_parts)

            self.con.execute(f"""
                CREATE OR REPLACE TABLE raw_unified AS
                {union_query};
            """)

            # Get columns and normalize like v1.1.2
            columns_result = self.con.execute("DESCRIBE raw_unified").fetchall()
            original_columns = [row[0] for row in columns_result]

            logging.info(f"Raw table created with {len(original_columns)} columns, normalizing...")

            # Normalize column names (like v1.1.2 _kbc_normalize_header)
            normalized_columns = [self._normalize_column_name(col) for col in original_columns]

            # Deduplicate case-insensitive (like v1.1.2 _dedupe_header)
            new_header = []
            new_header_lower = []
            dup_cols = {}
            column_mappings = []  # (original, final_normalized)

            for orig_col, norm_col in zip(original_columns, normalized_columns):
                norm_lower = norm_col.lower()
                if norm_lower in new_header_lower:
                    # Duplicate - add suffix
                    new_index = dup_cols.get(norm_lower, 0) + 1
                    final_col = f"{norm_col}_{new_index}"
                    dup_cols[norm_lower] = new_index
                    logging.info(f"Duplicate column: '{orig_col}' -> '{final_col}'")
                else:
                    final_col = norm_col
                    new_header_lower.append(norm_lower)

                new_header.append(final_col)
                column_mappings.append((orig_col, final_col))

            # Build SELECT with normalized and deduplicated names
            select_parts = []
            for orig_col, final_col in column_mappings:
                quoted_orig = self._quote_ident(orig_col)
                quoted_final = self._quote_ident(final_col)
                select_parts.append(f"{quoted_orig} AS {quoted_final}")

            select_sql = ",\n                ".join(select_parts)

            logging.info("Creating final table with normalized column names...")
            self.con.execute(f"""
                CREATE OR REPLACE TABLE {UNIFIED_REPORTS_VIEW} AS
                SELECT {select_sql}
                FROM raw_unified;
            """)

            # Drop temp table
            self.con.execute("DROP TABLE raw_unified")

            elapsed = time.time() - start_time
            row_count = self.con.execute(f"SELECT COUNT(*) FROM {UNIFIED_REPORTS_VIEW}").fetchone()[0]

            logging.info(
                f"Table created in {elapsed:.1f}s: {row_count:,} rows, "
                f"{len(new_header)} columns from {len(csv_patterns)} files"
            )

            return True

        except Exception as e:
            logging.error(f"Failed to create unified table: {e}")
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
