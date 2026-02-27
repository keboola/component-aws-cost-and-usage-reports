import glob
import logging
import os
import shutil
import time

import duckdb


class DuckDBClient:
    """
    DuckDB client for local processing of AWS Cost and Usage Reports.
    Uses VIEW with lazy evaluation to avoid OOM errors.
    """

    def __init__(self):
        # Use disk-backed database to allow spillover and avoid OOM
        # Set extension directory to /tmp to avoid permission issues in containers
        self._connection = duckdb.connect('/tmp/cur.duckdb', config={
            'extension_directory': '/tmp/duckdb_extensions',
            'temp_directory': '/tmp/duckdb_temp'
        })
        # Memory limits to prevent OOM in containers (leaves room for Python + overhead)
        self._connection.execute("SET memory_limit='1536MB'")
        self._connection.execute("SET threads=1")
        self._connection.execute("SET preserve_insertion_order=false")

        # Track CSV files and column mappings for later VIEW creation
        self._csv_files = []
        self._table_name = None
        self._aws_credentials = None
        self._final_columns = None  # Populated after export
        self._column_mapping = {}  # original_name -> normalized_name (from manifests)

    def open_connection(self):
        """Compatibility method - connection is always open"""
        pass

    def close(self):
        """Close the DuckDB connection and cleanup all temporary files"""
        if self._connection:
            self._connection.close()

        # Clean up all DuckDB temporary files
        for pattern in ['/tmp/cur.duckdb*', '/tmp/duckdb_temp/*', '/tmp/duckdb_extensions/*']:
            for path in glob.glob(pattern):
                try:
                    if os.path.isfile(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                except Exception as e:
                    logging.debug(f"Failed to cleanup {path}: {e}")

    def get_final_columns(self):
        """
        Get final column list after export.

        Returns:
            List of column names as they appear in the exported CSV, or None if export hasn't run yet.
        """
        return self._final_columns

    def create_table(self, name, columns: list[dict]):
        """
        Store table name (no actual table created - using VIEW approach).

        Args:
            name: Table name
            columns: List of dicts with 'name' and 'type' keys (ignored for VIEW approach)
        """
        self._table_name = name
        logging.debug(f"Table name set to: {name} (VIEW will be created during export)")

    def load_csv_file(self, table_name: str, original_columns: list[str],
                      normalized_columns: list[str], csv_file_path: str):
        """
        Add CSV file to processing queue and update column mapping.

        Args:
            table_name: Target table name
            original_columns: Original column names from manifest (e.g., "identity/LineItemId")
            normalized_columns: Normalized column names from component.py
            csv_file_path: Path to the CSV file
        """
        logging.debug(f"Queueing CSV file for VIEW: {csv_file_path}")
        self._csv_files.append(csv_file_path)

        # Update global column mapping from manifest metadata
        for orig, norm in zip(original_columns, normalized_columns):
            if orig not in self._column_mapping:
                self._column_mapping[orig] = norm
            elif self._column_mapping[orig] != norm:
                # Keep existing canonical mapping; ignore alternate variant to ensure stable target column
                logging.debug(f"Column mapping alternate ignored for '{orig}': "
                              f"'{self._column_mapping[orig]}' vs '{norm}'")

    def load_csv_from_s3(self, table_name: str, original_columns: list[str],
                         normalized_columns: list[str], s3_path: str,
                         aws_access_key_id: str, aws_secret_access_key: str,
                         aws_region: str):
        """
        Add S3 CSV to processing queue, configure S3 credentials, and update column mapping.

        Args:
            table_name: Target table name
            original_columns: Original column names from manifest (e.g., "identity/LineItemId")
            normalized_columns: Normalized column names from component.py
            s3_path: S3 path (s3://bucket/key)
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret key
            aws_region: AWS region
        """
        # Configure S3 credentials once (lazy - only when first S3 file is added)
        if self._aws_credentials is None:
            logging.debug("Configuring S3 credentials for DuckDB")
            self._connection.execute("INSTALL httpfs")
            self._connection.execute("LOAD httpfs")
            # Escape single quotes in credentials to prevent SQL injection
            safe_region = aws_region.replace("'", "''")
            safe_key_id = aws_access_key_id.replace("'", "''")
            safe_secret = aws_secret_access_key.replace("'", "''")
            self._connection.execute(f"SET s3_region='{safe_region}'")
            self._connection.execute(f"SET s3_access_key_id='{safe_key_id}'")
            self._connection.execute(f"SET s3_secret_access_key='{safe_secret}'")
            self._aws_credentials = True

        logging.debug(f"Queueing S3 file for VIEW: {s3_path}")
        self._csv_files.append(s3_path)

        # Update global column mapping from manifest metadata
        for orig, norm in zip(original_columns, normalized_columns):
            if orig not in self._column_mapping:
                self._column_mapping[orig] = norm
            elif self._column_mapping[orig] != norm:
                # Keep existing canonical mapping; ignore alternate variant to ensure stable target column
                logging.debug(f"Column mapping alternate ignored for '{orig}': "
                              f"'{self._column_mapping[orig]}' vs '{norm}'")

    def export_to_csv(self, table_name: str, output_path: str, columns: list[str]):
        """
        Create VIEW from queued files and export to CSV using streaming.

        Process:
        1. Use column mapping from manifest metadata (passed via load_csv_* methods)
        2. Create VIEW with column mapping (just SQL definition, no data)
        3. COPY streams directly: Files -> normalization -> CSV

        Args:
            table_name: Source table name (VIEW name)
            output_path: Path to output CSV file
            columns: Expected columns from component.py (self.last_header)
        """
        if not self._csv_files:
            logging.warning("No CSV files queued for export")
            return

        if not self._column_mapping:
            raise ValueError("No column mapping provided. Call load_csv_* methods first.")

        logging.info(f"Creating VIEW from {len(self._csv_files)} CSV files using manifest metadata...")
        start_time = time.time()

        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Step 1: Use column mapping from manifest metadata (already normalized by component.py)
            # This ensures we use the same normalization as v1.1.3 (from manifest category/name)
            column_mappings = list(self._column_mapping.items())  # (original, normalized) tuples
            mapped_normalized = {norm for orig, norm in column_mappings}
            logging.info(f"Using {len(column_mappings)} columns from manifest metadata")

            # Step 2: Build SELECT with column mapping from manifest metadata
            # Include ALL columns from expected header (self.last_header), filling missing with NULL
            select_parts = []
            new_header = list(columns)
            for col in columns:
                if col in mapped_normalized:
                    orig_col = next(orig for orig, norm in column_mappings if norm == col)
                    select_parts.append(f"{self._quote_ident(orig_col)} AS {self._quote_ident(col)}")
                else:
                    select_parts.append(f"NULL AS {self._quote_ident(col)}")

            select_sql = ",\n                ".join(select_parts)

            # Step 3: Create VIEW with all files (lazy - no data loaded)
            logging.info(f"Creating VIEW with {len(self._csv_files)} files (no data in memory)...")
            # Escape single quotes in file paths
            escaped_paths = [path.replace("'", "''") for path in self._csv_files]
            all_files = ", ".join([f"'{path}'" for path in escaped_paths])
            quoted_table_name = self._quote_ident(table_name)
            options = self._get_read_csv_auto_options()

            self._connection.execute(f"""
                CREATE OR REPLACE VIEW {quoted_table_name} AS
                SELECT {select_sql}
                FROM read_csv_auto([{all_files}],
                                   {options},
                                   union_by_name=true);
            """)

            # Step 4: Export using COPY (streaming - no materialization)
            logging.info(f"Exporting to {output_path} (streaming)...")
            self._connection.execute(f"""
                COPY {quoted_table_name}
                TO '{output_path}'
                (HEADER, DELIMITER ',', FORCE_QUOTE *)
            """)

            elapsed = time.time() - start_time
            logging.info(
                f"Export completed in {elapsed:.1f}s with {len(new_header)} columns. "
                f"Data streamed through VIEW (no materialization)."
            )

            # Store final columns for component.py to use in manifest and state
            self._final_columns = new_header

        except Exception as e:
            logging.error(f"Error during VIEW export: {e}")
            raise

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
        - PARALLEL=FALSE: Single-threaded parsing to reduce memory usage
        """
        return """HEADER=TRUE,
                                           ALL_VARCHAR=TRUE,
                                           NULLSTR=['null', 'NULL', 'None'],
                                           filename=true,
                                           PARALLEL=FALSE"""
