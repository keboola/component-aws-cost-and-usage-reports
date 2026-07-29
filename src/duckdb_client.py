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

        # Track CSV files and their per-file column names for later VIEW creation.
        # Each entry is a tuple: (file_path, [column names in physical order]).
        # Names are the already-normalized, case-insensitively unique names produced
        # by component.py, so they can be applied as an explicit header override. This
        # avoids DuckDB's case-insensitive header handling collapsing source columns
        # that differ only in letter case (e.g. "...user:Team" vs "...user:team").
        self._files = []
        self._table_name = None
        self._aws_credentials = None
        self._final_columns = None  # Populated after export

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

    def load_csv_file(self, table_name: str, column_names: list[str], csv_file_path: str):
        """
        Add CSV file to processing queue together with its column names.

        Args:
            table_name: Target table name
            column_names: Normalized, case-insensitively unique column names for the file,
                in the same physical order as the columns in the CSV.
            csv_file_path: Path to the CSV file
        """
        logging.debug(f"Queueing CSV file for VIEW: {csv_file_path}")
        self._files.append((csv_file_path, column_names))

    def load_csv_from_s3(self, table_name: str, column_names: list[str], s3_path: str,
                         aws_access_key_id: str, aws_secret_access_key: str,
                         aws_region: str):
        """
        Add S3 CSV to processing queue and configure S3 credentials.

        Args:
            table_name: Target table name
            column_names: Normalized, case-insensitively unique column names for the file,
                in the same physical order as the columns in the CSV.
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
        self._files.append((s3_path, column_names))

    def export_to_csv(self, table_name: str, output_path: str, columns: list[str]):
        """
        Create VIEW from queued files and export to CSV using streaming.

        Process:
        1. Read each file with its own explicit (already normalized) column names, so
           DuckDB never sniffs the raw CSV header. Raw AWS CUR headers can contain
           columns that differ only in letter case (e.g. "...user:Team" vs
           "...user:team"); DuckDB treats identifiers case-insensitively, so relying on
           its header handling would collapse such columns and lose one column's data.
        2. Combine the per-file relations with UNION ALL BY NAME (aligns columns across
           files by their normalized, case-insensitively unique names).
        3. Project the expected header, filling any missing columns with NULL.
        4. COPY streams directly: Files -> normalization -> CSV.

        Args:
            table_name: Source table name (VIEW name)
            output_path: Path to output CSV file
            columns: Expected columns from component.py (self.last_header)
        """
        if not self._files:
            logging.warning("No CSV files queued for export")
            return

        logging.info(f"Creating VIEW from {len(self._files)} CSV files using manifest metadata...")
        start_time = time.time()

        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Columns available across all queued files (normalized names from component.py)
            available_columns = {name for _, names in self._files for name in names}
            logging.info(f"Using {len(available_columns)} columns from manifest metadata")

            # Group files that share the exact same column layout so each group can be read
            # by a single multi-file read_csv_auto (one streaming reader, low memory). Files
            # are read with explicit column names so DuckDB never sniffs the raw CSV header;
            # this keeps source columns that differ only in letter case distinct instead of
            # collapsing them (DuckDB treats identifiers case-insensitively).
            groups = {}
            for path, names in self._files:
                groups.setdefault(tuple(names), []).append(path)

            options = self._get_read_csv_auto_options()
            union_parts = []
            for names, paths in groups.items():
                paths_sql = ", ".join(self._quote_string(p) for p in paths)
                names_sql = ", ".join(self._quote_string(name) for name in names)
                # union_by_name=false: files within a group share an identical layout, so
                # align by position and apply the explicit names.
                union_parts.append(
                    f"SELECT * FROM read_csv_auto([{paths_sql}], {options}, "
                    f"union_by_name=false, names=[{names_sql}])"
                )
            # UNION ALL BY NAME reconciles differing layouts across groups by column name.
            union_sql = "\n                UNION ALL BY NAME\n                ".join(union_parts)

            # Project the expected header, filling missing columns with NULL. All projected
            # names are case-insensitively unique, so name resolution is unambiguous.
            select_parts = []
            new_header = list(columns)
            for col in columns:
                if col in available_columns:
                    select_parts.append(self._quote_ident(col))
                else:
                    select_parts.append(f"NULL AS {self._quote_ident(col)}")

            select_sql = ",\n                ".join(select_parts)

            # Create VIEW with all files (lazy - no data loaded)
            logging.info(f"Creating VIEW with {len(self._files)} files (no data in memory)...")
            quoted_table_name = self._quote_ident(table_name)

            self._connection.execute(f"""
                CREATE OR REPLACE VIEW {quoted_table_name} AS
                SELECT {select_sql}
                FROM (
                {union_sql}
                );
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

    @staticmethod
    def _quote_string(value: str) -> str:
        """Quote a SQL string literal with single quotes, escaping embedded quotes."""
        return "'" + value.replace("'", "''") + "'"

    def _get_read_csv_auto_options(self) -> str:
        """
        Return standardized read_csv_auto options for consistent CSV parsing.

        Options:
        - HEADER=TRUE: First row is the CSV header (skipped; column names come from the
          explicit `names` override supplied per file)
        - ALL_VARCHAR=TRUE: Load all columns as strings to avoid type inference issues
        - NULLSTR: Treat these strings as NULL values
        - PARALLEL=FALSE: Single-threaded parsing to reduce memory usage
        """
        return """HEADER=TRUE,
                                           ALL_VARCHAR=TRUE,
                                           NULLSTR=['null', 'NULL', 'None'],
                                           PARALLEL=FALSE"""
