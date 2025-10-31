import logging
import os

import duckdb
from duckdb import DuckDBPyConnection

from configuration import Configuration

# DuckDB temporary directory configuration
DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")

# DuckDB table and view names
RAW_REPORTS_TABLE = "raw_reports"
UNIFIED_REPORTS_VIEW = "unified_reports"

# DuckDB metadata column names
FILENAME_COLUMN = "filename"


class DuckDB:
    """Handles all DuckDB operations for report data processing."""

    def __init__(self, config: Configuration):
        self.config = config
        self.con = None

    @staticmethod
    def _init_connection(db_path: str = None) -> DuckDBPyConnection:
        """
        Returns connection to on-disk DuckDB database with memory limits.
        Uses on-disk database to avoid OOM with large datasets.
        Sets conservative memory limits to force spilling to disk.
        Optional overrides via DUCKDB_THREADS and DUCKDB_MEMORY_MB environment variables.
        Aligns with processor-duckdb config style for cross-version compatibility.
        """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        if db_path is None:
            db_path = os.path.join(DUCK_DB_DIR, "work.duckdb")
        threads = os.getenv("DUCKDB_THREADS", "4")
        memory = os.getenv("DUCKDB_MEMORY_MB", "1536")
        if not memory.endswith("MB") and not memory.endswith("GB"):
            try:
                memory = f"{int(memory)}MB"
            except (ValueError, TypeError):
                logging.warning(f"Invalid DUCKDB_MEMORY_MB value '{memory}', using default 1536MB")
                memory = "1536MB"
        config = {
            "temp_directory": DUCK_DB_DIR,
            "extension_directory": os.path.join(DUCK_DB_DIR, "extensions"),
            "threads": threads,
            "memory_limit": memory,
            "max_memory": memory,
        }
        logging.info(f"Initializing DuckDB connection with db_path: {db_path}")
        logging.info(f"DuckDB config: threads={threads}, memory_limit={memory}, temp_directory={DUCK_DB_DIR}")
        conn = duckdb.connect(database=db_path, config=config)
        try:
            version = conn.execute("SELECT version()").fetchone()[0]
            logging.info(f"DuckDB version: {version}")
        except Exception:
            logging.warning("Could not determine DuckDB version")
        temp_dir = DUCK_DB_DIR
        try:
            result = conn.execute("SELECT value FROM duckdb_settings() WHERE name='temp_directory'").fetchone()
            if result:
                temp_dir = result[0]
        except Exception:
            try:
                result = conn.execute("PRAGMA temp_directory").fetchone()
                if result:
                    temp_dir = result[0]
            except Exception:
                pass
        logging.info(f"DuckDB temp_directory: {temp_dir}")
        return conn

    def setup_connection(self):
        """Setup DuckDB connection with S3 credentials and performance
        optimizations."""
        if self.con:
            return

        logging.info("Setting up DuckDB connection...")
        self.con = self._init_connection()
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        self.con.execute(f"SET s3_region='{self.config.aws_parameters.aws_region}';")
        self.con.execute(f"SET s3_access_key_id='{self.config.aws_parameters.api_key_id}';")
        self.con.execute(f"SET s3_secret_access_key='{self.config.aws_parameters.api_key_secret}';")

        if self.config.debug:
            try:
                threads = self.con.execute("PRAGMA threads").fetchone()[0]
                memory_limit = self.con.execute("PRAGMA memory_limit").fetchone()[0]
                logging.debug(f"DuckDB effective settings - threads: {threads}, memory_limit: {memory_limit}")
            except Exception as e:
                logging.debug(f"Could not read DuckDB settings: {e}")

    def load_csv_files_bulk(self, csv_patterns: list[str], batch_size: int = 10) -> bool:
        """Load CSV files from mixed patterns (S3 and local) using DuckDB bulk loading with batching.

        Loads files in batches to avoid OOM errors with large datasets.
        Each batch is loaded and committed before the next batch starts.
        Handles schema evolution by reconciling columns between batches.
        """
        if not csv_patterns:
            logging.info("No CSV patterns to load")
            return False
        s3_count = sum(1 for p in csv_patterns if p.startswith("s3://"))
        local_count = len(csv_patterns) - s3_count
        total_batches = (len(csv_patterns) + batch_size - 1) // batch_size
        logging.info(
            f"Loading {len(csv_patterns)} CSV files ({s3_count} from S3, {local_count} local) "
            f"in {total_batches} batches of {batch_size}..."
        )
        try:
            table_created = False
            for batch_num, i in enumerate(range(0, len(csv_patterns), batch_size), 1):
                batch = csv_patterns[i:i + batch_size]
                patterns_str = "', '".join(batch)
                logging.info(f"Loading batch {batch_num}/{total_batches}: {len(batch)} files")
                if not table_created:
                    self.con.execute(f"""
                        CREATE TABLE {RAW_REPORTS_TABLE} AS
                        SELECT *
                        FROM read_csv_auto(['{patterns_str}'],
                                           HEADER=TRUE,
                                           ALL_VARCHAR=TRUE,
                                           NULLSTR=['null', 'NULL', 'None'],
                                           union_by_name=true,
                                           filename=true);
                    """)
                    table_created = True
                else:
                    batch_cols = self._get_batch_columns(patterns_str)
                    table_cols = self._get_table_columns_with_filename()
                    new_cols = [c for c in batch_cols if c not in table_cols]
                    if new_cols:
                        logging.info(f"Adding {len(new_cols)} new columns to table: {new_cols[:5]}...")
                        for col in new_cols:
                            self.con.execute(f'ALTER TABLE {RAW_REPORTS_TABLE} ADD COLUMN "{col}" VARCHAR;')
                        table_cols = self._get_table_columns_with_filename()
                    select_parts = []
                    for col in table_cols:
                        if col in batch_cols:
                            select_parts.append(f'"{col}"')
                        else:
                            select_parts.append(f'NULL AS "{col}"')
                    select_sql = ", ".join(select_parts)
                    self.con.execute(f"""
                        INSERT INTO {RAW_REPORTS_TABLE}
                        SELECT {select_sql}
                        FROM read_csv_auto(['{patterns_str}'],
                                           HEADER=TRUE,
                                           ALL_VARCHAR=TRUE,
                                           NULLSTR=['null', 'NULL', 'None'],
                                           union_by_name=true,
                                           filename=true);
                    """)
                row_count = self.con.execute(f"SELECT COUNT(*) FROM {RAW_REPORTS_TABLE}").fetchone()[0]
                logging.info(f"Batch {batch_num}/{total_batches} loaded. Total rows: {row_count}")
            return True
        except Exception as e:
            logging.error(f"Failed to load CSV files bulk: {e}")
            return False

    def _get_batch_columns(self, patterns_str: str) -> list[str]:
        """Get column names from a batch of CSV files using LIMIT 0 probe."""
        try:
            result = self.con.execute(f"""
                DESCRIBE SELECT *
                FROM read_csv_auto(['{patterns_str}'],
                                   HEADER=TRUE,
                                   ALL_VARCHAR=TRUE,
                                   union_by_name=true,
                                   filename=true)
                LIMIT 0;
            """).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logging.error(f"Failed to get batch columns: {e}")
            return []

    def _get_table_columns_with_filename(self) -> list[str]:
        """Get all column names from table including filename column."""
        try:
            result = self.con.execute(f"SELECT name FROM pragma_table_info('{RAW_REPORTS_TABLE}');").fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logging.error(f"Failed to get table columns: {e}")
            return []

    def get_current_columns_from_table(self, table_name: str = RAW_REPORTS_TABLE) -> list[str]:
        """Get current columns from DuckDB table."""
        try:
            columns = [
                r[0]
                for r in self.con.execute(f"DESCRIBE {table_name};").fetchall()
                if r[0] != FILENAME_COLUMN  # filter out metadata column
            ]
            return columns
        except Exception as e:
            logging.error(f"Failed to get columns from table '{table_name}': {e}")
            return []

    def create_unified_view(self, final_columns: list[str], current_columns: list[str]) -> bool:
        """Create a unified view with all columns."""
        select_parts = []

        for col in final_columns:
            # Convert back from KBC format to original
            original_col = col.replace("__", "/")
            if original_col in current_columns:
                select_parts.append(f'"{original_col}" as "{col}"')
            else:
                select_parts.append(f'NULL as "{col}"')

        select_sql = ", ".join(select_parts)

        try:
            self.con.execute(f"""
                CREATE VIEW {UNIFIED_REPORTS_VIEW} AS
                SELECT {select_sql}
                FROM {RAW_REPORTS_TABLE};
            """)
            return True
        except Exception as e:
            logging.error(f"Failed to create unified view: {e}")
            return False

    def export_data_to_csv(self, output_path: str):
        """Export data from DuckDB table to CSV file."""
        self.con.execute(f"COPY {UNIFIED_REPORTS_VIEW} TO '{output_path}' (HEADER, DELIMITER ',');")
        logging.info(f"Data exported to {output_path}")
