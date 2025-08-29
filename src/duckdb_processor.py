import logging
import os
from typing import List

import duckdb
from duckdb import DuckDBPyConnection

from configuration import Configuration

# DuckDB temporary directory configuration
DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class DuckDBProcessor:
    """Handles all DuckDB operations for report data processing."""

    def __init__(self, config: Configuration):
        self.config = config
        self.con = None

    def _init_connection(
        self, threads: int = 4, max_memory: int = 1024, db_path: str = ":memory:"
    ) -> DuckDBPyConnection:
        """
        Returns connection to temporary DuckDB database with advanced optimizations.
        DuckDB supports thread-safe access to a single connection.
        """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        # Enhanced configuration with performance optimizations
        # Using only definitely valid DuckDB configuration parameters
        config = {
            # Basic settings
            "temp_directory": DUCK_DB_DIR,
            "threads": threads,
            "max_memory": f"{max_memory}MB",
            "extension_directory": os.path.join(DUCK_DB_DIR, "extensions"),
            # Performance optimizations
            "preserve_insertion_order": False,  # Faster inserts
        }

        logging.info(f"Initializing DuckDB connection with config: {config}")
        conn = duckdb.connect(database=db_path, config=config)
        return conn

    def setup_connection(self):
        """Setup DuckDB connection with S3 credentials and performance optimizations."""
        if self.con:
            return  # already setup

        logging.info("Setting up DuckDB connection...")
        self.con = self._init_connection()
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        self.con.execute(f"SET s3_region='{self.config.aws_parameters.aws_region}';")
        self.con.execute(
            f"SET s3_access_key_id='{self.config.aws_parameters.api_key_id}';"
        )
        self.con.execute(
            f"SET s3_secret_access_key='{self.config.aws_parameters.api_key_secret}';"
        )

    def cleanup_connection(self):
        """Cleanup DuckDB connection."""
        if self.con:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None

    def load_csv_files_bulk(self, s3_patterns: List[str]) -> bool:
        """Load CSV files from S3 patterns using DuckDB bulk loading."""
        if not s3_patterns:
            logging.info("No CSV patterns to load")
            return False

        patterns_str = "', '".join(s3_patterns)
        logging.info(f"Loading {len(s3_patterns)} CSV patterns from S3...")

        try:
            self.con.execute(f"""
                CREATE TABLE raw_reports AS
                SELECT *, filename() as source_file
                FROM read_csv_auto(['{patterns_str}'],
                                   HEADER=TRUE,
                                   ALL_VARCHAR=TRUE,
                                   union_by_name=true,
                                   filename=true);
            """)
            return True
        except Exception as e:
            logging.error(f"Failed to load CSV files bulk: {e}")
            return False

    def get_current_columns_from_table(
        self, table_name: str = "raw_reports"
    ) -> List[str]:
        """Get current columns from DuckDB table."""
        try:
            columns = [
                r[1]
                for r in self.con.execute(
                    f"PRAGMA table_info('{table_name}');"
                ).fetchall()
                if r[1] != "source_file"  # filter out metadata column
            ]
            return columns
        except Exception:
            return []

    def create_unified_view(
        self, final_columns: List[str], current_columns: List[str]
    ) -> bool:
        """Create unified view with all columns."""
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
                CREATE VIEW unified_reports AS
                SELECT {select_sql}
                FROM raw_reports;
            """)
            return True
        except Exception as e:
            logging.error(f"Failed to create unified view: {e}")
            return False

    def create_result_table(self, table_name: str, columns: List[str]) -> str:
        """Create DuckDB table with fixed VARCHAR schema for stability."""
        cols_sql = ", ".join([f'"{c}" VARCHAR' for c in columns])
        self.con.execute(f'CREATE TABLE "{table_name}" ({cols_sql});')
        return table_name

    def load_chunk_into_staging_table(self, source_path: str):
        """Load chunk data into a temporary staging table for processing."""
        self.con.execute("DROP TABLE IF EXISTS tmp_stage;")
        self.con.execute(
            f"CREATE TEMP TABLE tmp_stage AS "
            f"SELECT * FROM read_csv_auto('{source_path}', HEADER=TRUE, ALL_VARCHAR=TRUE);"
        )

    def get_staging_table_columns(self) -> List[str]:
        """Get list of column names from the staging table."""
        return [
            row[1]
            for row in self.con.execute("PRAGMA table_info('tmp_stage');").fetchall()
        ]

    def insert_staging_data_to_result(
        self, result_table: str, column_mapping: List[str]
    ):
        """Insert data from staging table to result table with proper column alignment."""
        select_statement = ", ".join(column_mapping)
        self.con.execute(
            f'INSERT INTO "{result_table}" SELECT {select_statement} FROM tmp_stage;'
        )

    def export_data_to_csv(self, table_name: str, output_path: str):
        """Export data from DuckDB table to CSV file."""
        self.con.execute(
            f"COPY {table_name} TO '{output_path}' (HEADER, DELIMITER ',');"
        )
        logging.info(f"Data exported to {output_path}")
