import logging
import os
from typing import List, Dict

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
    def _init_connection(
        threads: int = 4, max_memory: int = 1024, db_path: str = ":memory:"
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
            except Exception as e:
                logging.warning(f"Failed to close DuckDB connection: {e}")
                pass
            self.con = None

    def load_csv_files_bulk(self, csv_patterns: List[str]) -> bool:
        """Load CSV files from mixed patterns (S3 and local) using DuckDB bulk loading."""
        if not csv_patterns:
            logging.info("No CSV patterns to load")
            return False

        patterns_str = "', '".join(csv_patterns)
        s3_count = sum(1 for p in csv_patterns if p.startswith("s3://"))
        local_count = len(csv_patterns) - s3_count

        logging.info(
            f"Loading {len(csv_patterns)} CSV files ({s3_count} from S3, {local_count} local)..."
        )

        try:
            self.con.execute(f"""
                CREATE TABLE {RAW_REPORTS_TABLE} AS
                SELECT *
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
        self, table_name: str = RAW_REPORTS_TABLE
    ) -> List[str]:
        """Get current columns from DuckDB table."""
        try:
            columns = [
                r[1]
                for r in self.con.execute(
                    f"PRAGMA table_info('{table_name}');"
                ).fetchall()
                if r[1] != FILENAME_COLUMN  # filter out metadata column
            ]
            return columns
        except Exception as e:
            logging.error(f"Failed to get columns from table '{table_name}': {e}")
            return []

    def get_column_types_from_table(
        self, table_name: str = UNIFIED_REPORTS_VIEW
    ) -> Dict[str, str]:
        """Get column names and their data types from DuckDB table."""
        try:
            table_info = self.con.execute(
                f"PRAGMA table_info('{table_name}');"
            ).fetchall()

            # Convert DuckDB types to Keboola Storage types
            type_mapping = {
                "VARCHAR": "STRING",
                "BIGINT": "INTEGER",
                "INTEGER": "INTEGER",
                "DOUBLE": "NUMERIC",
                "DECIMAL": "NUMERIC",
                "FLOAT": "NUMERIC",
                "BOOLEAN": "BOOLEAN",
                "DATE": "DATE",
                "TIMESTAMP": "TIMESTAMP",
                "TIME": "TIME",
            }

            column_types = {}
            for row in table_info:
                col_name = row[1]  # column name
                col_type = row[2].upper()  # column type

                # Map DuckDB type to KBC Storage type
                kbc_type = type_mapping.get(col_type, "STRING")
                column_types[col_name] = kbc_type

            return column_types
        except Exception as e:
            logging.error(f"Failed to get column types: {e}")
            return {}

    @staticmethod
    def _convert_data_type(column_name: str, data_type: str) -> str:
        """Convert column to appropriate DuckDB data type with safe casting."""
        conversion_map = {
            "INTEGER": f'TRY_CAST("{column_name}" AS BIGINT)',
            "NUMERIC": f'TRY_CAST("{column_name}" AS DOUBLE)',
            "BOOLEAN": f'TRY_CAST("{column_name}" AS BOOLEAN)',
            "DATE": f'TRY_CAST("{column_name}" AS DATE)',
            "TIMESTAMP": f'TRY_CAST("{column_name}" AS TIMESTAMP)',
            "TIME": f'TRY_CAST("{column_name}" AS TIME)',
            "STRING": f'"{column_name}"',  # No conversion needed for strings
        }

        return conversion_map.get(data_type, f'"{column_name}"')

    def create_unified_view_with_types(
        self,
        final_columns: List[str],
        current_columns: List[str],
        column_types: Dict[str, str],
    ) -> bool:
        """Create unified view with proper data type conversions."""
        select_parts = []

        for col in final_columns:
            # Convert back from KBC format to original
            original_col = col.replace("__", "/")
            data_type = column_types.get(col, "STRING")

            if original_col in current_columns:
                # Apply type conversion
                converted_col = self._convert_data_type(original_col, data_type)
                select_parts.append(f'{converted_col} as "{col}"')
            else:
                # Column not present - use typed NULL
                null_value = self._get_typed_null(data_type)
                select_parts.append(f'{null_value} as "{col}"')

        select_sql = ", ".join(select_parts)

        try:
            self.con.execute(f"""
                CREATE OR REPLACE VIEW {UNIFIED_REPORTS_VIEW} AS
                SELECT {select_sql}
                FROM {RAW_REPORTS_TABLE};
            """)
            return True
        except Exception as e:
            logging.error(f"Failed to create typed unified view: {e}")
            return False

    def _get_typed_null(self, data_type: str) -> str:
        """Get properly typed NULL value for given data type."""
        null_map = {
            "INTEGER": "CAST(NULL AS BIGINT)",
            "NUMERIC": "CAST(NULL AS DOUBLE)",
            "BOOLEAN": "CAST(NULL AS BOOLEAN)",
            "DATE": "CAST(NULL AS DATE)",
            "TIMESTAMP": "CAST(NULL AS TIMESTAMP)",
            "TIME": "CAST(NULL AS TIME)",
            "STRING": "CAST(NULL AS VARCHAR)",
        }

        return null_map.get(data_type, "CAST(NULL AS VARCHAR)")

    def create_unified_view(
        self, final_columns: List[str], current_columns: List[str]
    ) -> bool:
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
        self.con.execute(
            f"COPY {UNIFIED_REPORTS_VIEW} TO '{output_path}' (HEADER, DELIMITER ',');"
        )
        logging.info(f"Data exported to {output_path}")
