import logging
import os

import duckdb


class DuckDBClient:
    """
    DuckDB client for local processing of AWS Cost and Usage Reports.
    Replaces Snowflake workspace for local CSV processing.
    """

    def __init__(self):
        # Use in-memory database for processing
        # Set extension directory to /tmp to avoid permission issues in containers
        self._connection = duckdb.connect(':memory:', config={
            'extension_directory': '/tmp/duckdb_extensions',
            'temp_directory': '/tmp/duckdb_temp'
        })
        self._table_name = None
        self._output_path = None

    def open_connection(self):
        """Compatibility method - connection is always open"""
        pass

    def close(self):
        """Close the DuckDB connection"""
        if self._connection:
            self._connection.close()

    def create_table(self, name, columns: list[dict]):
        """
        Create a table in DuckDB with the specified columns.

        Args:
            name: Table name
            columns: List of dicts with 'name' and 'type' keys
        """
        self._table_name = name

        # Convert Snowflake TEXT type to VARCHAR
        col_defs = []
        for col in columns:
            col_type = col['type'].replace('TEXT', 'VARCHAR')
            col_defs.append(f'"{col["name"]}" {col_type}')

        query = f'CREATE TABLE IF NOT EXISTS "{name}" ({", ".join(col_defs)})'
        logging.debug(f"Creating table: {query}")
        self._connection.execute(query)

    def load_csv_file(self, table_name: str, original_columns: list[str],
                      normalized_columns: list[str], csv_file_path: str):
        """
        Load a CSV file into the DuckDB table.

        Args:
            table_name: Target table name
            original_columns: Original column names in CSV (e.g., "identity/LineItemId")
            normalized_columns: Normalized column names in table (e.g., "identity__LineItemId")
            csv_file_path: Path to the CSV file
        """
        logging.debug(f"Loading CSV file {csv_file_path} into table {table_name}")

        # Build SELECT with original names and aliases to normalized names
        select_parts = []
        for orig, norm in zip(original_columns, normalized_columns):
            select_parts.append(f'"{orig}" AS "{norm}"')
        select_str = ', '.join(select_parts)

        # Prepare normalized column list for INSERT
        norm_columns_quoted = [f'"{col}"' for col in normalized_columns]
        norm_columns_str = ', '.join(norm_columns_quoted)

        # Read CSV and insert into table
        query = f"""
        INSERT INTO "{table_name}" ({norm_columns_str})
        SELECT {select_str}
        FROM read_csv_auto('{csv_file_path}',
            header=true,
            ignore_errors=true,
            sample_size=-1
        )
        """

        try:
            self._connection.execute(query)
            logging.debug(f"Successfully loaded {csv_file_path}")
        except Exception as e:
            logging.error(f"Error loading CSV file {csv_file_path}: {e}")
            raise

    def load_csv_from_s3(self, table_name: str, original_columns: list[str],
                         normalized_columns: list[str], s3_path: str,
                         aws_access_key_id: str, aws_secret_access_key: str):
        """
        Load CSV directly from S3 using DuckDB's httpfs extension.

        Args:
            table_name: Target table name
            original_columns: Original column names in CSV (e.g., "identity/LineItemId")
            normalized_columns: Normalized column names in table (e.g., "identity__LineItemId")
            s3_path: S3 path (s3://bucket/key)
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret key
        """
        logging.debug(f"Loading from S3: {s3_path}")

        # Install and load httpfs extension for S3 access
        self._connection.execute("INSTALL httpfs")
        self._connection.execute("LOAD httpfs")

        # Set S3 credentials
        self._connection.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        self._connection.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")

        # Build SELECT with original names and aliases to normalized names
        select_parts = []
        for orig, norm in zip(original_columns, normalized_columns):
            select_parts.append(f'"{orig}" AS "{norm}"')
        select_str = ', '.join(select_parts)

        # Prepare normalized column list for INSERT
        norm_columns_quoted = [f'"{col}"' for col in normalized_columns]
        norm_columns_str = ', '.join(norm_columns_quoted)

        # Read from S3 and insert into table
        query = f"""
        INSERT INTO "{table_name}" ({norm_columns_str})
        SELECT {select_str}
        FROM read_csv_auto('{s3_path}',
            header=true,
            ignore_errors=true,
            sample_size=-1
        )
        """

        try:
            self._connection.execute(query)
            logging.debug(f"Successfully loaded from S3: {s3_path}")
        except Exception as e:
            logging.error(f"Error loading from S3 {s3_path}: {e}")
            raise

    def export_to_csv(self, table_name: str, output_path: str, columns: list[str]):
        """
        Export the table to a CSV file.

        Args:
            table_name: Source table name
            output_path: Path to output CSV file
            columns: List of column names to export (in order)
        """
        self._output_path = output_path

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Prepare column list with quotes
        columns_quoted = [f'"{col}"' for col in columns]
        columns_str = ', '.join(columns_quoted)

        # Export to CSV
        query = f"""
        COPY (SELECT {columns_str} FROM "{table_name}")
        TO '{output_path}' (HEADER, DELIMITER ',')
        """

        logging.info(f"Exporting table to {output_path}")
        self._connection.execute(query)
        logging.info(f"Successfully exported {table_name} to CSV")
