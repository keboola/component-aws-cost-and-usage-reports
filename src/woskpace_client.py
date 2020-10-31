import sqlalchemy
from snowflake.sqlalchemy import URL


class SnowflakeClient:
    _DEFAULT_FILE_FORMAT = dict(TYPE="CSV", FIELD_DELIMITER="','", SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY="'\"'",
                                ERROR_ON_COLUMN_COUNT_MISMATCH=False)

    def __init__(self, account, user, password, database, schema, warehouse):
        self._engine = sqlalchemy.create_engine(URL(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            cache_column_metadata=True,
            echo=True
        ))
        self._connection: sqlalchemy.engine.Connection

    def execute_query(self, query):
        """
        executes the query
        """

        if self._connection.closed:
            self.open_connection()

        results = self._connection.execute(query).fetchall()
        return results

    def open_connection(self):
        self._connection = self._engine.connect()

    def close_connection(self):
        self._connection.close()

    def create_table(self, name, columns: [dict]):
        query = f"""
        CREATE OR REPLACE TABLE {self._wrap_in_quote(name)} (
        """
        col_defs = [f"\"{col['name']}\" {col['type']}" for col in columns]
        query += ', '.join(col_defs)
        query += ");"
        self.execute_query(query)

    def extend_table_columns(self, table_name, columns: [dict]):
        """
        Expand add non-existent columns to the table.
        :param table_name:
        :param columns: dictionary {'name': ,'type'}
        :return:
        """
        table_name = self._wrap_in_quote(table_name)
        existing_columns = self.get_table_column_names(table_name)
        for col in columns:
            if col['name'] not in existing_columns:
                self.execute_query(f"ALTER TABLE {table_name} ADD COLUMN {col['name']} {col['type']};")

    def get_table_column_names(self, table_name):
        table_name = self._wrap_in_quote(table_name)
        query = f"""select COLUMN_NAME
                        from INFORMATION_SCHEMA.COLUMNS
                        where TABLE_NAME = '{table_name}';
                """
        return [col['column_name'] for col in self.execute_query(query)]

    def copy_csv_into_table_from_s3(self, table_name, table_columns, path_to_object, aws_access_key_id,
                                    aws_secret_access_key,
                                    file_format: dict = _DEFAULT_FILE_FORMAT):
        """
        Import from S3 file by default CSV format with skip header setup and ERROR_ON_COLUMN_COUNT_MISMATCH=false.
        :param path_to_object:
        :param aws_access_key_id:
        :param aws_secret_access_key:
        :param file_format:
        :return:
        """
        table_name = self._wrap_in_quote(table_name)
        columns = self._wrap_columns_in_quotes(table_columns)
        query = f"""
        COPY INTO {table_name} ({', '.join(columns)}) FROM {path_to_object}
             CREDENTIALS =(aws_key_id = '{aws_access_key_id}' aws_secret_key = '{aws_secret_access_key}')
        """
        query += "FILE_FORMAT = ("
        for key in file_format:
            query += f"{key}={file_format[key]} "
        query += ");"
        self.execute_query(query)

    def _wrap_columns_in_quotes(self, columns):
        return [self._wrap_in_quote(col) for col in columns]

    def _wrap_in_quote(self, s):
        return s if s.startswith('"') else '"' + s + '"'

    def close(self):
        self._connection.close()
        self._engine.dispose()
