from ostrich_egg.connectors.base import BaseConnector


class FileSystemConnector(BaseConnector):
    def __init__(self, file_path: str = None, output_directory: str = None, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.output_directory = output_directory

    def load_source_table(
        self, table_name: str = None, source_file: str = None, *args, **kwargs
    ):
        """
        If files become particular, we'll need to implement reading parameters (e.g., csv configurations, different file_types)
        """
        table_name = table_name or self.table_name
        file_path = source_file or self.file_path
        self.duckdb_connection.execute(
            f"create or replace table {table_name} as (select * from '{file_path}')"
        )
