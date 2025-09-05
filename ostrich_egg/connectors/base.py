from abc import ABC as AbstractBaseClass, abstractmethod
from functools import cached_property

import duckdb

from ostrich_egg.utils import should_redact_along_axis

DEFAULT_TABLE_NAME = "dataset"
DEFAULT_RESULT_NAME = "result"


class BaseConnector(AbstractBaseClass):
    """
    Data can be read from s3, postgres, or a file system. Possibly other connectors could exist in the future.
    Every connector is going to have a duckdb connection associated with it which is used for running the results.

    If postgres, duckdb will attach to the postgres database to fetch either the query or the table/view to load in memory.

    Subsequent work on the results will be done in duckdb.

    The output can be written out to postgres, s3, or a file system..
    """

    extensions = (
        "httpfs",
        "aws",
        "spatial",
        "postgres",
    )

    def __init__(self, table_name=DEFAULT_TABLE_NAME, **kwargs):
        self.table_name = table_name
        self.init_duckdb()

    def __exit__(self):
        self.duckdb_connection.close()

    @cached_property
    def duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect()

    @property
    def db(self):
        return self.duckdb_connection

    def init_duckdb(self):
        try:
            del self.duckdb_connection
        except AttributeError:
            pass
        for extension in self.extensions:
            self.duckdb_connection.install_extension(extension)
            self.duckdb_connection.load_extension(extension)
        self.load_custom_functions()

    @abstractmethod
    def load_source_table(self, *args, **kwargs):
        """
        This is actually a duckdb specific requirement to create the table as the dataset.
        If this were postgres, you wouldn't actually need to create a table.
        """
        raise NotImplementedError("Connectors must implement a create table interface")

    def load_custom_functions(self):
        should_redact_function_exists_count = (
            self.db.sql(
                "from duckdb_functions() where function_name = 'should_redact_along_axis'"
            )
            .count("*")
            .fetchone()[0]
        )
        if should_redact_function_exists_count == 1:
            self.db.remove_function("should_redact_along_axis")
        self.db.create_function("should_redact_along_axis", should_redact_along_axis)
