import pytest

from connectors.file_system import FileSystemConnector
from engine import Engine
from config import (
    Config,
    MarkRedacted,
    MarkRedactedParameters,
    DatasetConfig,
    DataSource,
    Metric,
    Aggregations,
)


@pytest.fixture()
def file_system_config() -> Config:
    return Config(
        datasource=DataSource(
            connection_type="file",
            parameters={"output_directory": "/tmp/"},
        ),
        datasets=[
            DatasetConfig(
                source_file="./tests/data_outputs/library_example.csv",
                dimensions=["age", "sex", "zip_code", "library_friend"],
                metrics=[
                    Metric(aggregation=Aggregations.SUM, column="count", alias="count")
                ],
                suppression_strategies=[
                    MarkRedacted(
                        parameters=MarkRedactedParameters(redacted_dimension="sex")
                    )
                ],
            )
        ],
    )


def test_basic_mark_redaction(file_system_config):
    """
    prove that the engine runs and marks rows redacted in an expected way.
    """
    engine = Engine(config=file_system_config)
    output_file = "/tmp/output.parquet"
    engine.datasets[0].output_file = output_file
    engine.run()
    validation_sql = f""" select * from '{output_file}' """
    t = engine.connector.duckdb_connection.sql(validation_sql)
    anonymous_count, *_ = t.filter("not is_anonymous").count("*").fetchone()
    redaction_count, *_ = t.filter("is_redacted").count("*").fetchone()
    assert anonymous_count == 1
    assert redaction_count == 2
