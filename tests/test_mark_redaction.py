import pytest

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
                source_file="./tests/data_inputs/library_example.csv",
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
    non_anonymous_count, *_ = t.filter("not is_anonymous").count("*").fetchone()
    redaction_count, *_ = t.filter("is_redacted").count("*").fetchone()
    assert non_anonymous_count == 1
    assert redaction_count == 2

    # confirm that suppression strategies are deserialized correctly (i.e., assert no error)
    file_system_config.datasets[0].suppression_strategies[0] = {
        "strategy": "mark-redacted",
        "parameters": {"redacted_dimension": "sex"},
    }
    engine = Engine(config=file_system_config)
    output_file = "/tmp/output.parquet"
    engine.datasets[0].output_file = output_file
    engine.run()
