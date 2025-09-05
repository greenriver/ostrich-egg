import os
from uuid import uuid4
import pytest

from conftest import TEST_DIRECTORY, TEST_S3_PARAMS
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

files = [
    os.path.join(TEST_DIRECTORY, "data_inputs", f"{name}.csv")
    for name in ["test_joins_incidence", "test_joins_population"]
]
JOIN_SQL = """
    select
        incidence.*,
        population.population
    from '{incidence_file}' as incidence
    left join '{population_file}' as population
        on population.population_method = incidence.age
        and population.year = incidence.year
        and population.searchable_type = 'zip'
        and population.location = incidence.zip
"""

REDACTION_EXPRESSION = """\
case
    when incidence < 11 and population >= 2500 and population < 20000 then true
    when population >= 20000 then false
    when population < 2500 then true
    else false
end
"""

INCIDENCE_DIMENSIONS = [
    "age",
    "year",
    "zip",
    "expected_to_be_anonymous",
    "expected_to_be_redacted",
]

INCIDENCE_METRIC = Metric(
    aggregation=Aggregations.SUM,
    column="incidence",
    alias="incidence",
)
POPULATION_METRIC = Metric(
    aggregation=Aggregations.SUM,
    column="population",
    alias="population",
)

TEST_BUCKET_NAME = "test-bucket"


@pytest.fixture(autouse=True)
def mock_s3_bucket(mocked_s3_res):
    bucket = TEST_BUCKET_NAME
    mocked_s3_res.create_bucket(Bucket=bucket)
    return bucket


def file_name_as_key(prefix, file_name) -> str:
    return f"{prefix}/{os.path.basename(file_name)}"


@pytest.fixture(autouse=False)
def s3_files_prefix(mock_s3_bucket, mocked_s3_client):
    prefix = f"test_join-{uuid4()}"
    for file_name in files:
        mocked_s3_client.upload_file(
            Filename=file_name,
            Bucket=mock_s3_bucket,
            Key=file_name_as_key(prefix=prefix, file_name=file_name),
        )
    yield prefix
    for object_name in [file_name_as_key(prefix=prefix, file_name=f) for f in files]:
        mocked_s3_client.delete_object(
            Bucket=mock_s3_bucket,
            Key=object_name,
        )


class TestJoinExpressions:

    @pytest.fixture()
    def joined_config(self, mock_s3_bucket, moto_server):
        return Config(
            datasource=DataSource(
                connection_type="s3",
                parameters={
                    "bucket": mock_s3_bucket,
                    "key": "",
                    **TEST_S3_PARAMS | {"endpoint": moto_server.replace("http://", "")},
                },
            ),
            redaction_expression=REDACTION_EXPRESSION,
            datasets=[
                DatasetConfig(
                    name="joined",
                    dimensions=INCIDENCE_DIMENSIONS + ["population"],
                    metrics=[INCIDENCE_METRIC, POPULATION_METRIC],
                    output_file="joined.csv",
                    sql=JOIN_SQL,
                    suppression_strategies=[
                        MarkRedacted(
                            parameters=MarkRedactedParameters(redacted_dimension="age")
                        )
                    ],
                ),
            ],
        )

    def test_join_expression_redaction(
        self,
        s3_files_prefix,
        mock_s3_bucket,
        joined_config,
        moto_server,
    ):

        engine = Engine(
            config=joined_config,
            output_prefix=s3_files_prefix,
            output_bucket=mock_s3_bucket,
        )

        incidence_file = engine.get_absolute_source_file("test_joins_incidence.csv")
        population_file = engine.get_absolute_source_file("test_joins_population.csv")
        engine.datasets[0].sql = engine.datasets[0].sql.format(
            incidence_file=incidence_file, population_file=population_file
        )
        engine.run()
        validation_sql = f"select x from (select * from '{engine.datasets[0].output_file}' where (is_anonymous <> expected_to_be_anonymous) or (is_redacted <> expected_to_be_redacted)) x"
        result = engine.db.sql(validation_sql).fetchall()
        assert not result
