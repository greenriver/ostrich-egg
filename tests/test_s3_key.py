import pytest

from engine import Engine
from config import (
    Config,
    DatasetConfig,
    DataSource,
    Metric,
    Aggregations,
)

from conftest import TEST_S3_PARAMS

TEST_BUCKET_NAME = "test-bucket"


@pytest.fixture(autouse=True)
def mock_s3_bucket(mocked_s3_res):
    bucket = TEST_BUCKET_NAME
    mocked_s3_res.create_bucket(Bucket=bucket)
    return bucket


def test_s3_uri_formatting():
    engine = Engine(
        output_bucket="test",
        output_prefix="test_prefix",
        config=Config(
            datasource=DataSource(
                connection_type="s3",
                parameters={
                    "bucket": TEST_BUCKET_NAME,
                    "key": "test.csv",
                    **TEST_S3_PARAMS,
                },
            ),
            datasets=[
                DatasetConfig(
                    name="test",
                    dimensions=["a", "b"],
                    metrics=[
                        Metric(aggregation=Aggregations.SUM, column="c", alias="c")
                    ],
                    suppression_strategies=[],
                )
            ],
        ),
    )
    assert engine.get_absolute_source_file("s3://test/test.csv") == "s3://test/test.csv"
    assert (
        engine.get_absolute_source_file("test_prefix/test.csv")
        == "s3://test/test_prefix/test.csv"
    )
    assert (
        engine.get_absolute_source_file("test.csv") == "s3://test/test_prefix/test.csv"
    )
