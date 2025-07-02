import os
import pytest

from conftest import DATA_INPUTS_DIRECTORY, DATA_OUTPUTS_DIRECTORY
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

test_file = os.path.join(DATA_INPUTS_DIRECTORY, "count_based.json")

generate_sql = f"""
    select row_number() over() as id, *
    from '{test_file}', generate_series(1, count) as gen
"""

REDACTION_EXPRESSION = """\
case
    when incidence < 11 and population >= 2500 and population < 20000 then true
    when population >= 20000 then false
    when population < 2500 then true
    else false
end
"""

DIMENSIONS = [
    "month",
    "county",
    "zip_code",
]

INITIAL_INCIDENCE_METRIC = Metric(
    aggregation=Aggregations.COUNT,
    column="*",
    alias="incidence",
    is_initial=True,
)
SUBSEQUENT_INCIDENCE_METRIC = Metric(
    aggregation=Aggregations.SUM,
    column="incidence",
    alias="incidence",
    is_initial=False,
)
INITIAL_POPULATION_METRIC = Metric(
    aggregation=Aggregations.ANY_VALUE,
    column="zip_code_population",
    alias="population",
    is_initial=True,
)
SUBSEQUENT_POPULATION_METRIC = Metric(
    aggregation=Aggregations.ANY_VALUE,
    column="population",
    alias="population",
    is_initial=False,
)


class TestInitialMetrics:

    @pytest.fixture()
    def implicit_config(self):
        return Config(
            datasource=DataSource(
                connection_type="file",
                parameters={
                    "file_path": test_file,
                    "output_directory": DATA_OUTPUTS_DIRECTORY,
                },
            ),
            datasets=[
                DatasetConfig(
                    name="base",
                    dimensions=DIMENSIONS,
                    unit_level_id="id",
                    output_file="implicit_metric.csv",
                    sql=generate_sql,
                    suppression_strategies=[
                        MarkRedacted(
                            parameters=MarkRedactedParameters(
                                redacted_dimension="zip_code"
                            )
                        )
                    ],
                ),
            ],
        )

    @pytest.fixture()
    def explicit_config(self):
        return Config(
            datasource=DataSource(
                connection_type="file",
                parameters={
                    "file_path": test_file,
                    "output_directory": DATA_OUTPUTS_DIRECTORY,
                },
            ),
            redaction_expression=REDACTION_EXPRESSION,
            datasets=[
                DatasetConfig(
                    name="base",
                    dimensions=DIMENSIONS,
                    unit_level_id="id",
                    metrics=[
                        INITIAL_INCIDENCE_METRIC,
                        SUBSEQUENT_INCIDENCE_METRIC,
                        INITIAL_POPULATION_METRIC,
                        SUBSEQUENT_POPULATION_METRIC,
                    ],
                    output_file="implicit_metric.csv",
                    sql=generate_sql,
                    suppression_strategies=[
                        MarkRedacted(
                            parameters=MarkRedactedParameters(
                                redacted_dimension="zip_code"
                            )
                        )
                    ],
                ),
            ],
        )

    def test_implicit_metric(self, implicit_config):
        """
        Assert that the default metric situation for a row-level counting dataset iterates on the aggregations appropriately.
        """
        engine = Engine(config=implicit_config)
        engine.run()
        assert str(engine.metrics[0].aggregation) == "count_distinct"
        assert str(engine.metrics[1].aggregation) == "sum"

    def test_explicit_subsequent_metric(self, explicit_config):
        """
        Assert that the explicit metric situation for a row-level counting dataset iterates on the aggregations appropriately.
        """
        engine = Engine(config=explicit_config)
        engine.run()
        output = engine.db.read_csv(engine.active_dataset.output_file)  # noqa: F841
        results = [r[0] for r in engine.db.sql("select output from output").fetchall()]
        assert {
            "incidence": 20,
            "population": 4000,
            "month": "2025-01",
            "county": "A",
            "zip_code": 23456,
            "is_anonymous": True,
            "is_redacted": True,
        } in results
        assert {
            "incidence": 10,
            "population": 3000,
            "month": "2025-01",
            "county": "A",
            "zip_code": 12345,
            "is_anonymous": False,
            "is_redacted": True,
        } in results
        assert {
            "incidence": 21,
            "population": 4000,
            "month": "2025-01",
            "county": "B",
            "zip_code": 23456,
            "is_anonymous": True,
            "is_redacted": False,
        } in results
