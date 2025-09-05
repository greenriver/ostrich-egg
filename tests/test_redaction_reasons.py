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

test_file = os.path.join(DATA_INPUTS_DIRECTORY, "redaction_examples.json")

REDACTION_EXPRESSION = """\
case
    when population_value is null then true
    when incidence = 0 then false
    when incidence < 11 and population_value >= 2500 and population_value < 20000 then true
    when population_value >= 20000 then false
    when population_value < 2500 then true
    else false
end
"""

DIMENSIONS = [
    "month",
    "county",
    "municipality",
    "zip_code",
]

INCIDENCE_METRIC = Metric(
    aggregation=Aggregations.SUM,
    column="incidence",
    alias="incidence",
    is_initial=True,
    is_subsequent=True,
)
POPULATION_METRIC = Metric(
    aggregation=Aggregations.MAX,
    column="population_value",
    alias="population_value",
    is_initial=True,
    is_subsequent=True,
)

PURPOSE_METRIC = Metric(
    aggregation=Aggregations.ANY_VALUE,
    column="purpose",
    alias="purpose",
    is_initial=True,
    is_subsequent=True,
)
EXPECTED_TO_BE_REDACTED_METRIC = Metric(
    aggregation=Aggregations.ANY_VALUE,
    column="expected_to_be_redacted",
    alias="expected_to_be_redacted",
    is_initial=True,
    is_subsequent=True,
)


class TestRedactionReasons:

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
                    redaction_order_dimensions=["month", "county"],
                    metrics=[
                        INCIDENCE_METRIC,
                        POPULATION_METRIC,
                        PURPOSE_METRIC,
                        EXPECTED_TO_BE_REDACTED_METRIC,
                    ],
                    output_file="redaction_examples_metrics.csv",
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

    def test_explicit_subsequent_metric(self, explicit_config):
        """
        Assert redaction happens in a clearly understandable, deterministic way.
        """
        import json

        engine = Engine(config=explicit_config)
        engine.run()
        output = engine.db.read_csv(engine.active_dataset.output_file).order(
            "month, county, municipality, zip_code"
        )  # noqa: F841
        results = [
            r[0]
            for r in engine.db.sql(
                "select x from (select * replace( peer_group::json as peer_group, redacted_peers::json as redacted_peers) from output) x "
            ).fetchall()
        ]
        for result in results:
            if result["peer_group"] is not None:
                result["peer_group"] = json.loads(result["peer_group"])
            if result["redacted_peers"] is not None:
                result["redacted_peers"] = json.loads(result["redacted_peers"])
        output_file = os.path.join(DATA_INPUTS_DIRECTORY, "redaction_outputs.json")
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        for result in results:
            is_redacted = result["is_redacted"]
            redaction_reason = result["redaction_reason"]
            assert (
                result["expected_to_be_redacted"] == result["is_redacted"]
            ), f"{result=}  was not what we expected"
            if not (
                (is_redacted and redaction_reason is not None)
                or (not is_redacted and redaction_reason is None)
            ):
                print(f"{result=}")
            assert (is_redacted and redaction_reason is not None) or (
                not is_redacted and redaction_reason is None
            ), f"{result=}  mismatches redaction flag and reason"
