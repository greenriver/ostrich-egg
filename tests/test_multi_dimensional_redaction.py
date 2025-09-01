import os
import json
import pytest

from engine import Engine
from config import (
    Config,
    DatasetConfig,
    DataSource,
    Metric,
    Aggregations,
    MarkRedacted,
    MarkRedactedParameters,
)

from conftest import DATA_INPUTS_DIRECTORY

multi_dimensional_redaction_file = os.path.join(
    DATA_INPUTS_DIRECTORY, "multi-dimensional-revelation.json"
)


@pytest.fixture()
def multi_dimensional_redaction_config():
    return Config(
        datasource=DataSource(
            connection_type="file",
            parameters={"output_directory": "/tmp/"},
        ),
        datasets=[
            DatasetConfig(
                dimensions=["county", "month"],
                source_file=multi_dimensional_redaction_file,
                metrics=[
                    Metric(
                        aggregation=Aggregations.SUM,
                        column="incidence",
                        alias="incidence",
                        initial=True,
                        subsequent=True,
                    ),
                    Metric(
                        aggregation=Aggregations.ANY_VALUE,
                        column="expected_to_be_redacted",
                        alias="expected_to_be_redacted",
                        initial=True,
                        subsequent=True,
                    ),
                    Metric(
                        aggregation=Aggregations.ANY_VALUE,
                        column="note",
                        alias="note",
                        initial=True,
                        subsequent=True,
                    ),
                ],
                suppression_strategies=[
                    MarkRedacted(
                        parameters=MarkRedactedParameters(redacted_dimension="county")
                    )
                ],
            )
        ],
    )


def test_multi_dimensional_redaction(multi_dimensional_redaction_config):
    engine = Engine(config=multi_dimensional_redaction_config)
    engine.run()
    output = engine.db.table(engine.datasets[0].output_file).order("county, month")
    errors = (
        output.filter("expected_to_be_redacted != is_redacted")
        .to_df()
        .to_dict(orient="records")
    )

    assert (
        not errors
    ), f"The following errors were found\n{json.dumps(errors, indent=2)}"
