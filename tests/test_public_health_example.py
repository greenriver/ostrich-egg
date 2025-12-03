import os
import json
import yaml

from engine import Engine
from config import (
    Config,
    Metric,
    Aggregations,
)

from conftest import DATA_INPUTS_DIRECTORY

public_health_data_file = os.path.join(
    DATA_INPUTS_DIRECTORY, "public_health_example.json"
)

public_health_config_file = os.path.join(
    DATA_INPUTS_DIRECTORY, "public_health_config.yml"
)


def test_public_health_example():
    """
    Test that the public health example produces expected redaction annotations.
    """
    with open(public_health_config_file, "r") as f:
        yaml_data = yaml.safe_load(f)
        config = Config(**yaml_data)

    # Set the source file to the JSON data
    config.datasets[0].source_file = public_health_data_file

    # Add metrics for expected_to_be_redacted and note to preserve them in output
    config.datasets[0].metrics.extend(
        [
            Metric(
                aggregation=Aggregations.ANY_VALUE,
                column="expected_to_be_redacted",
                alias="expected_to_be_redacted",
                is_initial=True,
                is_subsequent=True,
            ),
            Metric(
                aggregation=Aggregations.ANY_VALUE,
                column="note",
                alias="note",
                is_initial=True,
                is_subsequent=True,
            ),
        ]
    )

    # Run the engine
    engine = Engine(config=config)
    engine.run()

    # Get output and verify redaction annotations
    output = engine.db.read_parquet(engine.datasets[0].output_file).order(
        "county, month, age_range"
    )

    # Find rows where expected_to_be_redacted doesn't match is_redacted
    errors = (
        output.filter("expected_to_be_redacted != is_redacted")
        .to_df()
        .to_dict(orient="records")
    )

    assert (
        not errors
    ), f"The following errors were found where expected_to_be_redacted != is_redacted:\n{json.dumps(errors, indent=2)}"
