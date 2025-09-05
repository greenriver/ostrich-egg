import os
import json
import yaml

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

from conftest import DATA_INPUTS_DIRECTORY, DATA_OUTPUTS_DIRECTORY

multi_dimensional_redaction_file = os.path.join(
    DATA_INPUTS_DIRECTORY, "multi-dimensional-revelation.json"
)

redaction_order_config_file = os.path.join(
    DATA_INPUTS_DIRECTORY, "redaction_order_config.yml"
)

redaction_order_data_file = os.path.join(
    DATA_INPUTS_DIRECTORY, "redaction_order_data.csv"
)

redaction_order_output_file = os.path.join(
    DATA_INPUTS_DIRECTORY, "redaction_order_config_output.json"
)

multi_dimensional_redaction_config = Config(
    redaction_expression="incidence < 11",
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


def test_multi_dimensional_redaction():
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


def test_multi_dimensional_redaction_with_redaction_order_dimensions():
    with open(redaction_order_config_file, "r") as f:
        json_data = yaml.safe_load(f)
        config = Config(**json_data)
    config.datasets[0].source_file = redaction_order_data_file
    engine = Engine(config=config)
    db = engine.db
    engine.run()
    output = db.table(engine.datasets[0].output_file)
    expected_to_redact = (
        output.filter("is_redacted")
        .select("* replace(incidence::int as incidence)")
        .order("year, month, zip_code, peer_group nulls first")
    )
    expected_to_redact.to_df().to_json(
        redaction_order_output_file, orient="records", indent=2
    )
    errors = (
        output.filter("expected_to_redact != is_redacted")
        .to_df()
        .to_dict(orient="records")
    )
    assert (
        not errors
    ), f"The following errors were found\n{json.dumps(errors, indent=2)}"

    # now, let's add a new small cell to see if it impacts the previous result.
    new_data = db.sql(
        """
        select zip_code, population_value, year, month, incidence, expected_to_redact
        from output
            union
        select zip_code, population_value, 2022 as year, month, case when zip_code = '4' then 4 else incidence end as incidence,
        zip_code in ('4', '5') as expected_to_redact
        from output
        where year = 2021
        and month = 1
        """
    )
    output_file = redaction_order_data_file.replace(
        DATA_INPUTS_DIRECTORY, DATA_OUTPUTS_DIRECTORY
    )
    new_data.to_csv(output_file)
    config.datasets[0].source_file = output_file
    engine = Engine(config=config)
    engine.run()
    output = engine.db.table(engine.datasets[0].output_file)
    errors = (
        output.filter("expected_to_redact != is_redacted")
        .to_df()
        .to_dict(orient="records")
    )
    assert (
        not errors
    ), f"The following errors were found\n{json.dumps(errors, indent=2)}"


def generate_redaction_order_data():
    import duckdb

    db = duckdb.connect()
    useful_data = [
        {"zip_code": 1, "population_value": 10000},
        {"zip_code": 2, "population_value": 7000},
        {"zip_code": 3, "population_value": 6000},
        {"zip_code": 4, "population_value": 4000},
        {"zip_code": 5, "population_value": 1500},
    ]
    db.sql(
        f"""\
        select zip_code, population_value, unnest([2020, 2021]) as year, month,
        (random() * (100 - 5) )::int + 5 as incidence
        from (select unnest({useful_data}, recursive := true)) as data,
        generate_series(1, 12) as s(month)
        """
    ).to_csv(redaction_order_data_file)


if __name__ == "__main__":
    generate_redaction_order_data()
