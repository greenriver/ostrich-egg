from dataclasses import dataclass
import csv
import os

import pytest

from conftest import TEST_DIRECTORY
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

POPULATION_FILE_NAME = os.path.join(
    TEST_DIRECTORY, "data_inputs/read_only_compound_population_example.csv"
)

REDACTION_EXPRESSION = """\
case
    when incidence < 11 and population >= 2500 and population < 20000 then true
    when population >= 20000 then false
    when population < 2500 then true
    else false
end
"""

# These are the test dimensions that we don't want to affect peering in the suppression engine.
EXCLUDED_DIMENSIONS = ["expected_to_be_redacted", "expected_to_be_anonymous"]


@dataclass
class PopulationTestRow:
    sex: str
    zip: str
    incidence: int
    population: int
    expected_to_be_anonymous: bool = False
    _expected_to_be_redacted: bool = None

    def to_dict(self) -> dict:
        columns = self.get_header()
        return {dimension: getattr(self, dimension) for dimension in columns}

    @classmethod
    def get_fact_names(cls) -> list[str]:
        return ["incidence", "population"]

    @classmethod
    def get_dimensions(cls) -> list[str]:
        dimensions = [
            dim
            for dim in cls.__dataclass_fields__.keys()
            if dim not in cls.get_fact_names()
        ]
        dimensions.remove("_expected_to_be_redacted")
        dimensions.append("expected_to_be_redacted")
        return dimensions

    @classmethod
    def get_header(cls):
        return [*cls.get_dimensions(), *cls.get_fact_names()]

    @property
    def expected_to_be_redacted(self) -> bool:
        if self._expected_to_be_redacted is None:
            return not self.expected_to_be_anonymous
        return self._expected_to_be_redacted

    @expected_to_be_redacted.setter
    def expected_to_be_redacted(self, val: bool):
        self._expected_to_be_redacted = val


@pytest.fixture()
def compound_redaction_expression_config() -> Config:
    return Config(
        datasource=DataSource(
            connection_type="file",
            parameters={
                "output_directory": os.path.join(TEST_DIRECTORY, "data_outputs")
            },
        ),
        redaction_expression=REDACTION_EXPRESSION,
        datasets=[
            DatasetConfig(
                name="test_compound_redaction",
                source_file=POPULATION_FILE_NAME,
                dimensions=[
                    dim
                    for dim in PopulationTestRow.get_dimensions()
                    if dim not in EXCLUDED_DIMENSIONS
                ],
                metrics=[
                    Metric(
                        aggregation=Aggregations.SUM,
                        column="incidence",
                        alias="incidence",
                    ),
                    Metric(
                        aggregation=Aggregations.SUM,
                        column="population",
                        alias="population",
                    ),
                ],
                suppression_strategies=[
                    MarkRedacted(
                        parameters=MarkRedactedParameters(redacted_dimension="sex")
                    )
                ],
            )
        ],
    )


def generate_population_redaction_rule_test_data():
    low_incidence_midsize_population = PopulationTestRow(
        sex="M",
        zip="00000",
        incidence=1,
        population=10_000,
        expected_to_be_anonymous=False,
    )
    latent_1 = PopulationTestRow(
        sex="F",
        zip="00000",
        incidence=12,
        population=10_000,
        expected_to_be_anonymous=True,
        _expected_to_be_redacted=True,
    )

    low_incidence_big_population = PopulationTestRow(
        sex="F",
        zip="00001",
        incidence=22,
        population=20_000,
        expected_to_be_anonymous=True,
    )
    normal_incidence_midsize_population = PopulationTestRow(
        sex="M",
        zip="00002",
        incidence=15,
        population=10_000,
        expected_to_be_anonymous=True,
    )
    normal_incidence_small_population = PopulationTestRow(
        sex="M",
        zip="00003",
        incidence=15,
        population=1_000,
        expected_to_be_anonymous=False,
    )
    # This one will actually get latently redacted.
    # TODO: The determinism on latent redaction, particularly across peers, needs improvement
    latent_low_incidence_big_population = PopulationTestRow(
        sex="M",
        zip="00001",
        incidence=2,
        population=20_000,
        expected_to_be_anonymous=True,
        _expected_to_be_redacted=True,
    )
    dict_rows = [
        row.to_dict()
        for row in [
            low_incidence_midsize_population,
            latent_1,
            low_incidence_big_population,
            normal_incidence_midsize_population,
            normal_incidence_small_population,
            latent_low_incidence_big_population,
        ]
    ]
    with open(POPULATION_FILE_NAME, "w+") as f:
        writer = csv.DictWriter(
            f=f,
            fieldnames=PopulationTestRow.get_header(),
        )
        writer.writeheader()
        writer.writerows(dict_rows)


def test_compound_threshold_expression(compound_redaction_expression_config):
    generate_population_redaction_rule_test_data()
    engine = Engine(config=compound_redaction_expression_config)
    engine.run()

    output_table = engine.db.read_parquet(  # noqa: F841
        engine.active_dataset.output_file
    )
    source_table = engine.db.read_csv(POPULATION_FILE_NAME)  # noqa: F841
    validation_sql = """
    select as_struct
    from (
        select zip, sex, source_table.expected_to_be_anonymous, source_table.expected_to_be_redacted,
            output_table.is_anonymous, output_table.is_redacted
        from source_table
        left join output_table
            using(zip, sex)
        where output_table.is_anonymous is null
        or output_table.is_anonymous <> source_table.expected_to_be_anonymous
        or output_table.is_redacted <> source_table.expected_to_be_redacted
    ) as as_struct
    """
    failures = engine.db.sql(validation_sql).fetchone()
    if failures:
        failures = failures[0]
    assert not failures
