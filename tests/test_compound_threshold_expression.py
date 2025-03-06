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
    TEST_DIRECTORY, "data_outputs/read_only_compound_population_example.csv"
)

REDACTION_EXPRESSION = """\
case
    when incidence < 11 and population >= 2500 and population < 20000 then true
    when population >= 20000 then false
    when population < 2500 then true
    else false
end
"""


@dataclass
class PopulationTestRow:
    sex: str
    zip: str
    incidence: int
    population: int
    expected_to_be_anonymous: bool = False
    _expected_to_be_redacted: bool = None

    def to_dict(self) -> dict:
        dimensions = self.get_dimensions()
        return {dimension: getattr(self, dimension) for dimension in dimensions}

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
            parameters={"output_directory": "/tmp/"},
        ),
        redaction_expression=REDACTION_EXPRESSION,
        parameters={"output_directory": "/tmp/"},
        datasets=[
            DatasetConfig(
                source_file=POPULATION_FILE_NAME,
                dimensions=PopulationTestRow.get_dimensions(),
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
                        parameters=MarkRedactedParameters(redacted_dimension="zip")
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
        sex="M",
        zip="00001",
        incidence=2,
        population=20_000,
        expected_to_be_anonymous=False,
    )
    normal_incidence_midsize_population = PopulationTestRow(
        sex="M",
        zip="00002",
        incidence=15,
        population=10_000,
        expected_to_be_anonymous=False,
    )
    normal_incidence_small_population = PopulationTestRow(
        sex="M",
        zip="00003",
        incidence=15,
        population=1_000,
        expected_to_be_anonymous=True,
    )
    dict_rows = [
        row.to_dict()
        for row in [
            low_incidence_midsize_population,
            latent_1,
            low_incidence_big_population,
            normal_incidence_midsize_population,
            normal_incidence_small_population,
        ]
    ]
    with open(POPULATION_FILE_NAME, "w+") as f:
        writer = csv.DictWriter(f=f, fieldnames=PopulationTestRow.get_dimensions())
        writer.writeheader()
        writer.writerows(dict_rows)


def test_compound_threshold_expression(compound_redaction_expression_config):

    engine = Engine(config=compound_redaction_expression_config)
    engine.run()
    # TODO: check that expected redaction held the whole time.
    raise NotImplementedError
