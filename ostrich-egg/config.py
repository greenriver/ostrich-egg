"""
Configuration schema
"""

import os
import sys

DIR = os.path.dirname(__file__)
SRC_DIR = os.path.join(DIR, 'src')
sys.path.extend([DIR, SRC_DIR])
from typing import Any, Union, Annotated, List, Sequence, Optional, Literal

from pydantic import BaseModel, Field
from enum import StrEnum

from utils import identifier, get_logger, DEFAULT_MASKING_VALUE

DEFAULT_THRESHOLD = 11


class Strategy(BaseModel):
    strategy: str
    parameters: Any


class ReplaceWithRedactedParameters(BaseModel):
    redacted_dimension: Annotated[
        str,
        Field(
            description="The dimension to check for redaction. The output's `is_redacted` flag will apply to this dimension. One iteration will only produce cell-level suppression and applies only to this dimension. You must iterate for other dimensions to achieve full dataset anonymization."
        ),
    ]
    dimensions: Sequence[str] = Field(
        description="A prioritized sequential list of the dimensions to mark small values as `redacted` (or other masking value)"
    )
    masking_value: Optional[
        Annotated[
            Union[str, None],
            Field(description='The masking value to apply to values in the dimension.', default=DEFAULT_MASKING_VALUE),
        ]
    ] = None


class ReplaceWithRedacted(Strategy):
    strategy: Literal['replace-with-redacted'] = 'replace-with-redacted'
    parameters: ReplaceWithRedactedParameters


class MarkRedactedParameters(BaseModel):
    redacted_dimension: Annotated[
        str,
        Field(
            description="The dimension to check for redaction. The output's `is_redacted` flag will apply to this dimension. One iteration will only produce cell-level suppression and applies only to this dimension. You must iterate for other dimensions to achieve full dataset anonymization."
        ),
    ]
    non_summable_dimensions: Optional[
        Annotated[
            Union[List[str], None],
            Field(
                description="List of dimensions that are part of the dataset but will not ever actually be aggregated. For example, if you have unrelated indicators in a column or years when you won't ever sum the total number of incidences across time (preventing revelation through subtraction). Use with caution."
            ),
        ]
    ] = None


class MarkRedacted(Strategy):
    """
    Simply flag cells that must be suppressed by virtue of values under threshold or by virtue of requirement through latent revelation.
    """

    strategy: Literal['mark-redacted'] = 'mark-redacted'
    parameters: Optional[MarkRedactedParameters] = None


class ReduceDimensionsParameters(BaseModel):
    dimensions: Sequence[str] = Field(
        description="A prioritized sequential list of the dimensions to 'prune' for re-aggregation; the service will attempt to re-aggregate and re-sample in this order. "
    )


class ReduceDimensionsStrategy(Strategy):
    strategy: Literal['reduce-dimensions']
    parameters: Annotated[
        ReduceDimensionsParameters, Field(description="An object containing parameters for the strategy")
    ]


class MergeDimensionValuesParameters(BaseModel):
    dimension: str = Field(description='The dimension whose values are to be merged')
    values: List[str] = Field(description='The ordinal precedence for which values to merge into one another')
    merged_value: Annotated[Union[str, None], Field(description='The name of the value to merge the values into.')] = (
        None
    )


class MergeDimensionValuesStrategy(Strategy):
    strategy: Literal['merge-dimension-values']
    parameters: Annotated[
        List[MergeDimensionValuesParameters],
        Field(description='The list of dimensions with their values and merged values to apply.'),
    ]


class FabricateUnitRecordsParameters(BaseModel):
    """
    How to insert "noisy" records into the output dataset to increase minimum population values.

    This strategy will not know about real populations, so it might make a fabrication wherein if a knowing person
        knew that the combinations of dimensions selected by some _other_ definition meant only 1 dimension value was possible,
        someone could infer presence of value though not count.

        Use in tandem with merge strategies to collapse or redact known small populations in advance of deploying this strategy so that it will fabricate obfuscated populations
        and not merely obscure a knowable population.
    """

    unit_id_field: str = Field(description='The unit_id_field whose distinct count must be suppressed.')

    minimum_population_threshold: Annotated[
        int,
        Field(description='The minimum population threshold for a dimension value to be considered for aggregation.'),
    ] = 20
    round_populations_to_the_nearest: Annotated[
        Literal[1, 5, 10, 20, 50, 100],
        Field(
            description='The nearest multiple to round up non-suppressed populations. If 1, do not change the counts for non-suppressed populations.'
        ),
    ] = 10


class FabricateUnitRecordsStrategy(Strategy):
    strategy: Literal['fabricate-unit-records']
    parameters: Annotated[
        List[FabricateUnitRecordsParameters],
        Field(description='The list of fabrication requirements.'),
    ]


class Aggregations(StrEnum):
    SUM = 'sum'
    AVG = 'avg'
    COUNT = 'count'
    COUNT_DISTINCT = 'count_distinct'


aggregation_values = [e.value for e in Aggregations.__members__.values()]


class Metric(BaseModel):
    aggregation: Annotated[
        Aggregations, Field(description=f'What aggregation to use for this metric. one of {aggregation_values} ')
    ]
    column: Annotated[
        str,
        Field(
            description="""The column name this metric wraps around. if pre-aggregated, just use sum \
                (assuming you have correctly flagged the dimensions to aggregate). Use star for COUNT """
        ),
    ]
    alias: Optional[
        Annotated[
            Union[str, None],
            Field(
                description='Alias for this metric to use as a name for reporting it. Not strictly necessary but might make reading the report easier if you have multiple metrics.'
            ),
        ]
    ] = None

    def render_as_sql_expression(self, include_alias=False):
        alias_expression = ""
        if self.alias:
            alias_expression = f' as {identifier(self.alias)}'
        if self.aggregation == Aggregations.COUNT_DISTINCT and self.column:
            column_expression = f"count (distinct {self.column})"
        elif self.aggregation == Aggregations.COUNT_DISTINCT:
            logger = get_logger()
            logger.warning("Configured with count distinct but no column specified. Setting to count.")
            self.aggregation = Aggregations.COUNT
        if self.aggregation != Aggregations.COUNT_DISTINCT:
            column_expression = f"{self.aggregation}({identifier(self.column) or "*"})"
        return f"{column_expression}{alias_expression if include_alias else ''}"


class Dataset(BaseModel):

    name: Optional[
        Annotated[
            Union[str, None],
            Field(description='A human-friendly way to name this dataset'),
        ]
    ] = None
    dimensions: Annotated[
        Union[List[str], None],
        Field(
            description='A list of dimensions that should are included in the dataset. These are the actual column headers. If null, then all columns excluding the unit-level-id and the metrics will be considered dimensions.'
        ),
    ]
    unit_level_id: Annotated[
        Union[str, None],
        Field(
            description='The column name for the unit-level-id, e.g., not-aggregate but identifies a unique record. This will be ignored in calculations except for count(distinct unit-level-id) and the metrics will group by the dimensions.',
            alias='unit-level-id',
        ),
    ] = None
    metrics: Annotated[
        Union[List[Metric], None],
        Field(
            description='The metrics that will be aggregated; if not specified and no unit-level-id, will just use count(*). If unit-level-id, will use count(distinct unit-level-id).'
        ),
    ] = None


class DataSource(BaseModel):
    connection_type: Annotated[
        Literal['s3', 'file'],
        Field(
            description='The type of connection to use for this data source. Must be one of "s3" "file" or "postgres.'
        ),
    ]
    parameters: Optional[Annotated[dict, Field(description="Connection parameters for the data source")]] = None

    @property
    def connection_params(self):
        params = {"connection_type": self.connection_type}
        if self.parameters is not None:
            if isinstance(self.parameters, BaseModel):
                params.update(self.parameters.model_dump())
            elif isinstance(self.parameters, dict):
                params.update(self.parameters)
        return params


class DatasetConfig(Dataset):
    """Service configuration"""

    source_file: Optional[
        Annotated[
            Union[str, None],
            Field(
                description="At present this is called a file for simplicity. It's actually likely a key for s3 or a fully qualified table name for postgres. If not specified, the source is in the datasource."
            ),
        ]
    ] = None
    suppression_strategies: Annotated[
        # List[SuppressionStrategy], discriminator='strategy',
        Union[
            List[Union[ReduceDimensionsStrategy, MergeDimensionValuesStrategy, ReplaceWithRedacted, MarkRedacted]], list
        ],
        Field(description='List of suppression strategies', alias='suppression-strategies', default_factory=list),
    ]
    output_file: Optional[
        Annotated[
            Union[str, None],
            Field(
                description='The filepath you want want to write out to. In the future, this can be more of an object that can configure things like table, s3 location, etc, but for Proof-of-concept just writing to file (which is really an s3 key)'
            ),
        ]
    ] = None
    extra: Optional[
        Annotated[
            Union[dict, None],
            Field(
                description='Dataset-level configurations for your application. For example, if you need to map the results of intermediary dimensionality.'
            ),
        ]
    ] = None


class Config(BaseModel):
    """The actual configuration for the service will typically take the shape of a single datasource and multiple datasets.
    Items can callback to variables or anchors.
    The outlet is assumed to be always the same type for now, we can complicate later.
    """

    datasource: DataSource
    threshold: int = Field(
        description=f"The inclusive lower boundary of allowable values. By default, it is {DEFAULT_THRESHOLD}, meaning any value less than this threshold should be anonymized according to the provided strategy",
        default=DEFAULT_THRESHOLD,
    )
    allow_zeroes: bool = Field(
        description="Whether a 0 value counts as below the threshold for evaluating anonymity. By default, this is true, meaning a 0 is considered anonymous. When false, 0 is considered a small number that is masked.",
        default=True,
    )
    datasets: List[DatasetConfig]
