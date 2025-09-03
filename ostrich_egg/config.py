"""
Configuration schema
"""

from typing import Any, Union, Annotated, List, Sequence, Optional, Literal

from pydantic import BaseModel, Field, AliasChoices, TypeAdapter
from enum import StrEnum

from ostrich_egg.utils import identifier, get_logger, DEFAULT_MASKING_VALUE

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
            Field(
                description="The masking value to apply to values in the dimension.",
                default=DEFAULT_MASKING_VALUE,
            ),
        ]
    ] = None
    non_summable_dimensions: Optional[
        Annotated[
            Union[List[str], None],
            Field(
                description="List of dimensions that are part of the dataset but will not ever actually be aggregated. For example, if you have unrelated indicators in a column or years when you won't ever sum the total number of incidences across time (preventing revelation through subtraction). Use with caution."
            ),
        ]
    ] = None
    first_order_only: Optional[
        Annotated[
            bool,
            Field(
                description="Whether to only redact cells to prevent latent revelation along a single axis of dimensions. Enabling reduces the total number of cells suppressed but creates a risk of transitive revelation across dimensions.",
                default=False,
            ),
        ]
    ] = False


class ReplaceWithRedacted(Strategy):
    strategy: Literal["replace-with-redacted"] = "replace-with-redacted"
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
    first_order_only: Optional[
        Annotated[
            bool,
            Field(
                description="Whether to only redact cells to prevent latent revelation along a single axis of dimensions. Enabling reduces the total number of cells suppressed but creates a risk of transitive revelation across dimensions.",
                default=False,
            ),
        ]
    ] = False


class MarkRedacted(Strategy):
    """
    Simply flag cells that must be suppressed by virtue of values under threshold or by virtue of requirement through latent revelation.
    """

    strategy: Literal["mark-redacted"] = "mark-redacted"
    parameters: Optional[MarkRedactedParameters] = None


class ReduceDimensionsParameters(BaseModel):
    dimensions: Sequence[str] = Field(
        description="A prioritized sequential list of the dimensions to 'prune' for re-aggregation; the service will attempt to re-aggregate and re-sample in this order. "
    )


class ReduceDimensionsStrategy(Strategy):
    strategy: Literal["reduce-dimensions"]
    parameters: Annotated[
        ReduceDimensionsParameters,
        Field(description="An object containing parameters for the strategy"),
    ]


class MergeDimensionValuesParameters(BaseModel):
    dimension: str = Field(description="The dimension whose values are to be merged")
    values: List[str] = Field(
        description="The ordinal precedence for which values to merge into one another"
    )
    merged_value: Annotated[
        Union[str, None],
        Field(description="The name of the value to merge the values into."),
    ] = None


class MergeDimensionValuesStrategy(Strategy):
    strategy: Literal["merge-dimension-values"]
    parameters: Annotated[
        List[MergeDimensionValuesParameters],
        Field(
            description="The list of dimensions with their values and merged values to apply."
        ),
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

    unit_id_field: str = Field(
        description="The unit_id_field whose distinct count must be suppressed."
    )

    minimum_population_threshold: Annotated[
        int,
        Field(
            description="The minimum population threshold for a dimension value to be considered for aggregation."
        ),
    ] = 20
    round_populations_to_the_nearest: Annotated[
        Literal[1, 5, 10, 20, 50, 100],
        Field(
            description="The nearest multiple to round up non-suppressed populations. If 1, do not change the counts for non-suppressed populations."
        ),
    ] = 10


class FabricateUnitRecordsStrategy(Strategy):
    strategy: Literal["fabricate-unit-records"]
    parameters: Annotated[
        List[FabricateUnitRecordsParameters],
        Field(description="The list of fabrication requirements."),
    ]


class Aggregations(StrEnum):
    ANY_VALUE = "any_value"
    ARRAY_AGG = "array_agg"
    AVG = "avg"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MAX = "max"
    MIN = "min"
    SUM = "sum"


aggregation_values = [e.value for e in Aggregations.__members__.values()]


class Metric(BaseModel):
    aggregation: Annotated[
        Aggregations,
        Field(
            description=f"What aggregation to use for this metric. one of {aggregation_values} "
        ),
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
                description="Alias for this metric to use as a name for reporting it. Not strictly necessary but might make reading the report easier if you have multiple metrics."
            ),
        ]
    ] = None
    null_is_zero: Optional[bool] = Field(
        description="If true, null values will be treated as 0 via coalesce. If false, null values will be discarded in the result of the aggregation.",
        default=False,
    )
    expression: Optional[Union[str, None]] = Field(
        description="A literal expression to use for the metric. Let's a user create advanced custom metrics",
        default=None,
    )
    is_initial: bool = Field(
        description="Whether this metric is only an initial metric against the base dataset to produce the initial aggregation before running latent checks. It is by default false.",
        default=False,
    )
    is_subsequent: bool = Field(
        description="Whether this metric is a subsequent metric that runs after the initial metric. It is by default the opposite of is_initial; a metric could be the same for both, especially if already a a sum or average or any_value aggregation.",
        default_factory=lambda data: not data["is_initial"],
    )

    def render_as_sql_expression(self, include_alias=False):
        if self.expression:
            return self.expression
        alias_expression = ""
        column_identifier = (
            identifier(self.column) if self.column and self.column != "*" else "*"
        )
        if self.null_is_zero and self.column != "*":
            column_identifier = f"coalesce({column_identifier}, 0)"
        if self.alias:
            alias_expression = f" as {identifier(self.alias)}"
        if self.aggregation == Aggregations.COUNT_DISTINCT and self.column:
            column_expression = f"count (distinct {column_identifier})"
        elif self.aggregation == Aggregations.COUNT_DISTINCT:
            logger = get_logger()
            logger.warning(
                "Configured with count distinct but no column specified. Setting to count."
            )
            self.aggregation = Aggregations.COUNT
        if self.aggregation != Aggregations.COUNT_DISTINCT:
            column_expression = f"{self.aggregation}({column_identifier})"
        return f"{column_expression}{alias_expression if include_alias else ''}"

    def should_include_in_initial_state(self, initial: bool = False):
        initial_conditions_match = self.is_initial == initial
        counter_condition_does_not_exclude = not initial and self.is_subsequent
        return initial_conditions_match or counter_condition_does_not_exclude


class Dataset(BaseModel):

    name: Optional[
        Annotated[
            Union[str, None],
            Field(description="A human-friendly way to name this dataset"),
        ]
    ] = None
    dimensions: Annotated[
        Union[List[str], None],
        Field(
            description="A list of dimensions that should are included in the dataset. These are the actual column headers. If null, then all columns excluding the unit-level-id and the metrics will be considered dimensions."
        ),
    ]
    unit_level_id: Annotated[
        Union[str, None],
        Field(
            description="The column name for the unit-level-id, e.g., not-aggregate but identifies a unique record. This will be ignored in calculations except for count(distinct unit-level-id) and the metrics will group by the dimensions.",
            alias=AliasChoices("unit-level-id", "unit_level_id"),
        ),
    ] = None
    initial_metrics: Annotated[
        Optional[Union[List[Metric], None]],
        Field(
            description="An initial set of metrics that will be produced from the first aggregation of the dataset; if not specified and no unit-level-id, will just use count(*). If unit-level-id, will use count(distinct unit-level-id)."
        ),
    ] = None
    metrics: Annotated[
        Union[List[Metric], None],
        Field(
            description="The metrics that will be produced when aggregation is run. If not specified and no unit-level-id, will use count(*). If unit-level-id, will use count(distinct unit-level-id). If only count(*) is specified, a metric will be added to the engine to run subsequently to sum the count metric."
        ),
    ] = None
    sql: Optional[
        Annotated[
            Union[str, None],
            Field(
                description="The internal SQL to produce the 'view' of this dataset",
                default=None,
            ),
        ]
    ] = None
    redaction_order_dimensions: Optional[
        Annotated[
            Union[List[str], None],
            Field(
                default_factory=list,
                description="The dimensions to order the redaction by. If not specified, the dimensions will be ordered by the order they are specified in the dataset.",
            ),
        ]
    ] = None


class DataSource(BaseModel):
    connection_type: Annotated[
        Literal["s3", "file"],
        Field(
            description='The type of connection to use for this data source. Must be one of "s3" "file" or "postgres.'
        ),
    ]
    parameters: Optional[
        Annotated[dict, Field(description="Connection parameters for the data source")]
    ] = None

    @property
    def connection_params(self):
        params = {"connection_type": self.connection_type}
        if self.parameters is not None:
            if isinstance(self.parameters, BaseModel):
                params.update(self.parameters.model_dump())
            elif isinstance(self.parameters, dict):
                params.update(self.parameters)
        return params


SuppressionStrategy = Annotated[
    Union[
        ReduceDimensionsStrategy,
        MergeDimensionValuesStrategy,
        ReplaceWithRedacted,
        MarkRedacted,
    ],
    Field(discriminator="strategy"),
]

load_strategy_from_dict = TypeAdapter(SuppressionStrategy).validate_python


class DatasetConfig(Dataset):
    """Service configuration"""

    source_file: Optional[
        Annotated[
            Union[str, None],
            Field(
                description="At present this is called a file for simplicity. It's actually likely a key for s3 or a fully qualified table name for postgres. If not specified, the source is in the datasource.",
                default=None,
            ),
        ]
    ] = None
    suppression_strategies: Annotated[
        # List[SuppressionStrategy], discriminator='strategy',
        Optional[
            Union[
                List[SuppressionStrategy],
                list,
                None,
            ]
        ],
        Field(
            description="List of suppression strategies",
            alias=AliasChoices("suppression-strategies", "suppression_strategies"),
            default_factory=list,
        ),
    ]
    output_file: Optional[
        Annotated[
            Union[str, None],
            Field(
                description="The filepath you want want to write out to. In the future, this can be more of an object that can configure things like table, s3 location, etc, but for Proof-of-concept just writing to file (which is really an s3 key)"
            ),
        ]
    ] = None
    extra: Optional[
        Annotated[
            Union[dict, None],
            Field(
                description="Dataset-level configurations for your application. For example, if you need to map the results of intermediary dimensionality."
            ),
        ]
    ] = None


class Config(BaseModel):
    """The actual configuration for the service will typically take the shape of a single datasource and multiple datasets.
    Items can callback to variables or anchors.
    The outlet is assumed to be always the same type for now, we can complicate later.
    """

    datasource: DataSource

    allow_zeroes: bool = Field(
        description="Whether a 0 value counts as below the threshold for evaluating anonymity. By default, this is true, meaning a 0 is considered anonymous. When false, 0 is considered a small number that is masked.",
        default=True,
    )
    redaction_expression: Optional[str] = Field(
        description="This expression is used in the aggregation queries to determine if a cell should be redacted. If not specified, then the first metric will be evaluated at the default threshold of 11.",
        default=None,
    )
    datasets: List[DatasetConfig]

    threshold: Optional[int] = Field(
        description="[DEPRECATED]: Single value for a threshold, being replaced by an expression.",
        default=DEFAULT_THRESHOLD,
    )
