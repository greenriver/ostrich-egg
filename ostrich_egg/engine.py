"""
The actual "workhorse" part of the anonymization service. This is where the data gets loaded and processed.
"""

from __future__ import annotations
from collections import namedtuple
from copy import deepcopy
from itertools import combinations
import os
from typing import Dict, List

import duckdb
from jinja2 import Template

from ostrich_egg.config import (
    Aggregations,
    Config,
    DatasetConfig,
    DEFAULT_THRESHOLD,
    load_strategy_from_dict,
    MarkRedactedParameters,
    MergeDimensionValuesParameters,
    Metric,
    ReplaceWithRedactedParameters,
    Strategy,
)
from ostrich_egg.connectors import Connector, DEFAULT_TABLE_NAME
from ostrich_egg.connectors.s3 import key_as_s3_uri
from ostrich_egg.utils import (
    DEFAULT_MASKING_VALUE,
    dict_to_filter_expressions,
    get_logger,
    identifier,
    make_when_statement_from_dict,
    merge_conditions,
    ostrich_egg_jinja_env,
)


DEFAULT_METRIC = "count(*)"
DEFAULT_RESULT_NAME = "result"

IS_ANONYMOUS_COLUMN = "is_anonymous"
IS_REDACTED_COLUMN = "is_redacted"
DIMENSION_VALUE_COLUMN = "dimension_value"
METRIC_VALUE_COLUMN = "metric_value"

logger = get_logger()

SqlExpressionObject = namedtuple(
    typename="SqlExpressionObject", field_names=["select", "group_by", "aliases"]
)

RedactionIterationResult = namedtuple(
    typename="RedactionIterationResult",
    field_names=["other_dimension_values", "remapped_lookup", "reason"],
)


class Engine:
    """
    The powerhouse behind the anonymization service that processes configurations.
    Can produce reports of anonymization (proposals or summary of implementation) and implement anonymization according to configurations.
    """

    def __init__(
        self,
        config: Config,
        output_directory: str = None,
        output_prefix: str = None,
        output_bucket: str = None,
        cache_tables_in_memory: bool = False,
    ):
        self.config = config
        self.threshold = config.threshold
        self.allow_zeros = config.allow_zeroes
        self.datasets = config.datasets
        # leaving these as mutable for the moment; they're derived properties from the active dataset.
        self.active_dimensions = []
        self.removed_dimensions = []
        self.source_file_to_table_lkp = {}
        self.active_dataset = config.datasets[0]
        # TODO: Connectors shouldn't need a dataset name to init...
        self.connector = Connector(
            table_name=self.active_dataset.name, **config.datasource.connection_params
        )

        self.redactions: Dict[str, List[RedactionIterationResult]] = {}
        self.final_source_table = self.connector.table_name
        self.output_directory = (
            output_directory
            or config.datasource.connection_params.get("output_directory")
        )
        self.output_prefix = output_prefix
        self.output_bucket = output_bucket
        self.cache_tables_in_memory = cache_tables_in_memory

    @property
    def active_dataset(self):
        return self.__active_dataset

    @active_dataset.setter
    def active_dataset(self, dataset: DatasetConfig | None):
        self.__active_dataset = dataset
        self.metrics = dataset.metrics
        self.active_dimensions = dataset.dimensions
        self.removed_dimensions = []

    @property
    def metrics(self):
        return self.__metrics

    @metrics.setter
    def metrics(self, metrics: List[Metric] | None):
        """
        If no metrics were specified, apply the default logic (count(*)) or distinct unit_level_id.
        In all cases, ensure each metric has an alias.
        """
        if not metrics:
            # use default!

            aggregation = Aggregations.COUNT
            column = "*"
            if self.active_dataset.unit_level_id:
                aggregation = Aggregations.COUNT_DISTINCT
                column = self.active_dataset.unit_level_id
            metrics = [Metric(aggregation=aggregation, column=column, is_initial=True)]
        for index, metric in enumerate(metrics):
            metric.alias = metric.alias or f"m_{index}"
        if len(metrics) == 1 and metrics[0].is_initial:
            metrics.append(
                Metric(
                    aggregation=Aggregations.SUM,
                    column=metrics[0].alias,
                    is_initial=False,
                    alias=metrics[0].alias,
                )
            )
        initial_metrics = [metric for metric in metrics if metric.is_initial]
        subsequent_metrics = [metric for metric in metrics if not metric.is_initial]

        if not initial_metrics:
            logger.warning(
                "No initial metrics were specified, using subsequent metrics as initial metrics."
            )
            for metric in subsequent_metrics:
                metric.is_initial = True
        if not subsequent_metrics:
            logger.warning(
                "No subsequent metrics were specified, using initial metrics as subsequent metrics."
            )
            for metric in initial_metrics:
                metric.is_subsequent = True
        self.__metrics = initial_metrics + subsequent_metrics

    def get_metric_aliases(self, initial: bool = False) -> dict:
        """
        Return column-name: metric-name mapping.
        """
        metric_aliases = {
            metric.alias: metric.render_as_sql_expression()
            for metric in self.metrics
            if metric.should_include_in_initial_state(initial=initial)
        }
        return metric_aliases

    def get_metric_sql_list(self, initial: bool = False) -> list:
        metric_sql_list = [
            f"{metric} as {identifier(alias)}"
            for alias, metric in self.get_metric_aliases(initial=initial).items()
        ]
        return metric_sql_list

    @property
    def redaction_expression(self) -> str:
        redaction_expression = self.config.redaction_expression
        if not redaction_expression:
            redaction_expression = f"{self.metrics[0].alias} < {DEFAULT_THRESHOLD}"
        return redaction_expression

    @property
    def db(self) -> duckdb.DuckDBPyConnection:
        return self.connector.duckdb_connection

    @property
    def anonymous_expression(self) -> str:
        """
        Users configure the redaction expression, i.e., when to mark something as _not_ anymous.
        This is the inverse.

        In the output, is_redacted flags both non-anonymous _and_ latent suppression.
        """
        return f"not {self.redaction_expression}"

    def make_updated_expressions(self) -> dict:
        """
        After collecting redactions, this is the updated select statement expressions with redacted values as case statements.
        """
        updated_expressions = {}

        for dimension, redactions in self.redactions.items():
            whens = []
            for redaction in redactions:
                whens.extend(
                    self.make_when_statements_from_redaction(
                        dimension=dimension, redaction=redaction
                    )
                )
            updated_expressions[
                dimension
            ] = f''' \
            case
                {'\n'.join(whens)}
            else {identifier(dimension)}
            end
            '''
        return updated_expressions

    def dimensions_as_sql_expressions(self, dimensions: list) -> SqlExpressionObject:
        """
        The list of dimensions you want to select + group by.
        If you are iterating, you might have accumulated some expressions to update the dimensions (such as with CASE replacements from dimension_with_replacements )
        The keys for updated_expressions should be dimensions to updated with.
        Expressions not in the dimension list will be ignored.
        """
        updated_expressions = self.make_updated_expressions()
        sql_object = SqlExpressionObject(
            select=", ".join(
                [
                    f"{updated_expressions.get(dimension, identifier(dimension))} as {identifier(dimension)}"
                    for dimension in dimensions
                ]
            ),
            group_by=", ".join(
                [
                    f"{updated_expressions.get(dimension, identifier(dimension))}"
                    for dimension in dimensions
                ]
            ),
            aliases=", ".join([identifier(dimension) for dimension in dimensions]),
        )
        return sql_object

    @staticmethod
    def dimension_with_replacements(dimension: str, replacements: dict) -> str:
        """
        Make a case statement for a given dimension with replacements, e.g.,
        after latent checks. This allows us to re-render the aggregation without actually updating the underlying dataset
        ( so we can run multiple iterations without having to re-load the original dataset.)
        """
        dimension_replacements = "\n ".join(
            [f"when '{k}' then '{v}'" for k, v in replacements.items()]
        )
        case_statement = f"""
        case "{dimension}"
            {dimension_replacements}
            else "{dimension}"
        end
        """
        return case_statement

    @staticmethod
    def make_when_statements_from_redaction(
        redaction: RedactionIterationResult, dimension: str
    ) -> List[str]:
        whens = []
        for old, new in redaction.remapped_lookup.items():
            redaction.other_dimension_values[dimension] = old
            whens.append(
                make_when_statement_from_dict(
                    data=redaction.other_dimension_values, value=new
                )
            )
        return whens

    def get_wrapper_metrics_pass_expression_from_redaction_expression(
        self, initial: bool = False
    ) -> str:
        """
        Construct the expression that determines if the row itself passes privacy thresholds
        It evaluates the redaction expression and returns a single boolean for whether the row in question passes.

        """
        check_list_template = """\
        {% for metric in metrics %}
            ({{m}} >= {{threshold}}) {% if allow_zeroes is true %} or ({{m}} = 0 ){%endif%} {% if not loop.last%},{%endif%}
        {%- endfor %}
        """
        context = {
            "metric_aliases": self.get_metric_aliases(initial=initial),
            "allow_zeroes": self.allow_zeros,
            "threshold": self.threshold,
        }
        check_list = Template(check_list_template).render(context)
        check_for_did_not_pass = f"not list_contains([{check_list}], false)"
        return check_for_did_not_pass

    def get_rendered_aggregation_query(
        self, dimensions: list = None, table_name: str = None, initial: bool = False
    ) -> str:
        dimensions = dimensions or self.active_dimensions
        dimensions_as_sql_object = self.dimensions_as_sql_expressions(
            dimensions=dimensions
        )
        context = {
            "metric_list": self.get_metric_sql_list(initial=initial),
            "dimensions": dimensions_as_sql_object,
            "table_name": table_name or self.connector.table_name,
            # "wrapper_list_check": self.get_wrapper_metrics_pass_expression_from_redaction_expression(),
            "anonymous_expression": self.anonymous_expression,
            "is_anonymous": IS_ANONYMOUS_COLUMN,
        }

        sql_template = """\
        select *, {{anonymous_expression}} as {{is_anonymous}}
        from (
            select {{metric_list|join(', ')}}, {{ dimensions.select }}
            from "{{ table_name }}"
            group by {{ dimensions.group_by }}
        ) as aggregated
        """
        sql = Template(sql_template).render(context)
        return sql

    def run_aggregation(
        self,
        dimensions: list = None,
        result_name=DEFAULT_RESULT_NAME,
        table_name: str = None,
        initial: bool = False,
    ):
        """
        Run aggregations according to the dataset definition.
        This will be called iteratively, initially with the full set of dimensions, then potentially pruned.
        """
        dimensions = dimensions or self.active_dimensions
        sql = self.get_rendered_aggregation_query(
            dimensions=dimensions, table_name=table_name, initial=initial
        )
        __result__ = self.connector.duckdb_connection.sql(sql)
        if initial:
            self.connector.duckdb_connection.execute(
                f"create or replace table {result_name} as select * from __result__"
            )
        else:
            self.connector.duckdb_connection.register(result_name, __result__)

    def drop_dimension(self, dimension: str):
        if dimension not in self.active_dimensions:
            raise ValueError(
                f"Cannot remove dimension {dimension} as it is not presently in active dimensions."
            )
        self.removed_dimensions.append(self.active_dimensions.pop(dimension))

    def get_dimension_values_to_redact_with_latency_check(
        self,
        dimension: str,
        masking_value=DEFAULT_MASKING_VALUE,
    ) -> List[RedactionIterationResult]:
        """
        The goal here is to flag which rows based on dimension value
        need to be marked `redacted`; it will of course by the small value, but it also needs to be
        adjacent values until revelation by subtraction (or latent revelation) is not possible.

        For example, if the dimension we're checking is `race` and there are 4 values:
            * white: 100
            * black: 50
            * asian: 20
            * native_am: 10

        then we'd need to set `native_am` to `Redacted` but also `asian` to `Redacted` when strategy is `redact`

        If strategy is `merge`, construct a new value that is alpha-sort the redacted values, in this case `asian and native_am`.
        The merge strategy without constraint runs the risk of revelation through version iteration; if someone archived the results of this dataset in the past and the
        merging is dynamic, then theoretically the difference in versions could reveal a historic data point.

        It would be required to ensure that the same merged value was used in subsequent iterations so that it can only become more obfuscating and not allow for finer-detailed separation by subtraction.
        In this example, it would be necessary for the subsequent iterations of this dataset to not include `asian` and `native_am` but the merged `asian and native_am` value.
        """
        masking_value = masking_value or DEFAULT_MASKING_VALUE
        final_result = []

        dimensions_to_hold_constant = [
            d for d in self.active_dimensions if d != dimension
        ]

        sql_expressions = self.dimensions_as_sql_expressions(
            dimensions=dimensions_to_hold_constant
        )
        metrics_sql = ", ".join(self.get_metric_sql_list(initial=False))
        metric_sort = ", ".join(self.get_metric_aliases(initial=False).keys())
        anonymous_metric = "count(*) filter (where not is_anonymous) = 0"
        dimension_value_count_sql = f"""
        select dense_rank() over(order by {sql_expressions.aliases}) as peer_id , *
        from (
            select {sql_expressions.select}, {identifier(dimension)} as dimension_value, {metrics_sql}, {anonymous_metric} as is_anonymous
            from result
            group by {sql_expressions.group_by}, {identifier(dimension)}
            ) as re_agg /* in case we have already dropped columns, we need to reaggregate. */
            order by peer_id, {metric_sort}
        """
        identify_peers = self.connector.duckdb_connection.sql(dimension_value_count_sql)
        self.connector.duckdb_connection.register("identify_peers", identify_peers)

        # get smallest cells first.
        # unless the user wants to prioritize say, time and/or larger geographic units or other semantically useful sort conditions.
        order_peers_by = f"{IS_ANONYMOUS_COLUMN}, {metric_sort}, peer_id"
        if self.active_dataset.redaction_order_dimensions:
            order_by_dimensions = ", ".join(
                [
                    d
                    for d in self.active_dataset.redaction_order_dimensions
                    if d in dimensions_to_hold_constant
                ]
            )
            order_peers_by = f"{order_by_dimensions}, {order_peers_by}"
        peer_ids = [
            x[0]
            for x in self.connector.duckdb_connection.sql(
                f"""
                select distinct peer_id
                from identify_peers
                order by {order_peers_by}
                """
            ).fetchall()
        ]
        # this variable goes across peers
        # it will be set to True if a small cell needs a neighbor to be suppressed.
        must_anonymize_next = False
        reason = None
        for peer_id in peer_ids:
            # identify the peers within this aggregation that might need to be latently redacted
            peer_value_sql = f"""
                select x as peer
                from (
                    select {sql_expressions.aliases}
                    from identify_peers
                    where peer_id = $peer_id
                    group by {sql_expressions.aliases}
                ) as x
            """
            peer_values, *_ = self.connector.duckdb_connection.sql(
                query=peer_value_sql, params={"peer_id": peer_id}
            ).fetchone()

            filter_by_peer = duckdb.ColumnExpression("peer_id").isin(peer_id)
            # within a peer group, identify the smallest cells first.
            this_peer_relation = (
                identify_peers.filter(filter_by_peer)
                .select(
                    DIMENSION_VALUE_COLUMN,
                    IS_ANONYMOUS_COLUMN,
                    *self.get_metric_aliases(initial=False).keys(),
                )
                .order(
                    f"{IS_ANONYMOUS_COLUMN}, {metric_sort}, dimension_value nulls last"
                )
            )
            peer_result, must_anonymize_next = (
                self._collect_redactions_from_peer_relation(
                    this_peer_relation=this_peer_relation,
                    peer_values=peer_values,
                    masking_value=masking_value,
                    must_anonymize_next=must_anonymize_next,
                    reason=reason,
                )
            )
            if must_anonymize_next:
                reason = peer_result[-1].reason
            else:
                reason = None
            final_result.extend(peer_result)

        return final_result

    def _collect_redactions_from_peer_relation(
        self,
        this_peer_relation: duckdb.DuckDBPyRelation,
        peer_values: dict,
        must_anonymize_next: bool = False,
        masking_value: str = DEFAULT_MASKING_VALUE,
        reason: str | None = None,
    ) -> tuple[list[RedactionIterationResult], bool]:
        """
        Main engine for latent anonymization

        Assumes `this_peer_relation` has 2 values: dimension_value and metric_value
        and is sorted ascending by metric_value.

        Returns the redactions for this group and whether we need to anonymize the next batch.
        If it's the last batch and we need anonymizing we've just redacted everything.
        """
        values_to_mask = []
        seen_values = []

        row_count, *_ = this_peer_relation.count("*").fetchone()
        within_peer_index = 0
        values_meeting_redaction_criteria = []
        while within_peer_index < row_count:
            local_result = this_peer_relation.fetchone()
            if not local_result:
                break
            local_result = dict(zip(this_peer_relation.columns, local_result))
            value_is_fine = local_result[IS_ANONYMOUS_COLUMN]
            dimension_value = local_result[DIMENSION_VALUE_COLUMN]
            seen_values.append(dimension_value)

            if within_peer_index > 0:
                working_subtotal_query_template = Template(
                    """\
        select x
        from (
            select *, {{anonymous_expression}} as {{is_anonymous}}
            from (
                select {{metric_list|join(', ')}}
                from identify_peers
                where list_contains($seen_values, dimension_value)
            ) as aggregated
        ) as x
        """
                )
                sql = working_subtotal_query_template.render(
                    metric_list=self.get_metric_sql_list(initial=False),
                    anonymous_expression=self.anonymous_expression,
                    is_anonymous=IS_ANONYMOUS_COLUMN,
                )
                working_subtotal, *_ = self.connector.duckdb_connection.sql(
                    sql, params={"seen_values": seen_values}
                ).fetchone()
                working_total_is_fine = working_subtotal[IS_ANONYMOUS_COLUMN]
            else:
                working_total_is_fine = value_is_fine

            first_value_is_good = within_peer_index == 0 and value_is_fine

            sufficient_prior_redaction = (
                len(values_to_mask) >= 2 and working_total_is_fine
            )

            if sufficient_prior_redaction:
                must_anonymize_next = False

            if not must_anonymize_next and first_value_is_good:
                """
                The first value in the peer group is already anonymized and we don't need to anonymize from a previous peer group, this is anonymous already.
                """
                break

            elif not value_is_fine:
                """
                Base case small-cell for suppression.
                """

                logger.debug(
                    f"{dimension_value} meets redaction criteria\n {self.redaction_expression}"
                )
                values_meeting_redaction_criteria.append(dimension_value)
                values_to_mask.append(dimension_value)
                must_anonymize_next = True

            elif must_anonymize_next and value_is_fine:
                """
                Have to anonymize based on a previous redaction.
                """
                values_to_mask.append(dimension_value)
                must_anonymize_next = False
                break

            elif not sufficient_prior_redaction or must_anonymize_next:
                """
                This is the base case for latent suppression in which the value itself is fine but we must suppress to prevent latent revelation.
                We will check the next iteration if the redaction leaves us with exclusively sufficiently large cells and no additional revelation through subtraction.
                """
                logger.debug(
                    f"Redacting {dimension_value} as it would latently reveal an unacceptable metric value through subtraction."
                )
                values_to_mask.append(dimension_value)

                # If this is the last value in the peer group and we are _now_ anonymized, we do not need to anonymize the next peer group.
                # else we do not yet have sufficient prior redaction to prevent latent revelation, and we must suppress another cell.
                if len(values_to_mask) >= 2 and (
                    working_total_is_fine or value_is_fine
                ):
                    must_anonymize_next = False

            elif sufficient_prior_redaction and value_is_fine:
                """
                This is an exit condition from when redaction is required; there is enough small cell suppression
                and the values in this peer group from this value forward are all fine ( it was sorted by value ascending).
                """
                must_anonymize_next = False
                break

            within_peer_index += 1
        if values_meeting_redaction_criteria:
            value_string = ", ".join(
                [
                    "<null>" if value is None else str(value)
                    for value in values_meeting_redaction_criteria
                ]
            )

            reason = f"value{'s' if len(values_meeting_redaction_criteria) > 1 else ''} {value_string} meet{'s' if len(values_meeting_redaction_criteria) == 1 else ''} redaction criteria\n {self.redaction_expression}"
        peer_result = (
            [
                RedactionIterationResult(
                    other_dimension_values=peer_values,
                    remapped_lookup={
                        dimension_value: masking_value
                        for dimension_value in values_to_mask
                    },
                    reason=reason,
                )
            ]
            if values_to_mask
            else []
        )
        return peer_result, must_anonymize_next

    def redact_from_non_anonymous_cells(
        self,
        dimension: str,
        masking_value=DEFAULT_MASKING_VALUE,
        non_summable_dimensions: list[str] = [],
        first_order_only: bool = False,
    ):
        """
        Iteratively suppress adjacent cells according to dataset/suppression strategy configuration.

        The `dimension` is the intended target of suppression; this is most-relevant in upstream processes
        in which you need to redact across several dimensions.

        The strategy is to find non-anonymous cells (those that met the redaction expression criteria) and sort the dataset
        in a window partitioned by each combination of dimensions and ordering by the redacted dimension within the window (deferring to other sorting configurations first).

        We then iteratively suppress the output until the conditions for anonymity are met by virtue of flagging the cells to redact.
        """
        masking_value = masking_value or DEFAULT_MASKING_VALUE

        dimension_sets_to_check = sorted(
            [
                dimension_set
                for i in range(len(self.active_dimensions))
                for dimension_set in combinations(self.active_dimensions, i + 1)
            ],
            key=lambda x: len(x),
            reverse=True,
        )
        order_by_columns = [
            "is_redacted desc",
            identifier(dimension),
            identifier(self.active_dataset.metrics[0].alias),
        ]
        if self.active_dataset.redaction_order_dimensions:
            order_by_columns = list(
                set(
                    [
                        identifier(d)
                        for d in self.active_dataset.redaction_order_dimensions
                    ]
                    + order_by_columns
                )
            )

        redaction_context_view_template = ostrich_egg_jinja_env.get_template(
            "redaction_context_view.sql"
        )

        check_redacted_context_template = ostrich_egg_jinja_env.get_template(
            "check_redacted_context.sql"
        )

        check_redacted_context_sql = check_redacted_context_template.render(
            non_summable_dimensions=non_summable_dimensions,
            threshold=self.threshold,
            incidence_column=self.metrics[0].alias,
            first_order_only=first_order_only,
        )

        update_output_from_redaction_context_template = (
            ostrich_egg_jinja_env.get_template(
                "update_output_from_redaction_context.sql"
            )
        )

        update_output_from_redaction_context_sql = (
            update_output_from_redaction_context_template.render(
                dimensions=self.active_dimensions,
                output_table="output",
            )
        )

        for dimension_set in dimension_sets_to_check:
            redaction_context_sql = redaction_context_view_template.render(
                dimension_set=dimension_set,
                order_by_columns=order_by_columns,
                dimension=dimension,
                non_summable_dimensions=non_summable_dimensions,
                output_table="output",
                incidence_column=self.metrics[0].alias,
            )
            logger.info(f"Creating redaction context view for {dimension_set}")
            logger.debug(redaction_context_sql)
            self.db.execute(
                f"create or replace table redaction_context as\n\n {redaction_context_sql}"
            )

            logger.info(f"Looking for records to redact")
            logger.debug(check_redacted_context_sql)

            to_redact = self.db.sql(check_redacted_context_sql)
            self.db.register("to_redact", to_redact)
            to_redact_count = to_redact.count("*").fetchone()[0]
            while to_redact_count > 0:
                logger.info(f"Found {to_redact_count} records to redact")
                logger.info(to_redact.to_df().to_json(orient="records", indent=2))
                self.db.execute(update_output_from_redaction_context_sql)
                self.db.execute(
                    f"create or replace table redaction_context as\n\n {redaction_context_sql}"
                )
                to_redact = self.db.sql(check_redacted_context_sql)
                self.db.register("to_redact", to_redact)
                to_redact_count = to_redact.count("*").fetchone()[0]

    def update_the_dataset(
        self, dimension, existing_values: list, new_value: str = "Redacted"
    ):
        sql = f"""
        update {self.connector.table_name}
        set "{dimension}" = $new_value
        where list_contains($existing_values, "{dimension}" )
        """
        self.connector.duckdb_connection.execute(
            sql, parameters={"new_value": new_value, "existing_values": existing_values}
        )

    def merge_dimension_values(self, parameters: List[MergeDimensionValuesParameters]):
        # TODO:
        raise NotImplementedError(
            "This actually needs more thoughtful implementation, but so-far irrelevant to proof of concept. "
        )
        redactions: List[RedactionIterationResult] = []
        for param in parameters:
            redactions.extend(
                self.get_dimension_values_to_redact_with_latency_check(
                    dimension=param.dimension,
                    masking_value=param.merged_value,
                    metric_name=self.metrics[0].alias,
                )
            )

    def modify_output_for_redaction(self):
        self.connector.duckdb_connection.execute(
            "create or replace table output as select * from result"
        )
        alter_sql = """
        alter table output
          add column "is_redacted" boolean default false;
        alter table output
          add column "peer_group" json;
        alter table output
          add column "redacted_peers" json;
        alter table output
          add column "redaction_reason" text;
        """
        self.connector.duckdb_connection.execute(alter_sql)
        self.connector.duckdb_connection.execute(
            f"""\
                update output
                set is_redacted = true
                , redaction_reason = $$value meets redaction criteria '{self.redaction_expression}'$$
                where not is_anonymous
            """
        )

    def replace_with_redacted(self, params: ReplaceWithRedactedParameters):
        """
        Sets the update expression to be used in the final result by assigning redaction
        values to dimensions.
        """
        # TODO: add in the non-summable dimensions?
        self.modify_output_for_redaction()
        redactions = self.get_dimension_values_to_redact_with_latency_check(
            dimension=params.redacted_dimension,
            masking_value=params.masking_value,
        )

        self.redactions[params.redacted_dimension] = self.redactions.get(
            params.redacted_dimension, []
        )
        self.redactions[params.redacted_dimension].extend(redactions)
        self.run_aggregation()
        self.connector.duckdb_connection.execute(
            "create or replace table output as select * from result"
        )

    def mark_redacted(self, params: MarkRedactedParameters):
        """
        Adds a column `is_redacted` which is either true (for the value appears in the redactions) or false if the column is usable.
        This is useful for when you need the source values for your process but need to indicate which cells
        must be suppressed.
        """
        self.modify_output_for_redaction()
        self.redact_from_non_anonymous_cells(
            dimension=params.redacted_dimension,
            non_summable_dimensions=params.non_summable_dimensions or [],
            first_order_only=params.first_order_only,
        )
        # Use this new table instead of the source data.
        self.final_source_table = "output"

    def process_one_suppression_strategy(self, strategy: Strategy):
        if strategy.strategy == "merge-dimension-values":
            self.merge_dimension_values(strategy.parameters)
        elif strategy.strategy == "replace-with-redacted":
            self.replace_with_redacted(strategy.parameters)
        elif strategy.strategy == "mark-redacted":
            self.mark_redacted(params=strategy.parameters)

    def process_suppression_strategies(self):
        for index, strategy in enumerate(self.active_dataset.suppression_strategies):
            if isinstance(strategy, dict):
                strategy = load_strategy_from_dict(strategy)
            logger.info(f"Processing strategy {index}: {strategy.strategy}")
            self.process_one_suppression_strategy(strategy)

    def make_anonymized_dataset(self):
        # Some suppression strategies might create the output during processing, no need to duplicate.
        tables = [
            table_name
            for row in self.connector.duckdb_connection.sql(
                "select table_name from information_schema.tables"
            ).fetchall()
            for table_name in row
        ]

        if "output" not in tables:
            self.run_aggregation(
                dimensions=self.active_dimensions,
                result_name="output",
                table_name=self.final_source_table,
            )

    def get_absolute_source_file(self, file_path: str):
        if not file_path:
            return file_path
        if self.output_directory:
            file_path = os.path.join(self.output_directory, os.path.basename(file_path))
        if self.output_bucket and not file_path.startswith("s3://"):
            key = file_path
            if self.output_prefix:
                if not key.startswith(f"{self.output_prefix}/"):
                    key = f"{self.output_prefix}/{key}"
                file_path = key_as_s3_uri(bucket=self.output_bucket, key=key)
        return file_path

    def write_anonymized_dataset_to_file(self, file_path=None, **file_kwargs):
        file_path = file_path or self.config.output_file
        if not file_path:
            raise ValueError(
                "You must specify a file path at run time or in the config to produce an output file."
            )
        file_path = self.get_absolute_source_file(file_path)
        file_format = file_path.split(".")[-1]
        logger.info(f"Writing output to {file_path}")
        result_table = self.connector.duckdb_connection.table("output")
        if file_format == "csv":
            result_table.to_csv(file_name=file_path, **file_kwargs)
        elif file_format == "parquet":
            result_table.to_parquet(file_name=file_path, **file_kwargs)
        return file_path

    def run(self, output_file: str = None):
        """
        Read configs passed to the engine and processes the dataset to produce output accordingly.
        """
        for index, dataset in enumerate(self.datasets):
            self.run_one_dataset(index=index, dataset=dataset, output_file=output_file)

    def run_one_dataset(
        self, index: int, dataset: DatasetConfig, output_file: str = None
    ):
        dataset.name = dataset.name or f"{DEFAULT_TABLE_NAME}_{index}"
        table_name = dataset.name
        if index > 0 and not dataset.sql:
            dataset.source_file = self.get_absolute_source_file(dataset.source_file)
        self.connector.table_name = dataset.name
        self.active_dataset = dataset

        logger.info("Loading source dataset")
        if (
            dataset.source_file
            and dataset.source_file in self.source_file_to_table_lkp
            and self.cache_tables_in_memory
        ):
            # We're running in a process during which we've seen this dataset, so we don't need to re-read the file, we already have the data cached in memory.
            table_name = self.source_file_to_table_lkp[dataset.source_file]
            logger.info(
                f"This process has a table {table_name} in memory for {dataset.source_file}"
            )
        elif (
            dataset.source_file
            and dataset.source_file not in self.source_file_to_table_lkp
        ):
            # We are seeing this file reference for the first time, we'll keep track in case we're running in a single process and want to re-use cached data.
            self.connector.load_source_table(source_file=dataset.source_file)
        elif not dataset.source_file and dataset.sql:
            logger.info(
                f"Attempting to create an in-memory table {dataset.name} using: \n{dataset.sql}"
            )
            wrapper_sql = f"create or replace table {dataset.name} as {dataset.sql}"
            self.db.sql(wrapper_sql)
        elif not dataset.source_file:
            # load it in the default manner without a specified table name.
            self.connector.load_source_table()
        logger.info("Running initial aggregation")
        self.run_aggregation(table_name=table_name, initial=True)
        logger.info("Running suppression strategies")
        self.process_suppression_strategies()
        logger.info("Producing anonymized dataset")
        self.make_anonymized_dataset()
        output_file = dataset.output_file or output_file or f"{table_name}.parquet"
        if output_file:
            written_file = self.write_anonymized_dataset_to_file(file_path=output_file)
            logger.info(f"wrote out to {written_file}")
            self.active_dataset.output_file = written_file
            if self.cache_tables_in_memory:
                self.source_file_to_table_lkp[written_file] = dataset.name
                self.connector.duckdb_connection.execute(
                    f'create or replace table "{dataset.name}" as select * from output'
                )
            else:
                self.connector.duckdb_connection.execute(
                    f""" drop table if exists "{dataset.name}" cascade """
                )
            self.connector.duckdb_connection.unregister("output")
            self.connector.duckdb_connection.execute(
                """ drop table if exists output """
            )
