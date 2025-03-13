"""
The actual "workhorse" part of the anonymization service. This is where the data gets loaded and processed.
"""

from __future__ import annotations
from collections import namedtuple
import os
from typing import Dict, List

import duckdb
from jinja2 import Template

from utils import (
    DEFAULT_MASKING_VALUE,
    dict_to_filter_expressions,
    get_logger,
    identifier,
    make_when_statement_from_dict,
    merge_conditions,
)
from config import (
    Config,
    DatasetConfig,
    ReplaceWithRedactedParameters,
    MergeDimensionValuesParameters,
    MarkRedactedParameters,
    Metric,
    Aggregations,
    Strategy,
    # SuppressionStrategy,
    DEFAULT_THRESHOLD,
)
from connectors import Connector, DEFAULT_TABLE_NAME
from connectors.s3 import key_as_s3_uri


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
    field_names=["other_dimension_values", "remapped_lookup"],
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
        self.metrics = dataset.metrics
        self.active_dimensions = dataset.dimensions
        self.removed_dimensions = []
        self.__active_dataset = dataset

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
            metrics = [Metric(aggregation=aggregation, column=column)]
        for index, metric in enumerate(metrics):
            metric.alias = metric.alias or f"m_{index}"
        self.__metrics = metrics

    @property
    def metric_aliases(self) -> dict:
        """
        Return column-name: metric-name mapping.
        """
        metric_aliases = {
            metric.alias: metric.render_as_sql_expression() for metric in self.metrics
        }
        return metric_aliases

    @property
    def metric_sql_list(self) -> list:
        return [
            f"{metric} as {identifier(alias)}"
            for alias, metric in self.metric_aliases.items()
        ]

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

    def get_wrapper_metrics_pass_expression_from_redaction_expression(self) -> str:
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
            "metric_aliases": self.metric_aliases,
            "allow_zeroes": self.allow_zeros,
            "threshold": self.threshold,
        }
        check_list = Template(check_list_template).render(context)
        check_for_did_not_pass = f"not list_contains([{check_list}], false)"
        return check_for_did_not_pass

    def get_rendered_aggregation_query(
        self, dimensions: list = None, table_name: str = None
    ) -> str:
        dimensions = dimensions or self.active_dimensions
        dimensions_as_sql_object = self.dimensions_as_sql_expressions(
            dimensions=dimensions
        )
        context = {
            "metric_list": self.metric_sql_list,
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
    ):
        """
        Run aggregations according to the dataset definition.
        This will be called iteratively, initially with the full set of dimensions, then potentially pruned.
        """
        dimensions = dimensions or self.active_dimensions
        sql = self.get_rendered_aggregation_query(
            dimensions=dimensions, table_name=table_name
        )
        __result__ = self.connector.duckdb_connection.sql(sql)
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
        metric_name="m_0",
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
        metrics_sql = ", ".join(self.metric_sql_list)
        metric_sort = ", ".join(self.metric_aliases.keys())
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

        # process peers in smallest to largest order
        peer_ids = [
            x[0]
            for x in self.connector.duckdb_connection.sql(
                f"""
                select distinct peer_id
                from identify_peers
                order by {IS_ANONYMOUS_COLUMN}, {metric_sort}, peer_id
                """
            ).fetchall()
        ]
        # this variable goes across peers
        # it will be set to True if a small cell needs a neighbor to be suppressed.
        must_anonymize_next = False
        for peer_id in peer_ids:
            # identify the peers within this aggregation that might need to be latently redacted
            peer_value_sql = f"""
                select x as peer
                from (
                    select {sql_expressions.aliases}, sum({metric_name}) as total
                    from identify_peers
                    where peer_id = $peer_id
                    group by {sql_expressions.aliases}
                    limit 1
                ) as x
            """
            peer_values, *_ = self.connector.duckdb_connection.sql(
                query=peer_value_sql, params={"peer_id": peer_id}
            ).fetchone()
            total = peer_values.pop("total", None)

            filter_by_peer = duckdb.ColumnExpression("peer_id").isin(peer_id)
            this_peer_relation = (
                identify_peers.filter(filter_by_peer)
                .select(
                    DIMENSION_VALUE_COLUMN,
                    IS_ANONYMOUS_COLUMN,
                    *self.metric_aliases.keys(),
                )
                .order(metric_sort)
            )
            peer_result, must_anonymize_next = (
                self._collect_redactions_from_peer_relation(
                    this_peer_relation=this_peer_relation,
                    peer_total=total,
                    peer_values=peer_values,
                    masking_value=masking_value,
                    must_anonymize_next=must_anonymize_next,
                )
            )
            final_result.extend(peer_result)

        return final_result

    def _collect_redactions_from_peer_relation(
        self,
        this_peer_relation: duckdb.DuckDBPyRelation,
        peer_total: int,
        peer_values: dict,
        must_anonymize_next: bool = False,
        masking_value: str = DEFAULT_MASKING_VALUE,
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

        anonymized = False
        row_count, *_ = this_peer_relation.count("*").fetchone()
        within_peer_index = 0
        while within_peer_index < row_count:
            local_result = this_peer_relation.fetchone()
            if not local_result:
                break
            local_result = dict(zip(this_peer_relation.columns, local_result))
            value_is_fine = local_result[IS_ANONYMOUS_COLUMN]

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
                    metric_list=self.metric_sql_list,
                    anonymous_expression=self.anonymous_expression,
                    is_anonymous=IS_ANONYMOUS_COLUMN,
                )
                working_subtotal, *_ = self.connector.duckdb_connection.sql(
                    sql, params={"seen_values": seen_values}
                ).fetchone()
                working_total_is_fine = working_subtotal[IS_ANONYMOUS_COLUMN]
            else:
                working_total_is_fine = value_is_fine

            dimension_value = local_result[DIMENSION_VALUE_COLUMN]

            seen_values.append(dimension_value)

            first_value_is_good = within_peer_index == 0 and value_is_fine

            if not must_anonymize_next and first_value_is_good:
                """
                The first value in the peer group is already anonymized and we don't need to anonymize from a previous peer group, this is anonymous already.
                """
                anonymized = True
                break

            elif must_anonymize_next and value_is_fine:
                """
                Have to anonymize based on a previous redaction.
                """
                values_to_mask.append(dimension_value)
                must_anonymize_next = False
                anonymized = True
                break

            elif not value_is_fine:
                """
                Base case small-cell for suppression.
                """
                logger.debug(
                    f"Redacting {dimension_value} as it is below the threshold of {self.threshold}."
                )
                values_to_mask.append(dimension_value)
                must_anonymize_next = True

            elif (
                len(values_to_mask) >= 1
                and (not anonymized or must_anonymize_next)
                and value_is_fine
                and row_count > 2
                and working_total_is_fine
            ):
                """
                There has been suppression of previous values and we are now safely anonymized
                """
                filter_by_excluded_values = duckdb.ColumnExpression(
                    DIMENSION_VALUE_COLUMN
                ).isnotin(*[duckdb.ConstantExpression(v) for v in values_to_mask])
                remainder, *_ = (
                    this_peer_relation.filter(filter_by_excluded_values)
                    .sum(METRIC_VALUE_COLUMN)
                    .fetchone()
                )

                logger.debug(
                    f"Latent revelation through subtraction suppressed: \
                        Subtraction from total {peer_total} yields {remainder} with at least {row_count - len(values_to_mask)} revealed values out of {row_count}"
                )
                anonymized = True
                must_anonymize_next = False
                break

            elif values_to_mask and not anonymized:
                """
                This is the base case for latent suppression in which the value itself is fine but we must suppress to prevent latent revelation.
                """
                logger.debug(
                    f"Redacting {dimension_value} as it would latently reveal an unacceptable metric value through subtraction."
                )
                values_to_mask.append(dimension_value)
                seen_values.append(dimension_value)

            elif must_anonymize_next and not value_is_fine:
                # a small value was seen by previous peer, the next value must be anonymized irrespective of the rest of the peer size.
                logger.debug(
                    f"Redacting {dimension_value} as it would latently reveal an unacceptable metric value through subtraction."
                )
                values_to_mask.append(dimension_value)
                seen_values.append(dimension_value)

            within_peer_index += 1
        peer_result = (
            [
                RedactionIterationResult(
                    other_dimension_values=peer_values,
                    remapped_lookup={
                        dimension_value: masking_value
                        for dimension_value in values_to_mask
                    },
                )
            ]
            if values_to_mask
            else []
        )
        return peer_result, must_anonymize_next

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

    def replace_with_redacted(self, params: ReplaceWithRedactedParameters):
        """
        Sets the update expression to be used in the final result by assigning redaction
        values to dimensions.
        """
        # TODO: add in the non-summable dimensions?
        redactions = self.get_dimension_values_to_redact_with_latency_check(
            dimension=params.redacted_dimension,
            masking_value=params.masking_value,
            metric_name=self.metrics[0].alias,
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
        self.connector.duckdb_connection.execute(
            "create or replace table output as select * from result"
        )
        result_table = self.connector.duckdb_connection.table("output")

        dimension_to_use = params.redacted_dimension
        redactions = []
        if params.non_summable_dimensions:
            columns = [identifier(dim) for dim in params.non_summable_dimensions]
            pages = (
                result_table.select(*columns)
                .distinct()
                .to_df()
                .to_dict(orient="records")
            )
            for page in pages:
                filters = dict_to_filter_expressions(data=page)
                condition = merge_conditions(filters)
                result = result_table.filter(condition)
                self.connector.duckdb_connection.register("result", result)
                page_redactions = (
                    self.get_dimension_values_to_redact_with_latency_check(
                        dimension=dimension_to_use,
                        metric_name=list(self.metric_aliases.keys())[0],
                    )
                )
                redactions.extend(page_redactions)
        else:
            self.connector.duckdb_connection.register("result", result_table)
            redactions = self.get_dimension_values_to_redact_with_latency_check(
                dimension=dimension_to_use,
                metric_name=list(self.metric_aliases.keys())[0],
            )
        alter_sql = """
        alter table output
        add column "is_redacted" boolean default false
        """
        self.connector.duckdb_connection.execute(alter_sql)
        self.connector.duckdb_connection.execute(
            "update output set is_redacted = true where not is_anonymous"
        )
        for redaction in redactions:
            for old, new in redaction.remapped_lookup.items():
                redaction.other_dimension_values[dimension_to_use] = old
                filters = dict_to_filter_expressions(
                    data=redaction.other_dimension_values
                )
                condition = merge_conditions(filters)
                update_sql = f"""
                update output
                set "is_redacted" = true
                where {condition}
                """
                self.connector.duckdb_connection.execute(query=update_sql)
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
        if self.output_directory:
            file_path = os.path.join(self.output_directory, os.path.basename(file_path))
        if self.output_bucket:
            key = file_path
            if self.output_prefix:
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
        self.connector.init_duckdb()
        for index, dataset in enumerate(self.datasets):
            self.run_one_dataset(index=index, dataset=dataset)

    def run_one_dataset(
        self, index: int, dataset: DatasetConfig, output_file: str = None
    ):
        dataset.name = dataset.name or f"{DEFAULT_TABLE_NAME}_{index}"
        table_name = dataset.name
        if index > 0:
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
        self.run_aggregation(table_name=table_name)
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
