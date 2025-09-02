{#
Generates the context needed to evaluate which cells need to be suppressed to avoid latent revelation.

For each each combination of dimensions, the calling method `redact_from_non_anonymous_cells`
passes configurations for sorting and partitioning the dataset such that we find latent revelation through iterative windows.
 #}
{%- set partition_and_order_by -%}
over( partition by {{dimension_set | list_of_identifiers}} order by {{order_by_columns | join(', ')}})
{%- endset -%}
{%- set peer_group_expression -%}
{ {#- -#}
{% for dim in dimension_set %}
    {{ dim | identifier }}:{{ dim | identifier }}{% if not loop.last %}, {% endif %}
{% endfor %}
{#- -#} }
{%- endset -%}

{%- set redacted_peers_expression -%}
{ {#- -#}
{{ dimension | identifier }}:{{ dimension | identifier }}
{#- -#} }
{%- endset -%}
select
    *
    {#- Duckdb star expression `replace` replaces the newly created output columns with the windowed response. #}
    replace(
        coalesce(lag({{ peer_group_expression }}) {{ partition_and_order_by }}, {{ peer_group_expression }})::json
        as peer_group,
        coalesce(
            lag({{ redacted_peers_expression }}) {{ partition_and_order_by }}, {{ redacted_peers_expression }}
        )::json as redacted_peers
    ),
    lag(is_redacted) {{ partition_and_order_by }} as previous_cell_redacted,
    lag({{ incidence_column | identifier }}) {{ partition_and_order_by }} as previous_incidence,
    sum({{ incidence_column | identifier }}) {{ partition_and_order_by }} as run_sum_by_axis,
    count(*) filter(where is_redacted) over (
        partition by {{ dimension_set | list_of_identifiers }}
    ) as masked_value_count
    {%- for dim in non_summable_dimensions %}
        , lag({{ dim | identifier }}) {{ partition_and_order_by }} as "previous_{{ dim | dequote }}"
    {%- endfor %}
from {{ output_table | default("output") }}
