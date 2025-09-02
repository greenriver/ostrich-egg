{#
Query the redaction context to find cells that need suppressed to prevent latent revelation.
 #}
select *
from redaction_context
where
    not is_redacted
    and should_redact_along_axis(
        incidence := {{ incidence_column | identifier }},
        masked_value_count := masked_value_count,
        minimum_threshold := {{ threshold }},
        is_anonymous := is_anonymous,
        previous_cell_redacted := previous_cell_redacted,
        run_sum_by_axis := run_sum_by_axis
    )
    {#- If any dimensions are not aggregable, i.e., users won't know the total sum of these dimensions,
    then we just need to only consider redacting values where those dimensions match.

    Else, this might consider other peer-groups to redact along these dimensions.
 -#}
    {%- for dim in non_summable_dimensions %} and {{ dim | identifier }} = "previous_{{ dim | dequote }}" {% endfor %}
