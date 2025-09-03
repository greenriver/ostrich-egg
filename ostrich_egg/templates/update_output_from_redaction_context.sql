update {{output_table|default("output")}} as "output"
set is_redacted = true
, redacted_peers = list_distinct(flatten([[to_redact.redacted_peers, to_redact.previous_redacted_peers], coalesce("output".redacted_peers, [])]))
, peer_group = list_distinct(flatten([[to_redact.peer_group, to_redact.previous_peer_group], coalesce("output".peer_group, [])]))
, redaction_reason = case
    when output.redaction_reason is not null then output.redaction_reason
    when not to_redact.previous_cell_is_anonymous then format('{0} was a small cell', previous_row::json )
    when masked_value_count < 2 then previous_cell_redaction_reason
    when run_sum_by_axis - previous_incidence < {{ threshold }} then previous_cell_redaction_reason || ' and the delta would construct a small population.'

end
from to_redact
where {% for dim in dimensions -%}
    {% if not loop.first %} and {% endif %}coalesce("output".{{ dim | identifier }}::text, '<null>')    = coalesce(to_redact.{{ dim | identifier }}::text, '<null>')
{% endfor %}
