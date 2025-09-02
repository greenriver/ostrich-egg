update {{output_table|default("output")}} as "output"
set is_redacted = true
, redacted_peers = to_redact.redacted_peers
, peer_group = to_redact.peer_group
from to_redact
where {% for dim in dimensions -%}
    {% if not loop.first %} and {% endif %}"output".{{ dim | identifier }} = to_redact.{{ dim | identifier }}
{% endfor %}
