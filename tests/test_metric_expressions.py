from config import Metric, Aggregations


def test_basic_metric_expression():
    metric = Metric(aggregation=Aggregations.SUM, column="c", alias="c")
    assert metric.render_as_sql_expression() == 'sum("c")'


def test_custom_metric_expression():
    metric = Metric(
        aggregation=Aggregations.SUM,
        column="c",
        alias="c",
        expression="max(c, 0)",
    )
    assert metric.render_as_sql_expression() == "max(c, 0)"


def test_custom_metric_expression_with_null_is_zero():
    metric = Metric(
        aggregation=Aggregations.SUM,
        column="population_value",
        null_is_zero=True,
    )
    assert metric.render_as_sql_expression() == 'sum(coalesce("population_value", 0))'
