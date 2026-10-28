"""Build a portable AI/BI dashboard file. Does not connect to Databricks."""
import argparse
import copy
import hashlib
import json
import re
from pathlib import Path

import sqlglot
from sqlglot import exp

from databricks.labs.lsql.lakeview.model import (
    AxisSpec, BarSpec, CategoricalScale, ChartEncodingMapWithSingleXy,
    ControlEncodingMap, ControlFieldEncoding, CounterEncodingMap, CounterFieldEncoding,
    CounterSpec, Dashboard, Dataset, DateRangePickerSpec, Direction, Field,
    FormatConfig, Layout, LineSpec, MarkSpec, MomentTimeFormat, MultiSelectSpec,
    NamedQuery, NumeralNumberFormat, Order, Page, Position, QuantitativeScale,
    RenderFieldEncoding, SingleFieldAxisEncoding, Sort, SortBy, TableEncodingMap,
    TableV2Spec, TemporalScale, Widget, WidgetFrameSpec, Query,
)

ROOT = Path(__file__).resolve().parents[1]


def identifier(label):
    return hashlib.sha256(('olist_dashboard_' + label).encode()).hexdigest()[:12]


def build(catalog='workspace'):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', catalog):
        raise ValueError('Invalid catalog identifier.')
    dataset_name = identifier('eligible_orders')
    query = f"""SELECT order_id, purchase_date, purchase_month, customer_state,
       items_value, freight_value, delivery_days,
       CASE WHEN on_time_delivery IS NOT NULL THEN 1 ELSE 0 END AS sla_evaluable,
       CASE WHEN on_time_delivery THEN 1 ELSE 0 END AS on_time,
       CASE WHEN NOT on_time_delivery THEN 1 ELSE 0 END AS late,
       CAST(CASE WHEN on_time_delivery THEN 100
                 WHEN NOT on_time_delivery THEN 0
                 ELSE NULL END AS DOUBLE) AS on_time_percent_per_order
FROM `{catalog}`.`olist_gold`.`fact_orders`
WHERE is_sales_eligible
"""
    layout = []

    def frame(title, description=None):
        return WidgetFrameSpec(title=title, show_title=True, description=description,
                               show_description=bool(description))

    def place(label, widget, x, y, width, height):
        widget.name = identifier(label)
        layout.append(Layout(widget=widget, position=Position(x=x, y=y, width=width, height=height)))

    # Lakeview's visualizer resolves the implicit chart query by this exact name.
    # Databricks Labs' dashboard generator documents the same requirement.
    def named(fields, name='main_query', orders=None):
        return NamedQuery(name=name, query=Query(dataset_name=dataset_name,
            fields=[Field(name=alias, expression=expression) for alias, expression in fields],
            disaggregated=False, orders=orders))

    def counter(label, title, expression, x, number_format, description=None):
        place(label, Widget(name=None, queries=[named([('metric', expression)])], spec=CounterSpec(
            encodings=CounterEncodingMap(value=CounterFieldEncoding(field_name='metric', display_name=title)),
            format=FormatConfig(number_format=NumeralNumberFormat(format=number_format)),
            frame=frame(title, description))), x, 4, 2, 3)

    place('header', Widget(name=None, textbox_spec=(
        '# Olist · Sales & Delivery\n'
        '**Jamille Ghazaleh** · Brazilian e-commerce\n\n'
        'Explore delivered orders by purchase date and customer state. Item values in Brazilian reais (BRL), excluding freight.'
    )), 0, 0, 6, 2)

    for label, column, title, spec_type, x in [
        ('date_filter', 'purchase_date', 'Purchase date', DateRangePickerSpec, 0),
        ('state_filter', 'customer_state', 'Customer state (Brazil)', MultiSelectSpec, 3),
    ]:
        # Associativity is a Lakeview-provided predicate, as in Databricks Labs' filter generator.
        query_name = 'filter_values'
        filter_query = named([
            (column, '`' + column + '`'),
            (column + '_associativity', 'COUNT_IF(`associative_filter_predicate_group`)'),
        ], name=query_name)
        spec = spec_type(encodings=ControlEncodingMap(fields=[ControlFieldEncoding(
            field_name=column, query_name=query_name, display_name=title)]), frame=frame(title))
        place(label, Widget(name=None, queries=[filter_query], spec=spec), x, 2, 3, 2)

    counter('orders', 'Delivered orders', 'COUNT(`order_id`)', 0, '0,0',
            'Orders with valid items.')
    counter('items_value', 'Item value (BRL)', 'SUM(`items_value`)', 2, '0,0.00',
            'Item value, excluding freight.')
    # Keep the serialized visualization expression a single supported aggregate.
    # AVG of nullable per-order 0/100 indicators equals the eligible-order rate.
    counter('on_time', 'On-time delivery (%)',
            'AVG(`on_time_percent_per_order`)', 4, '0.00',
            'Orders with a valid deadline.')

    month_fields = [('month', '`purchase_month`'), ('total_items', 'SUM(`items_value`)')]
    month_spec = LineSpec(encodings=ChartEncodingMapWithSingleXy(
        x=SingleFieldAxisEncoding(field_name='month', scale=TemporalScale(),
            axis=AxisSpec(title='Purchase month'), display_name='Month'),
        y=SingleFieldAxisEncoding(field_name='total_items', scale=QuantitativeScale(),
            axis=AxisSpec(title='Item value (BRL)'), display_name='Item value (BRL)')),
        format=FormatConfig(number_format=NumeralNumberFormat(format='0,0'),
                            time_format=MomentTimeFormat(format='MMM YYYY')),
        mark=MarkSpec(colors=['#2563EB']), frame=frame('Monthly item value'))
    place('monthly_chart', Widget(name=None, queries=[named(month_fields,
        orders=[Order(expression='`month`', direction=Direction.ASC)])], spec=month_spec), 0, 7, 4, 8)

    state_fields = [('state', '`customer_state`'), ('total_items', 'SUM(`items_value`)')]
    state_spec = BarSpec(encodings=ChartEncodingMapWithSingleXy(
        x=SingleFieldAxisEncoding(field_name='total_items', scale=QuantitativeScale(),
            axis=AxisSpec(title='Item value (BRL)'), display_name='Item value (BRL)'),
        y=SingleFieldAxisEncoding(field_name='state', scale=CategoricalScale(sort=Sort(by=SortBy.X_REVERSED)),
            axis=AxisSpec(title='State'), display_name='State')),
        format=FormatConfig(number_format=NumeralNumberFormat(format='0,0')),
        mark=MarkSpec(colors=['#0D9488']), frame=frame('Item value by state'))
    place('state_chart', Widget(name=None, queries=[named(state_fields,
        orders=[Order(expression='`total_items`', direction=Direction.DESC)])], spec=state_spec), 4, 7, 2, 8)

    table_fields = [
        ('month', '`purchase_month`', 'Month'),
        ('orders', 'COUNT(`order_id`)', 'Orders'),
        ('items_value', 'SUM(`items_value`)', 'Item value (BRL)'),
        ('freight_value', 'SUM(`freight_value`)', 'Freight (BRL)'),
        ('average_items', 'AVG(`items_value`)', 'Avg. item value/order (BRL)'),
        ('sla_orders', 'SUM(`sla_evaluable`)', 'Evaluable deliveries'),
        ('on_time_pct', 'AVG(`on_time_percent_per_order`)', 'On time (%)'),
    ]
    table_spec = TableV2Spec(encodings=TableEncodingMap(columns=[
        RenderFieldEncoding(field_name=alias, display_name=title) for alias, expression, title in table_fields
    ]), frame=frame('Monthly breakdown'))
    place('monthly_table', Widget(name=None, queries=[named([(a,e) for a,e,_ in table_fields],
        orders=[Order(expression='`month`', direction=Direction.ASC)])], spec=table_spec), 0, 15, 6, 6)

    place('definitions', Widget(name=None, textbox_spec=(
        '### How to read these metrics\n'
        '- **Coverage:** delivered orders with items accepted by the project\'s quality rules. '
        'Rejected rows are retained in Bronze and quarantine tables.\n'
        '- **On time:** delivery date on or before the promised date; orders without a valid assessment are excluded from the rate denominator.\n'
        '- **Period:** purchase month. Historical data from 2016–2018; the first and last months may be partial. '
        'Start with the full date range and all states.\n'
        '- **Source:** Olist · Brazilian E-Commerce Public Dataset · CC BY-NC-SA 4.0.'
    )), 0, 21, 6, 4)

    dashboard = Dashboard(datasets=[Dataset(name=dataset_name, query=query,
        display_name='Delivered orders · Gold')], pages=[Page(name=identifier('overview'),
        display_name='Sales & Delivery', layout=layout)])
    payload = dashboard.as_dict()
    # Current file exports serialize queries as queryLines; the Labs model accepts both forms.
    for dataset in payload['datasets']:
        dataset['queryLines'] = dataset.pop('query').splitlines(keepends=True)
    assert Dashboard.from_dict(payload).as_dict() == dashboard.as_dict()
    return payload, query


def validate_structure(payload):
    canonical = Dashboard.from_dict(payload).as_dict()
    assert len(payload['datasets']) == 1 and len(payload['pages']) == 1
    dataset_name = payload['datasets'][0]['name']
    names = set()
    rectangles = []
    widget_types = []
    for item in payload['pages'][0]['layout']:
        widget, pos = item['widget'], item['position']
        assert widget['name'] not in names
        names.add(widget['name'])
        x,y,w,h = [pos[k] for k in ['x','y','width','height']]
        assert min(x,y) >= 0 and w > 0 and h > 0 and x+w <= 6
        for xx,yy,ww,hh in rectangles:
            assert not (x < xx+ww and x+w > xx and y < yy+hh and y+h > yy)
        rectangles.append((x,y,w,h))
        queries = {q['name']: q['query'] for q in widget.get('queries', [])}
        for query in queries.values():
            assert query['datasetName'] == dataset_name
            assert len({f['name'] for f in query['fields']}) == len(query['fields'])
        if 'spec' not in widget:
            continue
        spec = widget['spec']
        widget_types.append(spec['widgetType'])
        if not spec['widgetType'].startswith('filter-'):
            assert 'main_query' in queries, 'Visual queries must be named main_query for Lakeview rendering.'
            for field in queries['main_query']['fields']:
                tree = sqlglot.parse_one(field['expression'], read='databricks')
                # Keep this portable file's visual fields simple. Derived per-order
                # calculations live in dataset SQL rather than arithmetic widget fields.
                assert isinstance(tree, exp.Column) or (
                    isinstance(tree, (exp.Count, exp.Sum, exp.Avg)) and isinstance(tree.this, exp.Column)
                ), 'Visual fields must be a column or a single aggregate over a dataset column.'
        if widget['name'] == identifier('on_time'):
            expression = queries['main_query']['fields'][0]['expression']
            assert expression == 'AVG(`on_time_percent_per_order`)', 'Percentage counter must average nullable per-order indicators.'
            assert '%' not in spec.get('format', {}).get('numberFormat', {}).get('format', ''), 'Values on a 0-100 scale must use a numeric format; the unit is in the title.'
        fields = {f['name'] for query in queries.values() for f in query['fields']}
        def check_encoding(value):
            if isinstance(value, dict):
                if 'fieldName' in value:
                    assert value['fieldName'] in fields
                if 'queryName' in value:
                    assert value['queryName'] in queries
                for v in value.values():
                    check_encoding(v)
            elif isinstance(value, list):
                for v in value:
                    check_encoding(v)
        check_encoding(spec['encodings'])
    assert widget_types.count('counter') == 3
    assert {'filter-date-range-picker','filter-multi-select','line','bar','table'} <= set(widget_types)
    # Only portable definitions and SQL are included. No connection, publication or sharing settings.
    assert set(payload) == {'datasets', 'pages'}
    return {'model': 'Databricks Labs LSQL 0.17.0', 'datasets': 1, 'pages': 1,
            'widgets': len(names), 'widget_types': widget_types, 'layout_overlap': False,
            'model_roundtrip': bool(canonical), 'main_query_bindings': True,
            'simple_visual_aggregations': True, 'result': 'passed'}


def validate_binding_regression(payload):
    broken = copy.deepcopy(payload)
    counter = next(item['widget'] for item in broken['pages'][0]['layout']
                   if item['widget'].get('spec', {}).get('widgetType') == 'counter')
    counter['queries'][0]['name'] = 'main'
    try:
        validate_structure(broken)
    except AssertionError as error:
        assert 'main_query' in str(error)
        return True
    raise AssertionError('The validator accepted the visual binding regression.')


def validate_percentage_regression(payload):
    broken = copy.deepcopy(payload)
    counter = next(item['widget'] for item in broken['pages'][0]['layout']
                   if item['widget']['name'] == identifier('on_time'))
    counter['queries'][0]['query']['fields'][0]['expression'] = 'SUM(`on_time`) / NULLIF(SUM(`sla_evaluable`), 0)'
    try:
        validate_structure(broken)
    except AssertionError as error:
        assert 'single aggregate' in str(error)
        return True
    raise AssertionError('The validator accepted the percentage visualization regression.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', default='workspace')
    args = parser.parse_args()
    payload, query = build(args.catalog)
    validation = validate_structure(payload)
    validation['regression_wrong_main_name_rejected'] = validate_binding_regression(payload)
    validation['regression_compound_visual_measure_rejected'] = validate_percentage_regression(payload)
    destination = ROOT / 'dashboard'
    destination.mkdir(exist_ok=True)
    path = destination / 'Olist_Analytics.lvdash.json'
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n')
    (destination / 'orders_dataset.sql').write_text(query)
    (ROOT / 'docs/validation_dashboard_structure.json').write_text(json.dumps(validation, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'path':str(path),'validation':validation}, ensure_ascii=False))
