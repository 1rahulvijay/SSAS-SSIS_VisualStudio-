<!DOCTYPE html>
<html>
<head>
    <title>Tableau Metrics Dashboard</title>
    <style>
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid black; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>Tableau Metrics Dashboard</h1>
    <form method="POST">
        <label>Filters:</label>
        <select name="filters" multiple>
            {% for filter in available_filters %}
                <option value="{{ filter }}">{{ filter }}</option>
            {% endfor %}
        </select>
        <button type="submit">Apply Filters</button>
    </form>
    {% for view_id, metric in metrics.items() %}
        <h2>{{ view_id }}</h2>
        <h3>Value Counts</h3>
        {{ metric.value_counts.to_html(classes='table')|safe if metric.value_counts is not none else 'No data' }}
        <h3>Pivot Table</h3>
        {{ metric.pivot_table.to_html(classes='table')|safe if metric.pivot_table is not none else 'No data' }}
    {% endfor %}
</body>
</html>
