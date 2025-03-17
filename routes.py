from flask import Blueprint, render_template, request
from App.models import KPIs, DataCache, RedisCache, FileCache
from App.utils import Utils
from App import config

main_bp = Blueprint('main', __name__)

@main_bp.route('/', methods=['GET', 'POST'])
def index():
    cache = RedisCache(config.REDIS_URL) if config.PREFER_REDIS else FileCache(config.CACHE_DIR)
    data_cache = DataCache(cache)

    # Retrieve cached data
    df_dict = data_cache.get_data_dict(config.CACHE_KEY, config.views)

    # Handle filters from the form
    filters = request.form.getlist('filters') if request.method == 'POST' else []
    kpi_instance = KPIs()

    # Compute metrics in parallel
    metrics = {}
    tasks = []
    for view_id, df in df_dict.items():
        if df is not None and isinstance(df, pd.DataFrame):  # Ensure df is a DataFrame
            if filters:
                df = df[df.get('category', pd.Series()).isin(filters)]  # Safe filter application
            tasks.append(lambda vid=view_id, d=df.copy(): {
                'value_counts': kpi_instance.get_value_counts(d, 'category'),
                'pivot_table': kpi_instance.create_pivot(d, 'date', 'category', 'value')
            })

    # Execute tasks in parallel
    metric_results = Utils.run_parallel_tasks(tasks)
    for view_id, result in zip(config.views, metric_results):
        metrics[view_id] = result

    # Trigger background data fetch if cache is empty
    if all(df is None or not isinstance(df, pd.DataFrame) for df in df_dict.values()):
        fetch_and_cache_data.delay()

    return render_template('index.html', metrics=metrics, filters=filters, available_filters=['filter1', 'filter2', 'filter3'])
