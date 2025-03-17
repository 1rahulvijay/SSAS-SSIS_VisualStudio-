from flask import Blueprint, render_template, request
import pandas as pd
from App.models import KPIs, DataCache, FileCache
from App.utils import Utils
from App import config
from App.tableau_client import TableauClient
from App.models import DataFetcher

main_bp = Blueprint('main', __name__)

@main_bp.route('/', methods=['GET', 'POST'])
def index():
    print("Entering index function")  # Debug: Confirm route is accessed

    # Initialize file-based cache
    cache = FileCache(config.CACHE_DIR)
    data_cache = DataCache(cache)
    print("Cache initialized")  # Debug: Cache setup complete

    # Retrieve cached data
    df_dict = data_cache.get_data_dict(config.CACHE_KEY, config.views)
    print("Retrieved from cache:", list(df_dict.keys()))  # Debug: Show cached data keys

    # Identify missing views
    missing_views = [vid for vid, df in df_dict.items() if df is None]
    print("Missing views:", missing_views)  # Debug: List views not in cache

    # Fetch data for missing views
    if missing_views:
        print("Fetching missing views")  # Debug: Start fetching data
        try:
            tableau_client = TableauClient(
                config.TABLEAU_TOKEN_NAME,
                config.TABLEAU_TOKEN_VALUE,
                config.TABLEAU_SITE_ID,
                config.TABLEAU_SERVER_URL
            )
            fetcher = DataFetcher(tableau_client)
            missing_df_dict = fetcher.fetch_data(missing_views)
            print("Fetched data for missing views:", list(missing_df_dict.keys()))  # Debug: Show fetched keys
            # Cache the newly fetched data
            data_cache.set_data_dict(config.CACHE_KEY, missing_df_dict, ttl=config.CACHE_TIMEOUT)
            # Update df_dict with fetched data
            for vid, df in missing_df_dict.items():
                df_dict[vid] = df
        except Exception as e:
            print(f"Error fetching data: {str(e)}")  # Debug: Catch and display errors
    print("Final df_dict keys:", list(df_dict.keys()))  # Debug: All available data keys

    # Handle filters from the form
    filters = request.form.getlist('filters') if request.method == 'POST' else []
    kpi_instance = KPIs()

    # Compute metrics in parallel
    metrics = {}
    tasks = []
    for view_id, df in df_dict.items():
        if df is not None and isinstance(df, pd.DataFrame):
            print(f"Processing view: {view_id}")  # Debug: Processing each view
            if filters:
                df = df[df.get('category', pd.Series()).isin(filters)]
            tasks.append(lambda vid=view_id, d=df.copy(): {
                'value_counts': kpi_instance.get_value_counts(d, 'category'),
                'pivot_table': kpi_instance.create_pivot(d, 'date', 'category', 'value')
            })

    # Execute tasks in parallel
    try:
        metric_results = Utils.run_parallel_tasks(tasks)
        for view_id, result in zip([vid for vid, df in df_dict.items() if df is not None and isinstance(df, pd.DataFrame)], metric_results):
            metrics[view_id] = result
        print("Metrics computed for views:", list(metrics.keys()))  # Debug: Show computed metrics
    except Exception as e:
        print(f"Error computing metrics: {str(e)}")  # Debug: Catch metric computation errors

    # Render the template
    return render_template('index.html', metrics=metrics, filters=filters, available_filters=['filter1', 'filter2', 'filter3'])
