from App import create_app, celery
from App.models import DataFetcher, DataCache, RedisCache, FileCache
from App.tableau_client import TableauClient
import logging
from typing import List
from App import config

logger = logging.getLogger(__name__)

@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_and_cache_data(self, source: str = "tableau", view_ids: List[str] = None):
    """Celery task to fetch data from Tableau and cache it in the background."""
    if view_ids is None:
        view_ids = config.views
    with create_app().app_context():
        from App.utils import Utils
        Utils.setup_logging(config.LOG_LEVEL)
        try:
            tableau_client = TableauClient(
                config.args["NAME"],
                config.args["TOKEN"],
                config.args["SITE"],
                config.args["SERVER"]
            )
            cache = RedisCache(config.REDIS_URL) if config.PREFER_REDIS else FileCache(config.CACHE_DIR)
            data_cache = DataCache(cache)
            fetcher = DataFetcher(tableau_client)
            df_dict = fetcher.fetch_data(view_ids)
            data_cache.set_data_dict(config.CACHE_KEY, df_dict, ttl=config.CACHE_TIMEOUT)
            logger.info("Data fetched and cached successfully.")
            return True
        except Exception as e:
            logger.error(f"Error in fetch_and_cache_data task: {str(e)}")
            self.retry(exc=e)
