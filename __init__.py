import logging
import pandas as pd
from typing import List, Optional, Dict
from App.tableau_client import TableauClient
from App.utils import Utils
import os
import json

logger = logging.getLogger(__name__)

class KPIs:
    """Compute Key Performance Indicators from DataFrames."""

    def __init__(self) -> None:
        pass

    @staticmethod
    def get_value_counts(df: pd.DataFrame, column: str, normalize: bool = True) -> pd.Series:
        """Calculate value counts for a column."""
        return df[column].value_counts(normalize=normalize)

    @staticmethod
    def create_pivot(data: pd.DataFrame, index: str, columns: str, values: str, aggfunc: str = "count") -> pd.DataFrame:
        """Create a pivot table with grand totals."""
        pivot_table = pd.pivot_table(
            data,
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=0,
        )
        pivot_table["Grand Total"] = pivot_table.sum(axis=1)
        grand_total = pd.DataFrame(pivot_table.sum(axis=0)).T
        grand_total.name = "Grand Total"
        grand_total = grand_total.to_frame().T
        return pd.concat([pivot_table, grand_total])

class DataCache:
    """Manage caching with a specified backend."""

    def __init__(self, cache_backend):
        self.cache = cache_backend
        self.logger = logging.getLogger(__name__)

    def set_data_dict(self, base_key: str, df_dict: Dict[str, pd.DataFrame], ttl: int = 3600) -> None:
        """Store a dictionary of DataFrames in the cache."""
        try:
            for view_id, df in df_dict.items():
                if df is not None:
                    key = f"{base_key}:{view_id}"
                    value = df.to_json(orient="records")
                    self.cache.set(key, value, ttl=ttl)
            self.logger.info(f"Cached {len(df_dict)} DataFrames with base key: {base_key}")
        except Exception as e:
            self.logger.error(f"Failed to cache DataFrame dict: {str(e)}")
            raise

    def get_data_dict(self, base_key: str, view_ids: List[str]) -> Dict[str, Optional[pd.DataFrame]]:
        """Retrieve a dictionary of DataFrames from the cache."""
        df_dict = {}
        try:
            for view_id in view_ids:
                key = f"{base_key}:{view_id}"
                data = self.cache.get(key)
                df_dict[view_id] = pd.read_json(data, orient="records") if data else None
                self.logger.debug(f"Cache lookup for {key}: {'hit' if data else 'miss'}")
            return df_dict
        except Exception as e:
            self.logger.error(f"Failed to retrieve cached DataFrame dict: {str(e)}")
            return {view_id: None for view_id in view_ids}

class FileCache:
    """File-based caching implementation."""

    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
            logger.info(f"Created cache directory: {cache_dir}")

    def set(self, key: str, value: str, ttl: int) -> None:
        """Store a value in a file."""
        file_path = os.path.join(self.cache_dir, f"{key}.json")
        with open(file_path, 'w') as f:
            f.write(value)

    def get(self, key: str) -> Optional[str]:
        """Retrieve a value from a file."""
        file_path = os.path.join(self.cache_dir, f"{key}.json")
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return f.read()
        return None

class DataFetcher:
    """Fetch data from Tableau server."""

    def __init__(self, tableau_client: TableauClient):
        self.tableau_client = tableau_client

    def fetch_data(self, view_ids: List[str]) -> Dict[str, pd.DataFrame]:
        """Fetch data from specified Tableau views in parallel."""
        results = Utils.run_parallel_view_fetch(self.tableau_client, view_ids)
        return {vid: df for vid, df in results.items() if isinstance(df, pd.DataFrame)}
