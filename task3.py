from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional
from itertools import islice

from requests import request

DOMAIN = "https://recruitment.developers.emako.pl"


class Connector:
    @staticmethod
    def chunks_list(lst, n):
        """Yield successive n-sized chunks from list."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    @staticmethod
    def chunks_dict(data, SIZE=10000):
        """Yield successive n-sized chunks from dict."""
        it = iter(data)
        for i in range(0, len(data), SIZE):
            yield {k: data[k] for k in islice(it, SIZE)}

    @lru_cache(maxsize=32)
    def headers(self) -> Dict[str, str]:
        # reimplement as needed.
        return {
            "Authorization": None,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def request(self, method: str, path: str, data: dict = {}) -> dict:
        return request(
            method, f"{DOMAIN}/{path}", json=data, headers=self.headers()
        ).json()

    def get_products(self, ids: Optional[List[int]] = None) -> List[dict]:
        list_ = []
        for part in self.chunks_list({ids}, 40):
            list_.append(self.request("GET", "products", {"ids": part})["result"])
        return list_

    def get_all_products_summary(self) -> List[dict]:
        return self.request("GET", "products", {"detailed": False})["result"]

    def get_new_products(self, newer_than: Optional[datetime] = None) -> List[dict]:
        if newer_than is None:
            newer_than = datetime.now() - timedelta(days=5)
        return self.request(
            "GET", "products", {"created_at": {"start": newer_than.isoformat()}}
        )["result"]

    def add_products(self, products: List[dict]):
        for part in self.chunks_list(products, 20):
            self.request("POST", "products", {"products": part})
        try:
            return 'Products has been added'
        except Exception as e:
            return f'Error: {e}'

    def update_stocks(self, stocks: Dict[int, list]):
        current_data = self.get_products(list(stocks))
        for product_entry in current_data:
            product_entry["details"]["supply"] = stocks[product_entry["id"]]
        for part in self.chunks_dict({i: i for i in stocks}, 20):
            self.request("PUT", "products", {"products": part})
        try:
            return 'Products has been updated'
        except Exception as e:
            return f'Error: {e}'

