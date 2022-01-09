from requests import get
from typing import List

import datetime
import sqlite3
import json
import logging

logging.basicConfig(filename='logs/task2.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class SQLite:
    def __init__(self, file='database.sqlite'):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        self.conn.row_factory = sqlite3.Row
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


class InsertProduct:
    def __init__(self, domain: str):
        self.domain = domain
        self.products = []
        self.all = []
        self.supply = 0
        self.products_to_insert = []

    def get_product(self, target: int):
        return get(
            self.domain + "products/example?id=" + str(target)
        )

    def single_insertion(self):
        logging.info(f"Inserting {len(self.products_to_insert)} different records")
        query_base = "INSERT INTO product_stocks (time, product_id, variant_id, stock_id, supply) VALUES "
        query_values = ""
        for value_tuple in self.products_to_insert:
            logging.info(f"Inserting to db: {value_tuple}")
            query_values += f"{value_tuple}, "
        query = query_base + query_values
        with SQLite('database.sqlite') as cursor:
            cursor.execute(query[:-2])

    @staticmethod
    def insert_product(product_id: str, variant: str, stock_id: str, supply: str):
        logging.info("Inserting to database")
        with SQLite('database.sqlite') as cur:
            try:
                cur.execute(
                    "INSERT INTO product_stocks (time, product_id, variant_id, stock_id, supply) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (str(datetime.datetime.now())[:19], product_id, variant, stock_id, supply)
                )
            except Exception as e:
                return False
            return True

    def process_product(self, product):
        for x in product["details"]["supply"]:
            for y in x["stock_data"]:
                logging.info("Adding record")
                self.products_to_insert.append((str(datetime.datetime.now())[:19], str(product["id"]),
                                                str(x["variant_id"]), str(y['stock_id']), str(y['quantity'])))

    def process_bundle(self, product):
        bundle_products = []
        for p in product["bundle_items"]:
            bundle_products.append(p["id"])
        id = product["id"]
        for p in bundle_products:
            r = self.get_product(p)
            product = json.loads(r.content)
            for supply in product["details"]["supply"]:
                for stoc in supply["stock_data"]:
                    self.products_to_insert.append((str(datetime.datetime.now())[:19], str(id), supply['variant_id'],
                                                    stoc['stock_id'], stoc['quantity']))

    def get_product_details(self, product_list: List[int]):
        for product_id in product_list:
            try:
                logging.info(f"Getting details for product: {product_id}")
                response = self.get_product(product_id)
                product = json.loads(response.content)
                if product["type"] != "bundle":
                    self.process_product(product)
                else:
                    logging.info("Processing bundle")
                    self.process_bundle(product)
            except Exception as e:
                logging.error(f'Error: {e}')
                quit()
        logging.info("Inserting gathered data")
        self.single_insertion()


insert = InsertProduct(domain="https://recruitment.developers.emako.pl/")
insert.get_product_details([-2, -3])
