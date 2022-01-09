from sqlite3 import connect, Cursor
from requests.auth import HTTPBasicAuth
from dataclasses import dataclass
from datetime import datetime

import logging
import json
import requests

logging.basicConfig(filename='logs/task1.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

DOMAIN = "https://recruitment.developers.emako.pl"
HTTP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

sql = connect(database="database.sqlite")


@dataclass
class AuthSession:
    username: str
    password: str
    token: str
    session: requests

    def __init__(self):
        self.get_login_data()
        self.get_access_token()
        self.create_session()

    def get_login_data(self):
        with open('credentials.json') as file:
            data = json.load(file)
            self.username = data['username']
            self.password = data['password']

    def get_access_token(self):
        r = requests.post(DOMAIN + '/login/aws?grant_type=bearer',
                          data=HTTP_HEADERS,
                          auth=HTTPBasicAuth(self.username, self.password))
        self.token = r.json()['access_token']

    def create_session(self):
        self.session = requests.Session()
        self.session.headers.update({'Authorization': 'Bearer ' + self.token})

    def session(self):
        return self.session


class UpdateProductTable:
    def __init__(self, auth_session: requests):
        self.session = auth_session

    @staticmethod
    def get_product_details(product_id: int) -> json:
        body = {
            "ids": [
                product_id
            ],
            "detailed": True,
            "pagination": {
                "page_size": 40,
                "index": 0
            }
        }
        return session.session.get(DOMAIN + '/products',
                                   json=body)

    @staticmethod
    def get_time_now():
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S")

    def update_product_details(self, product_id: int) -> None:
        product_details = self.get_product_details(product_id=product_id)
        stock_details = product_details.json()['result'][0]['details']['supply']
        product_id = product_details.json()['result'][0]['id']
        for stock in stock_details:
            variant_id = stock['variant_id']
            logging.info(f'Stocks quantities for product: {product_id} and variant: {variant_id}')
            for stock in stock['stock_data']:
                stock_id = stock['stock_id']
                quantity = stock['quantity']
                logging.info(f"Stock id {stock_id}    Quantity: {quantity}")
                sql.execute(
                    "UPDATE product_stocks SET supply = ?, time = ? "
                    "WHERE product_id=? and variant_id=? and stock_id=? "
                    "ORDER BY id desc "
                    "limit 1",
                    (quantity, self.get_time_now(), product_id, variant_id, stock_id))
            sql.commit()

    @staticmethod
    def get_products_id() -> Cursor:
        q = "SELECT DISTINCT(product_id) FROM  product_stocks"
        return sql.execute(q)

    def update_stock_quantity(self) -> None:
        for product in self.get_products_id():
            self.update_product_details(product[0])


try:
    session = AuthSession()
    update_product_table = UpdateProductTable(session)
    update_product_table.update_stock_quantity()
except Exception as e:
    logging.error(e)

sql.close()
