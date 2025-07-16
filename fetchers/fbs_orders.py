import datetime
import json
import logging

import requests


def fetch_fbs_orders(api_token: str, yesterday: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://marketplace-api.wildberries.ru/api/v3/orders"
    limit = 1000

    next_val = 0
    all_orders = []

    date_from = datetime.datetime.strptime(yesterday, "%Y-%m-%d")
    date_to = date_from + datetime.timedelta(days=1)

    logging.info("Fetching FBS orders...")
    logging.info("Datefrom: %s", date_from)
    logging.info("Datefrom unix: %s", date_from.timestamp())
    logging.info("Dateto: %s", date_to)
    logging.info("Dateto unix: %s", date_to.timestamp())

    while True:
        params = {
            "limit": limit,
            "next": next_val,
            "dateFrom": int(date_from.timestamp()),
            "dateTo": int(date_to.timestamp()),
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        logging.info("FBS orders batch fetched, length: %s", len(response.json()['orders']))

        data = response.json()
        orders = data.get("orders", [])
        all_orders.extend(orders)

        next_val = data.get("next", 0)
        logging.info("Next value: %s", next_val)
        if not orders or next_val == 0:
            break

    fbs_orders = [o for o in all_orders if o.get("deliveryType") == "fbs" and o.get("supplyId")]

    # Формируем имя файла
    filename = "fbs_orders.json"

    raw_data_str = json.dumps(fbs_orders, indent=2)
    return filename, raw_data_str
