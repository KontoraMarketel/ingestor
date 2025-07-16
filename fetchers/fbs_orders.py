import datetime
import json

import requests


def fetch_fbs_orders(api_token: str, yesterday: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://marketplace-api.wildberries.ru/api/v3/orders"
    limit = 1000

    next_val = 0
    all_orders = []

    yesterday_unix = int(datetime.datetime.strptime(yesterday, "%Y-%m-%d").timestamp())

    while True:
        params = {
            "limit": limit,
            "next": next_val,
            "dateFrom": yesterday_unix,
            "dateTo": yesterday_unix
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        orders = data.get("orders", [])
        all_orders.extend(orders)

        next_val = data.get("next", 0)
        if not orders or next_val == 0:
            break

    fbs_orders = [o for o in all_orders if o.get("deliveryType") == "fbs" and o.get("supplyId")]

    # Формируем имя файла
    filename = "fbs_orders.json"

    raw_data_str = json.dumps(fbs_orders, indent=2)
    return filename, raw_data_str
