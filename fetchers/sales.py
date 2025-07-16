import json
import logging
from time import sleep

import requests
from botocore.client import BaseClient


def fetch_sales_data(api_token, nm_ids: list, yesterday: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    batch_size = 20
    result = []

    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i + batch_size]
        logging.info("Fetching data from NM IDs: {}".format(batch))
        payload = {
            "nmIDs": batch,
            "period": {
                "begin": yesterday,
                "end": yesterday
            },
            "timezone": "Europe/Moscow",
            "aggregationLevel": "day"
        }

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception(
                "Invalid response from WB API",
                f"response: {response.text}",
                f"status_code: {response.status_code}"
            )

        data = response.json()['data']
        result.extend(data)
        sleep(70)

    # Формируем имя файла
    filename = "sales.json"

    raw_data_str = json.dumps(result, indent=2)
    return filename, raw_data_str
