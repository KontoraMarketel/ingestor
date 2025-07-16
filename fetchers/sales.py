import json
import logging

import requests


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

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logging.error(err)
            continue

        data = response.json()['data']
        result.extend(data)

    # Формируем имя файла
    filename = "sales.json"

    raw_data_str = json.dumps(result, indent=2)
    return filename, raw_data_str
