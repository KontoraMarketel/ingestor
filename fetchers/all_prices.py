import json

import requests
from botocore.client import BaseClient


def fetch_all_prices(api_token, boto_client: BaseClient, prefix: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
    limit = 1000
    offset = 0
    result = []

    while True:
        params = {
            "limit": limit,
            "offset": offset,
        }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(
                "Invalid response from WB API",
                f"response: {response.text}",
                f"status_code: {response.status_code}"
            )

        goods = response.json().get("data", {}).get("listGoods", [])

        if not goods:
            break

        result.extend(goods)
        offset += limit

    # Формируем имя файла
    object_key = prefix + "all_prices.json"

    raw_data_str = json.dumps(result, indent=2)

    # Загружаем в S3
    boto_client.put_object(
        Bucket="ingests",
        Key=object_key,
        Body=raw_data_str,
        ContentType='application/json',
    )
