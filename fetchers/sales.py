import json

import requests
from botocore.client import BaseClient


def fetch_sales_data(api_token, boto_client: BaseClient, prefix: str, nm_ids: list, yesterday: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    payload = {
        "nmIDs": nm_ids,
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

    # Формируем имя файла
    object_key = prefix + "sales.json"

    raw_data_str = json.dumps(data, indent=2)

    # Загружаем в S3
    boto_client.put_object(
        Bucket="ingests",
        Key=object_key,
        Body=raw_data_str,
        ContentType='application/json',
    )
