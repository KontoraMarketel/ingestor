import json

import requests
from botocore.client import BaseClient


def fetch_commissions(api_token, boto_client: BaseClient, prefix: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"


    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            "Invalid response from WB API",
            f"response: {response.text}",
            f"status_code: {response.status_code}"
        )

    data = response.json()

    # Формируем имя файла
    object_key = prefix + "comissions.json"

    raw_data_str = json.dumps(data, indent=2)

    # Загружаем в S3
    boto_client.put_object(
        Bucket="ingests",
        Key=object_key,
        Body=raw_data_str,
        ContentType='application/json',
    )
