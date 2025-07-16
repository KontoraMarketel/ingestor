import json

import requests
from botocore.client import BaseClient


def fetch_all_cards(api_token, boto_client: BaseClient, prefix: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    limit = 100
    cursor = None
    all_cards = []

    while True:
        payload = {
            "settings": {
                "cursor": {"limit": limit},
                "filter": {"withPhoto": -1}
            }
        }

        if cursor:
            payload["settings"]["cursor"].update(cursor)

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            raise Exception(
                "Invalid response from WB API",
                f"response: {response.text}",
                f"status_code: {response.status_code}"
            )

        raw_data = response.json()

        cards = raw_data.get("cards", [])
        cursor_data = raw_data.get("cursor", {})
        total = cursor_data.get("total", 0)

        all_cards.extend(cards)

        if total == 0:
            break

        cursor = {
            "updatedAt": cursor_data.get("updatedAt"),
            "nmID": cursor_data.get("nmID")
        }

    # Формируем имя файла
    object_key = f"{prefix}all_cards.json"

    raw_data_str = json.dumps(all_cards, indent=2)

    # Загружаем в S3
    boto_client.put_object(
        Bucket="ingests",
        Key=object_key,
        Body=raw_data_str,
        ContentType='application/json',
    )
