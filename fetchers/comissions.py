import json

import requests


def fetch_commissions(api_token):
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
    filename = "comissions.json"

    raw_data_str = json.dumps(data, indent=2)
    return filename, raw_data_str
