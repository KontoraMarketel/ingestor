import json

import requests


def fetch_fbw_incomes(api_token, yesterday: str):
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"
    headers = {"Authorization": api_token}

    all_incomes = []
    datefrom = yesterday
    while True:
        params = {"dateFrom": datefrom}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        if not response.json():
            break

        res = response.json()
        all_incomes.extend(res)
        datefrom = res[-1]["lastChangeDate"]

        # Формируем имя файла
    filename = "fbw_incomes.json"

    raw_data_str = json.dumps(all_incomes, indent=2)
    return filename, raw_data_str
