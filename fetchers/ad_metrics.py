import json

import requests


def fetch_ad_metrics(api_token, yesterday: str):
    headers = {
        "Authorization": api_token
    }
    url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    campaign_ids = []
    for group in response.json().get("adverts", []):
        for advert in group.get("advert_list", []):
            campaign_ids.append(advert.get("advertId"))

    body = [{"id": cid, "dates": [yesterday]} for cid in campaign_ids]

    url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()

    # Формируем имя файла
    filename = "ad_metrics.json"

    raw_data_str = json.dumps(response.json(), indent=2)
    return filename, raw_data_str
