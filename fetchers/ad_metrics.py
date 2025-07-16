import json
from time import sleep

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
    stats_data = response.json()

    batch_size = 50
    adverts = []
    for i in range(0, len(stats_data), batch_size):
        batch = stats_data[i:i + batch_size]
        adv_ids = [item['advertId'] for item in batch]
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
        response = requests.get(url, headers=headers, json=adv_ids)
        response.raise_for_status()
        adverts.extend(response.json())
        sleep(0.6)

    compiled_data = enrich_stats_with_metadata(stats_data, adverts)

    # Формируем имя файла
    filename = "ad_metrics.json"

    raw_data_str = json.dumps(compiled_data, indent=2)
    return filename, raw_data_str


def enrich_stats_with_metadata(stats: list, ads: list) -> list:
    # Индексируем список кампаний по advertId
    ads_by_id = {ad["advertId"]: ad for ad in ads}

    result = []
    for stat in stats:
        ad_id = stat.get("advertId")
        enriched = stat.copy()

        # Вставляем всю метаинформацию
        enriched["metadata"] = ads_by_id.get(ad_id)

        result.append(enriched)

    return result
