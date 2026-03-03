"""
Extrator TikTok Ads Marketing API
Gera 5 CSVs em Dados/TikTok_Ads/

Requer:
  - requests
  - Access Token + Advertiser ID do TikTok Business Center

Uso:
  python scripts/extrair_tiktok_ads.py [--dias 90]
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "TikTok_Ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = "https://business-api.tiktok.com/open_api/v1.3"


def get_headers():
    return {"Access-Token": os.environ["TIKTOK_ACCESS_TOKEN"]}


def get_advertiser_id():
    return os.environ["TIKTOK_ADVERTISER_ID"]


def fetch_report(data_inicio, data_fim, dimensions, metrics, nome_arquivo):
    """Busca relatorio via TikTok Reporting API."""
    url = f"{API_BASE}/report/integrated/get/"
    advertiser_id = get_advertiser_id()

    all_rows = []
    page = 1
    page_size = 1000

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_CAMPAIGN",
            "dimensions": str(dimensions),
            "metrics": str(metrics),
            "start_date": data_inicio,
            "end_date": data_fim,
            "page": page,
            "page_size": page_size,
            "lifetime": False,
        }

        resp = requests.get(url, headers=get_headers(), params=params, timeout=60)
        data = resp.json()

        if data.get("code") != 0:
            print(f"  [TikTok] Erro em {nome_arquivo}: {data.get('message', 'Desconhecido')}")
            break

        rows = data.get("data", {}).get("list", [])
        if not rows:
            break

        for row in rows:
            dims = row.get("dimensions", {})
            mets = row.get("metrics", {})
            all_rows.append({**dims, **mets})

        page_info = data.get("data", {}).get("page_info", {})
        total = page_info.get("total_number", 0)
        if page * page_size >= total:
            break
        page += 1

    df = pd.DataFrame(all_rows)

    # Converter colunas numericas
    for col in df.columns:
        if col not in ['campaign_name', 'campaign_id', 'stat_time_day', 'gender', 'age', 'ac']:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except (ValueError, TypeError):
                pass

    df.to_csv(OUTPUT_DIR / f"{nome_arquivo}.csv", index=False, encoding='utf-8-sig')
    print(f"  [TikTok] {nome_arquivo}.csv: {len(df)} linhas")
    return df


def main():
    parser = argparse.ArgumentParser(description="Extrator TikTok Ads")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    print(f"[TikTok Ads] Extraindo de {data_inicio} a {data_fim}...")

    metrics_base = [
        "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
        "conversion", "cost_per_conversion", "conversion_rate",
        "total_complete_payment_rate",
    ]
    metrics_video = [
        "video_play_actions", "video_watched_2s", "video_watched_6s",
        "average_video_play", "average_video_play_per_user",
        "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
        "engaged_view", "engaged_view_15s",
    ]
    metrics_engagement = [
        "likes", "comments", "shares", "follows",
        "clicks_on_music_disc", "profile_visits",
    ]

    # 1. Campanhas diarias
    fetch_report(
        data_inicio, data_fim,
        dimensions=["campaign_id", "stat_time_day"],
        metrics=metrics_base + metrics_engagement,
        nome_arquivo="campanhas",
    )

    # 2. Video engagement
    fetch_report(
        data_inicio, data_fim,
        dimensions=["campaign_id", "stat_time_day"],
        metrics=metrics_base + metrics_video,
        nome_arquivo="video_engagement",
    )

    # 3. Demografico por idade
    fetch_report(
        data_inicio, data_fim,
        dimensions=["campaign_id", "age"],
        metrics=metrics_base,
        nome_arquivo="demografico_idade",
    )

    # 4. Demografico por genero
    fetch_report(
        data_inicio, data_fim,
        dimensions=["campaign_id", "gender"],
        metrics=metrics_base,
        nome_arquivo="demografico_genero",
    )

    # 5. Diario agregado (sem campaign_id para totais)
    fetch_report(
        data_inicio, data_fim,
        dimensions=["stat_time_day"],
        metrics=metrics_base + metrics_video + metrics_engagement,
        nome_arquivo="diario",
    )

    print("[TikTok Ads] Extracao concluida!")


if __name__ == "__main__":
    main()
