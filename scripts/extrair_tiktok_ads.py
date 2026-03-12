"""
Extrator TikTok Ads Marketing API — Multi-Shopping
Gera 5 CSVs em Dados/TikTok_Ads/ (todos com coluna 'shopping')

Requer:
  - requests
  - TIKTOK_ADS_CONFIG (JSON): {"BS": {"token": "x", "advertiser_id": "y"}, ...}

Uso:
  python scripts/extrair_tiktok_ads.py [--dias 90]
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "TikTok_Ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = "https://business-api.tiktok.com/open_api/v1.3"

SHOPPING_NOMES = {
    "CS": "Continente Shopping",
    "BS": "Balneario Shopping",
    "NK": "Neumarkt Shopping",
    "NR": "Norte Shopping",
    "GS": "Garten Shopping",
    "NS": "Nacoes Shopping",
}


def get_config():
    """Carrega configuracao multi-shopping do env."""
    config_json = os.environ.get("TIKTOK_ADS_CONFIG", "")
    if not config_json:
        # Fallback: token + advertiser_id unicos (retrocompativel)
        token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
        adv_id = os.environ.get("TIKTOK_ADVERTISER_ID", "")
        if token and adv_id:
            return {"GERAL": {"token": token, "advertiser_id": adv_id}}
        print("[ERRO] TIKTOK_ADS_CONFIG ou TIKTOK_ACCESS_TOKEN nao configurados")
        sys.exit(1)
    return json.loads(config_json)


def fetch_report(token, advertiser_id, data_inicio, data_fim, dimensions, metrics, shopping_sigla, data_level="AUCTION_CAMPAIGN", report_type="BASIC"):
    """Busca relatorio via TikTok Reporting API para 1 conta."""
    url = f"{API_BASE}/report/integrated/get/"
    headers = {"Access-Token": token}

    all_rows = []
    page = 1
    page_size = 1000

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "report_type": report_type,
            "data_level": data_level,
            "dimensions": json.dumps(dimensions),
            "metrics": json.dumps(metrics),
            "start_date": data_inicio,
            "end_date": data_fim,
            "page": page,
            "page_size": page_size,
            "lifetime": False,
        }

        resp = requests.get(url, headers=headers, params=params, timeout=60)
        data = resp.json()

        if data.get("code") != 0:
            print(f"  [TikTok/{shopping_sigla}] Erro: {data.get('message', 'Desconhecido')}")
            break

        rows = data.get("data", {}).get("list", [])
        if not rows:
            break

        for row in rows:
            dims = row.get("dimensions", {})
            mets = row.get("metrics", {})
            registro = {**dims, **mets}
            registro['shopping'] = SHOPPING_NOMES.get(shopping_sigla, shopping_sigla)
            registro['shopping_sigla'] = shopping_sigla
            all_rows.append(registro)

        page_info = data.get("data", {}).get("page_info", {})
        total = page_info.get("total_number", 0)
        if page * page_size >= total:
            break
        page += 1

    df = pd.DataFrame(all_rows)

    # Converter colunas numericas
    for col in df.columns:
        if col not in ['campaign_name', 'campaign_id', 'stat_time_day', 'gender', 'age', 'ac', 'shopping', 'shopping_sigla']:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except (ValueError, TypeError):
                pass

    return df


def main():
    parser = argparse.ArgumentParser(description="Extrator TikTok Ads (Multi-Shopping)")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    config = get_config()
    print(f"[TikTok Ads] Extraindo de {data_inicio} a {data_fim} para {len(config)} conta(s)...")

    metrics_base = [
        "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
        "conversion", "cost_per_conversion", "conversion_rate",
        "total_complete_payment_rate",
    ]
    metrics_audience = [
        "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
        "conversion", "cost_per_conversion", "conversion_rate",
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

    # Relatorios a extrair: (nome, dimensions, metrics, data_level)
    relatorios = [
        ("campanhas", ["campaign_id", "stat_time_day"], metrics_base + metrics_engagement, "AUCTION_CAMPAIGN", "BASIC"),
        ("video_engagement", ["campaign_id", "stat_time_day"], metrics_base + metrics_video, "AUCTION_CAMPAIGN", "BASIC"),
        ("demografico_idade", ["stat_time_day", "age"], metrics_audience, "AUCTION_ADVERTISER", "AUDIENCE"),
        ("demografico_genero", ["stat_time_day", "gender"], metrics_audience, "AUCTION_ADVERTISER", "AUDIENCE"),
        ("diario", ["stat_time_day"], metrics_base + metrics_video + metrics_engagement, "AUCTION_CAMPAIGN", "BASIC"),
    ]

    for nome_arquivo, dimensions, metrics, data_level, report_type in relatorios:
        dfs = []
        for sigla, creds in config.items():
            token = creds["token"]
            adv_id = creds["advertiser_id"]
            print(f"  [TikTok/{sigla}] Extraindo {nome_arquivo}...")
            df = fetch_report(token, adv_id, data_inicio, data_fim, dimensions, metrics, sigla, data_level, report_type)
            if not df.empty:
                dfs.append(df)

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
        else:
            df_final = pd.DataFrame()

        df_final.to_csv(OUTPUT_DIR / f"{nome_arquivo}.csv", index=False, encoding='utf-8-sig')
        print(f"  [TikTok] {nome_arquivo}.csv: {len(df_final)} linhas ({len(config)} shoppings)")

    print("[TikTok Ads] Extracao concluida!")


if __name__ == "__main__":
    main()
