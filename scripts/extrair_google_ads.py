"""
Extrator Google Ads API v23 — Multi-Customer (1 MCC, N contas)
Gera 6 CSVs em Dados/Google_Ads/ (todos com coluna 'shopping')

Requer:
  - google-ads>=24.0.0
  - OAuth2 (developer token + refresh token + MCC)
  - GOOGLE_ADS_CUSTOMER_IDS (JSON): {"BS": "1234567890", "NK": "0987654321", ...}

Uso:
  python scripts/extrair_google_ads.py [--dias 90]
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    from google.ads.googleads.client import GoogleAdsClient
except ImportError:
    print("[ERRO] google-ads nao instalado. pip install google-ads>=24.0.0")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "Google_Ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SHOPPING_NOMES = {
    "CS": "Continente Shopping",
    "BS": "Balneario Shopping",
    "NK": "Neumarkt Shopping",
    "NR": "Norte Shopping",
    "GS": "Garten Shopping",
    "NS": "Nacoes Shopping",
    "AJ": "Almeida Junior",
}


def get_client():
    """Cria cliente Google Ads a partir de variaveis de ambiente."""
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"],
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def get_customer_ids():
    """Retorna dict {sigla: customer_id} de todas as contas."""
    config_json = os.environ.get("GOOGLE_ADS_CUSTOMER_IDS", "")
    if config_json:
        return json.loads(config_json)
    # Fallback: customer_id unico
    cid = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").replace('-', '')
    if cid:
        return {"GERAL": cid}
    print("[ERRO] GOOGLE_ADS_CUSTOMER_IDS ou GOOGLE_ADS_CUSTOMER_ID nao configurados")
    sys.exit(1)


def query_google_ads(client, customer_id, query):
    """Executa GAQL query e retorna lista de rows."""
    service = client.get_service("GoogleAdsService")
    rows = []
    response = service.search_stream(customer_id=customer_id, query=query)
    for batch in response:
        for row in batch.results:
            rows.append(row)
    return rows


def extrair_campanhas(client, customer_id, shopping_sigla, data_inicio, data_fim):
    query = f"""
        SELECT
            campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc,
            metrics.cost_micros, metrics.conversions, metrics.conversions_value,
            metrics.cost_per_conversion, metrics.search_impression_share,
            metrics.view_through_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
            AND campaign.status != 'REMOVED'
        ORDER BY segments.date DESC
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        custo = r.metrics.cost_micros / 1_000_000
        data.append({
            'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
            'shopping_sigla': shopping_sigla,
            'campanha_id': r.campaign.id,
            'campanha': r.campaign.name,
            'status': r.campaign.status.name,
            'tipo_canal': r.campaign.advertising_channel_type.name,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'ctr': r.metrics.ctr,
            'cpc_medio': r.metrics.average_cpc / 1_000_000,
            'custo': custo,
            'conversoes': r.metrics.conversions,
            'valor_conversoes': r.metrics.conversions_value,
            'cpa': r.metrics.cost_per_conversion / 1_000_000 if r.metrics.cost_per_conversion else 0,
            'impression_share': r.metrics.search_impression_share,
            'view_through_conv': r.metrics.view_through_conversions,
        })
    return data


def extrair_keywords(client, customer_id, shopping_sigla, data_inicio, data_fim):
    query = f"""
        SELECT
            ad_group.name, ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.quality_info.quality_score,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.ctr,
            metrics.average_cpc, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value
        FROM keyword_view
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        ORDER BY metrics.cost_micros DESC
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        qs = r.ad_group_criterion.quality_info.quality_score
        data.append({
            'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
            'shopping_sigla': shopping_sigla,
            'grupo_anuncio': r.ad_group.name,
            'keyword': r.ad_group_criterion.keyword.text,
            'match_type': r.ad_group_criterion.keyword.match_type.name,
            'quality_score': qs if qs > 0 else None,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'ctr': r.metrics.ctr,
            'cpc_medio': r.metrics.average_cpc / 1_000_000,
            'custo': r.metrics.cost_micros / 1_000_000,
            'conversoes': r.metrics.conversions,
            'valor_conversoes': r.metrics.conversions_value,
        })
    return data


def extrair_demografico(client, customer_id, shopping_sigla, data_inicio, data_fim):
    data = []
    for view, tipo in [('age_range_view', 'faixa_etaria'), ('gender_view', 'genero')]:
        resource = 'age_range_view.resource_name' if view == 'age_range_view' else 'gender_view.resource_name'
        query = f"""
            SELECT
                {resource}, segments.date,
                metrics.impressions, metrics.clicks, metrics.cost_micros,
                metrics.conversions, metrics.conversions_value
            FROM {view}
            WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
        rows = query_google_ads(client, customer_id, query)
        for r in rows:
            res_name = getattr(r, view).resource_name
            segmento = res_name.split('~')[-1] if '~' in res_name else 'Desconhecido'
            data.append({
                'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
                'shopping_sigla': shopping_sigla,
                'tipo': tipo,
                'segmento': segmento,
                'data': r.segments.date,
                'impressoes': r.metrics.impressions,
                'cliques': r.metrics.clicks,
                'custo': r.metrics.cost_micros / 1_000_000,
                'conversoes': r.metrics.conversions,
                'valor_conversoes': r.metrics.conversions_value,
            })
    return data


def extrair_geografico(client, customer_id, shopping_sigla, data_inicio, data_fim):
    query = f"""
        SELECT
            campaign.name, segments.date,
            segments.geo_target_city, segments.geo_target_region,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value
        FROM geographic_view
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        data.append({
            'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
            'shopping_sigla': shopping_sigla,
            'campanha': r.campaign.name,
            'cidade': r.segments.geo_target_city,
            'estado': r.segments.geo_target_region,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'custo': r.metrics.cost_micros / 1_000_000,
            'conversoes': r.metrics.conversions,
            'valor_conversoes': r.metrics.conversions_value,
        })
    return data


def extrair_dispositivos(client, customer_id, shopping_sigla, data_inicio, data_fim):
    query = f"""
        SELECT
            campaign.name, segments.device, segments.date,
            metrics.impressions, metrics.clicks, metrics.ctr,
            metrics.cost_micros, metrics.conversions, metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
            AND campaign.status != 'REMOVED'
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        data.append({
            'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
            'shopping_sigla': shopping_sigla,
            'campanha': r.campaign.name,
            'dispositivo': r.segments.device.name,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'ctr': r.metrics.ctr,
            'custo': r.metrics.cost_micros / 1_000_000,
            'conversoes': r.metrics.conversions,
            'valor_conversoes': r.metrics.conversions_value,
        })
    return data


def extrair_diario(client, customer_id, shopping_sigla, data_inicio, data_fim):
    query = f"""
        SELECT
            segments.date,
            metrics.impressions, metrics.clicks, metrics.ctr,
            metrics.average_cpc, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value,
            metrics.search_impression_share
        FROM customer
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        ORDER BY segments.date
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        custo = r.metrics.cost_micros / 1_000_000
        conversoes = r.metrics.conversions
        valor = r.metrics.conversions_value
        data.append({
            'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
            'shopping_sigla': shopping_sigla,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'ctr': r.metrics.ctr,
            'cpc_medio': r.metrics.average_cpc / 1_000_000,
            'custo': custo,
            'conversoes': conversoes,
            'valor_conversoes': valor,
            'roas': valor / custo if custo > 0 else 0,
            'cpa': custo / conversoes if conversoes > 0 else 0,
            'impression_share': r.metrics.search_impression_share,
        })
    return data


def main():
    parser = argparse.ArgumentParser(description="Extrator Google Ads (Multi-Customer)")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    client = get_client()
    customer_ids = get_customer_ids()

    print(f"[Google Ads] Extraindo de {data_inicio} a {data_fim} para {len(customer_ids)} conta(s)...")

    extratores = {
        'campanhas': extrair_campanhas,
        'keywords': extrair_keywords,
        'demografico': extrair_demografico,
        'geografico': extrair_geografico,
        'dispositivos': extrair_dispositivos,
        'diario': extrair_diario,
    }

    for nome_csv, func_extrair in extratores.items():
        all_data = []
        for sigla, cid in customer_ids.items():
            cid_clean = cid.replace('-', '')
            print(f"  [Google Ads/{sigla}] Extraindo {nome_csv}...")
            try:
                rows = func_extrair(client, cid_clean, sigla, data_inicio, data_fim)
                all_data.extend(rows)
            except Exception as e:
                print(f"  [Google Ads/{sigla}] Erro em {nome_csv}: {e}")

        df = pd.DataFrame(all_data)
        df.to_csv(OUTPUT_DIR / f"{nome_csv}.csv", index=False, encoding='utf-8-sig')
        print(f"  [Google Ads] {nome_csv}.csv: {len(df)} linhas ({len(customer_ids)} contas)")

    print("[Google Ads] Extracao concluida!")


if __name__ == "__main__":
    main()
