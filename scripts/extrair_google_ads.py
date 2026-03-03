"""
Extrator Google Ads API v23
Gera 6 CSVs em Dados/Google_Ads/

Requer:
  - google-ads>=24.0.0
  - Credenciais OAuth2 (developer token + refresh token + MCC)

Uso:
  python scripts/extrair_google_ads.py [--dias 90]
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    from google.ads.googleads.client import GoogleAdsClient
except ImportError:
    print("[ERRO] google-ads nao instalado. pip install google-ads>=24.0.0")
    sys.exit(1)

# Diretorio de saida
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "Google_Ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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


def query_google_ads(client, customer_id, query):
    """Executa GAQL query e retorna lista de dicts."""
    service = client.get_service("GoogleAdsService")
    rows = []
    response = service.search_stream(customer_id=customer_id, query=query)
    for batch in response:
        for row in batch.results:
            rows.append(row)
    return rows


def extrair_campanhas(client, customer_id, data_inicio, data_fim):
    """Extrai performance por campanha."""
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.average_cpc,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_per_conversion,
            metrics.search_impression_share,
            metrics.view_through_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
            AND campaign.status != 'REMOVED'
        ORDER BY segments.date DESC
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        data.append({
            'campanha_id': r.campaign.id,
            'campanha': r.campaign.name,
            'status': r.campaign.status.name,
            'tipo_canal': r.campaign.advertising_channel_type.name,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'ctr': r.metrics.ctr,
            'cpc_medio': r.metrics.average_cpc / 1_000_000,
            'custo': r.metrics.cost_micros / 1_000_000,
            'conversoes': r.metrics.conversions,
            'valor_conversoes': r.metrics.conversions_value,
            'cpa': r.metrics.cost_per_conversion / 1_000_000 if r.metrics.cost_per_conversion else 0,
            'impression_share': r.metrics.search_impression_share,
            'view_through_conv': r.metrics.view_through_conversions,
        })
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_DIR / "campanhas.csv", index=False, encoding='utf-8-sig')
    print(f"  [Google Ads] campanhas.csv: {len(df)} linhas")
    return df


def extrair_keywords(client, customer_id, data_inicio, data_fim):
    """Extrai performance por keyword."""
    query = f"""
        SELECT
            ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.quality_info.quality_score,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.average_cpc,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM keyword_view
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        ORDER BY metrics.cost_micros DESC
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        qs = r.ad_group_criterion.quality_info.quality_score
        data.append({
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
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_DIR / "keywords.csv", index=False, encoding='utf-8-sig')
    print(f"  [Google Ads] keywords.csv: {len(df)} linhas")
    return df


def extrair_demografico(client, customer_id, data_inicio, data_fim):
    """Extrai performance por faixa etaria e genero."""
    # Faixa etaria
    query_age = f"""
        SELECT
            ad_group.name,
            age_range_view.resource_name,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM age_range_view
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
    """
    # Genero
    query_gender = f"""
        SELECT
            ad_group.name,
            gender_view.resource_name,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM gender_view
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
    """
    rows_age = query_google_ads(client, customer_id, query_age)
    rows_gender = query_google_ads(client, customer_id, query_gender)

    data = []
    for r in rows_age:
        # Extrair faixa do resource_name
        faixa = r.age_range_view.resource_name.split('~')[-1] if '~' in r.age_range_view.resource_name else 'Desconhecido'
        data.append({
            'tipo': 'faixa_etaria',
            'segmento': faixa,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'custo': r.metrics.cost_micros / 1_000_000,
            'conversoes': r.metrics.conversions,
            'valor_conversoes': r.metrics.conversions_value,
        })
    for r in rows_gender:
        genero = r.gender_view.resource_name.split('~')[-1] if '~' in r.gender_view.resource_name else 'Desconhecido'
        data.append({
            'tipo': 'genero',
            'segmento': genero,
            'data': r.segments.date,
            'impressoes': r.metrics.impressions,
            'cliques': r.metrics.clicks,
            'custo': r.metrics.cost_micros / 1_000_000,
            'conversoes': r.metrics.conversions,
            'valor_conversoes': r.metrics.conversions_value,
        })
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_DIR / "demografico.csv", index=False, encoding='utf-8-sig')
    print(f"  [Google Ads] demografico.csv: {len(df)} linhas")
    return df


def extrair_geografico(client, customer_id, data_inicio, data_fim):
    """Extrai performance por localizacao geografica."""
    query = f"""
        SELECT
            geographic_view.country_criterion_id,
            geographic_view.location_type,
            campaign.name,
            segments.date,
            segments.geo_target_city,
            segments.geo_target_region,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM geographic_view
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        data.append({
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
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_DIR / "geografico.csv", index=False, encoding='utf-8-sig')
    print(f"  [Google Ads] geografico.csv: {len(df)} linhas")
    return df


def extrair_dispositivos(client, customer_id, data_inicio, data_fim):
    """Extrai performance por dispositivo."""
    query = f"""
        SELECT
            campaign.name,
            segments.device,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
            AND campaign.status != 'REMOVED'
    """
    rows = query_google_ads(client, customer_id, query)
    data = []
    for r in rows:
        data.append({
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
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_DIR / "dispositivos.csv", index=False, encoding='utf-8-sig')
    print(f"  [Google Ads] dispositivos.csv: {len(df)} linhas")
    return df


def extrair_diario(client, customer_id, data_inicio, data_fim):
    """Extrai metricas agregadas por dia (para tendencias)."""
    query = f"""
        SELECT
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.average_cpc,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
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
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_DIR / "diario.csv", index=False, encoding='utf-8-sig')
    print(f"  [Google Ads] diario.csv: {len(df)} linhas")
    return df


def main():
    parser = argparse.ArgumentParser(description="Extrator Google Ads")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    print(f"[Google Ads] Extraindo de {data_inicio} a {data_fim}...")

    client = get_client()
    customer_id = os.environ["GOOGLE_ADS_CUSTOMER_ID"].replace('-', '')

    extrair_campanhas(client, customer_id, data_inicio, data_fim)
    extrair_keywords(client, customer_id, data_inicio, data_fim)
    extrair_demografico(client, customer_id, data_inicio, data_fim)
    extrair_geografico(client, customer_id, data_inicio, data_fim)
    extrair_dispositivos(client, customer_id, data_inicio, data_fim)
    extrair_diario(client, customer_id, data_inicio, data_fim)

    print("[Google Ads] Extracao concluida!")


if __name__ == "__main__":
    main()
