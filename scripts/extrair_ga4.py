"""
Extrator GA4 (Google Analytics 4) Data API
Gera 4 CSVs em Dados/GA4/

Requer:
  - google-analytics-data>=0.18.0
  - Service Account JSON (base64 em GA4_SERVICE_ACCOUNT_JSON)

Uso:
  python scripts/extrair_ga4.py [--dias 90]
"""

import os
import sys
import json
import base64
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest, Dimension, Metric, DateRange,
    )
    from google.oauth2.service_account import Credentials
except ImportError:
    print("[ERRO] google-analytics-data nao instalado. pip install google-analytics-data>=0.18.0")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "GA4"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_client():
    """Cria cliente GA4 a partir de Service Account JSON (base64)."""
    sa_json_b64 = os.environ["GA4_SERVICE_ACCOUNT_JSON"]
    sa_info = json.loads(base64.b64decode(sa_json_b64))
    credentials = Credentials.from_service_account_info(sa_info)
    return BetaAnalyticsDataClient(credentials=credentials)


def run_report(client, property_id, dimensions, metrics, data_inicio, data_fim):
    """Executa report GA4 e retorna DataFrame."""
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=data_inicio, end_date=data_fim)],
        limit=100000,
    )

    response = client.run_report(request)

    rows = []
    for row in response.rows:
        r = {}
        for i, dim in enumerate(dimensions):
            r[dim] = row.dimension_values[i].value
        for i, met in enumerate(metrics):
            val = row.metric_values[i].value
            try:
                r[met] = float(val)
            except ValueError:
                r[met] = val
        rows.append(r)

    return pd.DataFrame(rows)


def extrair_sessoes_por_fonte(client, property_id, data_inicio, data_fim):
    """Sessoes por fonte/meio/campanha (atribuicao UTM)."""
    df = run_report(
        client, property_id,
        dimensions=["date", "sessionSource", "sessionMedium", "sessionCampaignName"],
        metrics=["sessions", "totalUsers", "bounceRate",
                 "averageSessionDuration", "screenPageViews",
                 "conversions", "totalRevenue"],
        data_inicio=data_inicio, data_fim=data_fim,
    )
    df.to_csv(OUTPUT_DIR / "sessoes_por_fonte.csv", index=False, encoding='utf-8-sig')
    print(f"  [GA4] sessoes_por_fonte.csv: {len(df)} linhas")
    return df


def extrair_conversoes(client, property_id, data_inicio, data_fim):
    """Conversoes por evento e fonte."""
    df = run_report(
        client, property_id,
        dimensions=["date", "eventName", "sessionSource", "sessionMedium"],
        metrics=["eventCount", "totalRevenue", "totalUsers"],
        data_inicio=data_inicio, data_fim=data_fim,
    )
    df.to_csv(OUTPUT_DIR / "conversoes.csv", index=False, encoding='utf-8-sig')
    print(f"  [GA4] conversoes.csv: {len(df)} linhas")
    return df


def extrair_landing_pages(client, property_id, data_inicio, data_fim):
    """Landing pages com melhor conversao."""
    df = run_report(
        client, property_id,
        dimensions=["date", "landingPage", "sessionSource", "sessionMedium"],
        metrics=["sessions", "totalUsers", "bounceRate",
                 "averageSessionDuration", "conversions", "totalRevenue"],
        data_inicio=data_inicio, data_fim=data_fim,
    )
    df.to_csv(OUTPUT_DIR / "landing_pages.csv", index=False, encoding='utf-8-sig')
    print(f"  [GA4] landing_pages.csv: {len(df)} linhas")
    return df


def extrair_diario(client, property_id, data_inicio, data_fim):
    """Metricas agregadas por dia."""
    df = run_report(
        client, property_id,
        dimensions=["date"],
        metrics=["sessions", "totalUsers", "newUsers", "bounceRate",
                 "averageSessionDuration", "screenPageViews",
                 "conversions", "totalRevenue"],
        data_inicio=data_inicio, data_fim=data_fim,
    )
    df.to_csv(OUTPUT_DIR / "diario.csv", index=False, encoding='utf-8-sig')
    print(f"  [GA4] diario.csv: {len(df)} linhas")
    return df


def extrair_dispositivos(client, property_id, data_inicio, data_fim):
    """Sessoes por dispositivo e navegador."""
    df = run_report(
        client, property_id,
        dimensions=["date", "deviceCategory", "browser", "operatingSystem"],
        metrics=["sessions", "totalUsers", "bounceRate",
                 "averageSessionDuration", "conversions", "totalRevenue"],
        data_inicio=data_inicio, data_fim=data_fim,
    )
    df.to_csv(OUTPUT_DIR / "dispositivos.csv", index=False, encoding='utf-8-sig')
    print(f"  [GA4] dispositivos.csv: {len(df)} linhas")
    return df


def extrair_geografico(client, property_id, data_inicio, data_fim):
    """Sessoes por cidade e estado."""
    df = run_report(
        client, property_id,
        dimensions=["date", "city", "region", "country"],
        metrics=["sessions", "totalUsers", "bounceRate",
                 "averageSessionDuration", "conversions", "totalRevenue"],
        data_inicio=data_inicio, data_fim=data_fim,
    )
    df.to_csv(OUTPUT_DIR / "geografico.csv", index=False, encoding='utf-8-sig')
    print(f"  [GA4] geografico.csv: {len(df)} linhas")
    return df


def main():
    parser = argparse.ArgumentParser(description="Extrator GA4")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    print(f"[GA4] Extraindo de {data_inicio} a {data_fim}...")

    client = get_client()
    property_id = os.environ["GA4_PROPERTY_ID"]

    extrair_sessoes_por_fonte(client, property_id, data_inicio, data_fim)
    extrair_conversoes(client, property_id, data_inicio, data_fim)
    extrair_landing_pages(client, property_id, data_inicio, data_fim)
    extrair_diario(client, property_id, data_inicio, data_fim)
    extrair_dispositivos(client, property_id, data_inicio, data_fim)
    extrair_geografico(client, property_id, data_inicio, data_fim)

    print("[GA4] Extracao concluida!")


if __name__ == "__main__":
    main()
