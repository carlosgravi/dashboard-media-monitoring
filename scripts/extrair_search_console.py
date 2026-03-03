"""
Extrator Google Search Console
Gera 3 CSVs em Dados/Search_Console/

Requer:
  - google-api-python-client>=2.100.0
  - Mesma Service Account do GA4

Uso:
  python scripts/extrair_search_console.py [--dias 90]
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
    from googleapiclient.discovery import build
    from google.oauth2.service_account import Credentials
except ImportError:
    print("[ERRO] google-api-python-client nao instalado. pip install google-api-python-client>=2.100.0")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "Search_Console"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']


def get_service():
    """Cria servico Search Console usando Service Account."""
    sa_json_b64 = os.environ["GA4_SERVICE_ACCOUNT_JSON"]  # mesma SA do GA4
    sa_info = json.loads(base64.b64decode(sa_json_b64))
    credentials = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build('searchconsole', 'v1', credentials=credentials)


def fetch_search_analytics(service, site_url, data_inicio, data_fim, dimensions, row_limit=25000):
    """Busca dados do Search Analytics."""
    all_rows = []
    start_row = 0

    while True:
        request = {
            'startDate': data_inicio,
            'endDate': data_fim,
            'dimensions': dimensions,
            'rowLimit': row_limit,
            'startRow': start_row,
        }
        response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        rows = response.get('rows', [])
        if not rows:
            break

        for row in rows:
            keys = row.get('keys', [])
            r = {}
            for i, dim in enumerate(dimensions):
                r[dim] = keys[i] if i < len(keys) else ''
            r['cliques'] = row.get('clicks', 0)
            r['impressoes'] = row.get('impressions', 0)
            r['ctr'] = row.get('ctr', 0)
            r['posicao'] = row.get('position', 0)
            all_rows.append(r)

        if len(rows) < row_limit:
            break
        start_row += row_limit

    return pd.DataFrame(all_rows)


def main():
    parser = argparse.ArgumentParser(description="Extrator Search Console")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    # Search Console tem delay de ~3 dias
    data_fim = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    print(f"[Search Console] Extraindo de {data_inicio} a {data_fim}...")

    service = get_service()
    site_url = os.environ.get("SEARCH_CONSOLE_SITE_URL", "https://www.almeidajunior.com.br")

    # 1. Consultas por data
    df = fetch_search_analytics(service, site_url, data_inicio, data_fim,
                                dimensions=['date', 'query'])
    df.to_csv(OUTPUT_DIR / "consultas.csv", index=False, encoding='utf-8-sig')
    print(f"  [Search Console] consultas.csv: {len(df)} linhas")

    # 2. Paginas por data
    df = fetch_search_analytics(service, site_url, data_inicio, data_fim,
                                dimensions=['date', 'page'])
    df.to_csv(OUTPUT_DIR / "paginas.csv", index=False, encoding='utf-8-sig')
    print(f"  [Search Console] paginas.csv: {len(df)} linhas")

    # 3. Consultas agregadas (oportunidades SEO)
    df = fetch_search_analytics(service, site_url, data_inicio, data_fim,
                                dimensions=['query'])
    # Marcar oportunidades: alto impressoes + baixo CTR
    if not df.empty and 'impressoes' in df.columns and 'ctr' in df.columns:
        mediana_imp = df['impressoes'].median()
        df['oportunidade_seo'] = (df['impressoes'] > mediana_imp) & (df['ctr'] < 0.03)
    df.to_csv(OUTPUT_DIR / "oportunidades_seo.csv", index=False, encoding='utf-8-sig')
    print(f"  [Search Console] oportunidades_seo.csv: {len(df)} linhas")

    print("[Search Console] Extracao concluida!")


if __name__ == "__main__":
    main()
