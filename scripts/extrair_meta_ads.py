"""
Extrator Meta Ads (Facebook + Instagram) Marketing API v22
Gera 7 CSVs em Dados/Meta_Ads/

Requer:
  - facebook-business>=19.0.0
  - System User Token (long-lived via Business Manager)

Uso:
  python scripts/extrair_meta_ads.py [--dias 90]
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
except ImportError:
    print("[ERRO] facebook-business nao instalado. pip install facebook-business>=19.0.0")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "Meta_Ads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def init_api():
    """Inicializa a API do Meta Ads."""
    FacebookAdsApi.init(
        app_id=os.environ["META_APP_ID"],
        app_secret=os.environ["META_APP_SECRET"],
        access_token=os.environ["META_ACCESS_TOKEN"],
    )
    return AdAccount(os.environ["META_AD_ACCOUNT_ID"])


def extrair_insights(account, data_inicio, data_fim, breakdowns=None, nome_arquivo="campanhas"):
    """Extrai insights generico com breakdowns opcionais."""
    fields = [
        'campaign_name', 'campaign_id', 'objective',
        'impressions', 'reach', 'frequency',
        'clicks', 'ctr', 'cpc', 'cpm',
        'spend',
        'actions', 'action_values', 'cost_per_action_type',
        'video_avg_time_watched_actions',
        'video_p25_watched_actions', 'video_p50_watched_actions',
        'video_p75_watched_actions', 'video_p100_watched_actions',
        'estimated_ad_recall_lift',
    ]

    params = {
        'time_range': {'since': data_inicio, 'until': data_fim},
        'time_increment': 1,  # diario
        'level': 'campaign',
        'filtering': [{'field': 'campaign.delivery_info', 'operator': 'IN', 'value': ['active', 'completed', 'inactive']}],
    }
    if breakdowns:
        params['breakdowns'] = breakdowns

    try:
        insights = account.get_insights(fields=fields, params=params)
    except Exception as e:
        print(f"  [Meta Ads] Erro em {nome_arquivo}: {e}")
        return pd.DataFrame()

    data = []
    for row in insights:
        row_dict = dict(row)

        # Deserializar actions[] para colunas individuais
        acoes = {}
        for action_field in ['actions', 'action_values', 'cost_per_action_type']:
            if action_field in row_dict and row_dict[action_field]:
                for action in row_dict[action_field]:
                    action_type = action.get('action_type', '')
                    prefix = '' if action_field == 'actions' else ('valor_' if 'value' in action_field else 'custo_')
                    acoes[f'{prefix}{action_type}'] = float(action.get('value', 0))

        # Video metrics
        video = {}
        for vf in ['video_p25_watched_actions', 'video_p50_watched_actions',
                    'video_p75_watched_actions', 'video_p100_watched_actions']:
            if vf in row_dict and row_dict[vf]:
                for v in row_dict[vf]:
                    quartil = vf.replace('video_', '').replace('_watched_actions', '')
                    video[f'video_{quartil}'] = float(v.get('value', 0))

        registro = {
            'campanha': row_dict.get('campaign_name', ''),
            'campanha_id': row_dict.get('campaign_id', ''),
            'objetivo': row_dict.get('objective', ''),
            'data': row_dict.get('date_start', ''),
            'impressoes': int(row_dict.get('impressions', 0)),
            'alcance': int(row_dict.get('reach', 0)),
            'frequencia': float(row_dict.get('frequency', 0)),
            'cliques': int(row_dict.get('clicks', 0)),
            'ctr': float(row_dict.get('ctr', 0)),
            'cpc': float(row_dict.get('cpc', 0)),
            'cpm': float(row_dict.get('cpm', 0)),
            'custo': float(row_dict.get('spend', 0)),
            'ad_recall_lift': float(row_dict.get('estimated_ad_recall_lift', 0) or 0),
        }

        # Adicionar acoes desserializadas
        for k in ['link_click', 'landing_page_view', 'lead', 'purchase',
                   'add_to_cart', 'initiate_checkout', 'complete_registration']:
            registro[k] = acoes.get(k, 0)
            registro[f'valor_{k}'] = acoes.get(f'valor_{k}', 0)
            registro[f'custo_{k}'] = acoes.get(f'custo_{k}', 0)

        # Video
        registro.update(video)

        # Breakdowns
        if breakdowns:
            for bd in breakdowns:
                registro[bd] = row_dict.get(bd, '')

        data.append(registro)

    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_DIR / f"{nome_arquivo}.csv", index=False, encoding='utf-8-sig')
    print(f"  [Meta Ads] {nome_arquivo}.csv: {len(df)} linhas")
    return df


def main():
    parser = argparse.ArgumentParser(description="Extrator Meta Ads")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    print(f"[Meta Ads] Extraindo de {data_inicio} a {data_fim}...")

    account = init_api()

    # 1. Campanhas (sem breakdown)
    extrair_insights(account, data_inicio, data_fim, nome_arquivo="campanhas")

    # 2. Por plataforma (Facebook / Instagram / Messenger / Audience Network)
    extrair_insights(account, data_inicio, data_fim,
                     breakdowns=['publisher_platform'], nome_arquivo="plataforma")

    # 3. Por posicionamento (Feed / Stories / Reels / Explore)
    extrair_insights(account, data_inicio, data_fim,
                     breakdowns=['publisher_platform', 'platform_position'],
                     nome_arquivo="posicionamento")

    # 4. Demografico - Faixa Etaria
    extrair_insights(account, data_inicio, data_fim,
                     breakdowns=['age'], nome_arquivo="demografico_idade")

    # 5. Demografico - Genero
    extrair_insights(account, data_inicio, data_fim,
                     breakdowns=['gender'], nome_arquivo="demografico_genero")

    # 6. Demografico cruzado
    extrair_insights(account, data_inicio, data_fim,
                     breakdowns=['age', 'gender'], nome_arquivo="demografico_cruzado")

    # 7. Video performance
    extrair_insights(account, data_inicio, data_fim, nome_arquivo="video")

    print("[Meta Ads] Extracao concluida!")


if __name__ == "__main__":
    main()
