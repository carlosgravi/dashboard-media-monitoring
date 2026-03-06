"""
Extrator Meta Ads (Facebook + Instagram) Marketing API v22
Gera 7 CSVs em Dados/Meta_Ads/ (com coluna shopping)

Multi-conta: META_ADS_CONFIG (JSON) com ad_account_id por shopping
Requer:
  - facebook-business>=19.0.0
  - System User Token (long-lived via Business Manager)

Uso:
  python scripts/extrair_meta_ads.py [--dias 90]
"""

import os
import sys
import json
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

# Mapeamento de contas por shopping/entidade
# META_ADS_CONFIG = {"CS": "act_xxx", "BS": "act_yyy", ...}
META_ADS_CONFIG = {
    "CS": "act_2649388528700191",
    "BS": "act_207729971285382",
    "NK": "act_342862273862537",
    "NR": "act_242349187718819",
    "GS": "act_1431101650367906",
    "NS": "act_570612793930568",
    "AJ_Realty": "act_3778410928887624",
    "CS_Residences": "act_1239656477887657",
}


def init_api():
    """Inicializa a API do Meta Ads e retorna dict de accounts."""
    FacebookAdsApi.init(
        app_id=os.environ["META_ADS_APP_ID"],
        app_secret=os.environ["META_ADS_APP_SECRET"],
        access_token=os.environ["META_ADS_ACCESS_TOKEN"],
    )
    config = json.loads(os.environ.get("META_ADS_CONFIG", "{}"))
    if not config:
        config = META_ADS_CONFIG
    return {sigla: AdAccount(act_id) for sigla, act_id in config.items()}


def extrair_insights(account, data_inicio, data_fim, breakdowns=None, nome_arquivo="campanhas", shopping=""):
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

        # Identificar shopping/entidade
        registro['shopping'] = shopping

        data.append(registro)

    return pd.DataFrame(data)


def extrair_todas_contas(accounts, data_inicio, data_fim):
    """Extrai insights de todas as contas e consolida por tipo de CSV."""
    tipos_extracao = [
        {"nome": "campanhas", "breakdowns": None},
        {"nome": "plataforma", "breakdowns": ['publisher_platform']},
        {"nome": "posicionamento", "breakdowns": ['publisher_platform', 'platform_position']},
        {"nome": "demografico_idade", "breakdowns": ['age']},
        {"nome": "demografico_genero", "breakdowns": ['gender']},
        {"nome": "demografico_cruzado", "breakdowns": ['age', 'gender']},
        {"nome": "dispositivo", "breakdowns": ['device_platform']},
        {"nome": "video", "breakdowns": None},
    ]

    for tipo in tipos_extracao:
        dfs = []
        for sigla, account in accounts.items():
            print(f"  [Meta Ads] {sigla} → {tipo['nome']}...")
            df = extrair_insights(
                account, data_inicio, data_fim,
                breakdowns=tipo['breakdowns'],
                nome_arquivo=tipo['nome'],
                shopping=sigla,
            )
            if not df.empty:
                dfs.append(df)

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            df_final.to_csv(OUTPUT_DIR / f"{tipo['nome']}.csv", index=False, encoding='utf-8-sig')
            print(f"  [Meta Ads] {tipo['nome']}.csv: {len(df_final)} linhas ({len(dfs)} contas)")
        else:
            print(f"  [Meta Ads] {tipo['nome']}.csv: sem dados")


def main():
    parser = argparse.ArgumentParser(description="Extrator Meta Ads")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    print(f"[Meta Ads] Extraindo de {data_inicio} a {data_fim} (8 contas)...")

    accounts = init_api()
    extrair_todas_contas(accounts, data_inicio, data_fim)

    print("[Meta Ads] Extracao concluida!")


if __name__ == "__main__":
    main()
