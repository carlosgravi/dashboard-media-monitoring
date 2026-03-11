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
import signal
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

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


TIMEOUT_POR_CONTA = 120  # 2 minutos max por conta/tipo


class TimeoutException(Exception):
    pass


def _fetch_insights_with_timeout(account, fields, params, timeout=TIMEOUT_POR_CONTA):
    """Executa get_insights + paginacao com timeout via signal (Linux) ou ThreadPool (Windows)."""
    def _fetch():
        insights = account.get_insights(fields=fields, params=params)
        return list(insights)

    # signal.alarm so funciona em Linux (GitHub Actions)
    if hasattr(signal, 'SIGALRM'):
        def _handler(signum, frame):
            raise TimeoutException(f"Timeout ({timeout}s)")

        old_handler = signal.signal(signal.SIGALRM, _handler)
        signal.alarm(timeout)
        try:
            result = _fetch()
            signal.alarm(0)
            return result
        except TimeoutException:
            raise FuturesTimeoutError(f"Timeout ({timeout}s)")
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    else:
        # Fallback Windows
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_fetch)
            return future.result(timeout=timeout)


def extrair_insights(account, data_inicio, data_fim, breakdowns=None, nome_arquivo="campanhas", shopping="", time_increment=1):
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
        'time_increment': time_increment,
        'level': 'campaign',
        'filtering': [{'field': 'campaign.delivery_info', 'operator': 'IN', 'value': ['active', 'completed', 'inactive']}],
    }
    if breakdowns:
        params['breakdowns'] = breakdowns

    try:
        rows_list = _fetch_insights_with_timeout(account, fields, params)
    except FuturesTimeoutError:
        print(f"  [Meta Ads] TIMEOUT ({TIMEOUT_POR_CONTA}s) ao buscar {nome_arquivo} para {shopping}")
        return pd.DataFrame()
    except Exception as e:
        print(f"  [Meta Ads] Erro ao buscar {nome_arquivo}: {e}")
        return pd.DataFrame()

    data = []

    for row in rows_list:
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
    # Apenas campanhas usa diario — todos os breakdowns usam mensal
    # para reduzir volume e tempo de API (8 contas × N dias)
    tipos_extracao = [
        {"nome": "campanhas", "breakdowns": None, "increment": 1},
        {"nome": "plataforma", "breakdowns": ['publisher_platform'], "increment": 'monthly'},
        {"nome": "posicionamento", "breakdowns": ['publisher_platform', 'platform_position'], "increment": 'monthly'},
        {"nome": "demografico_idade", "breakdowns": ['age'], "increment": 'monthly'},
        {"nome": "demografico_genero", "breakdowns": ['gender'], "increment": 'monthly'},
        {"nome": "dispositivo", "breakdowns": ['device_platform'], "increment": 'monthly'},
        {"nome": "video", "breakdowns": None, "increment": 'monthly'},
    ]

    for tipo in tipos_extracao:
        dfs = []
        for sigla, account in accounts.items():
            print(f"  [Meta Ads] {sigla} → {tipo['nome']}...", flush=True)
            df = extrair_insights(
                account, data_inicio, data_fim,
                breakdowns=tipo['breakdowns'],
                nome_arquivo=tipo['nome'],
                shopping=sigla,
                time_increment=tipo['increment'],
            )
            if not df.empty:
                dfs.append(df)

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            df_final.to_csv(OUTPUT_DIR / f"{tipo['nome']}.csv", index=False, encoding='utf-8-sig')
            print(f"  [Meta Ads] {tipo['nome']}.csv: {len(df_final)} linhas ({len(dfs)} contas)", flush=True)
        else:
            print(f"  [Meta Ads] {tipo['nome']}.csv: sem dados", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Extrator Meta Ads")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    print(f"[Meta Ads] Extraindo de {data_inicio} a {data_fim}...", flush=True)

    accounts = init_api()
    print(f"[Meta Ads] {len(accounts)} contas inicializadas.", flush=True)
    extrair_todas_contas(accounts, data_inicio, data_fim)

    print("[Meta Ads] Extracao concluida!", flush=True)


if __name__ == "__main__":
    main()
