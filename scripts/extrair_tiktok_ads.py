"""
Extrator TikTok Ads Marketing API — Multi-Shopping
Gera 12 CSVs em Dados/TikTok_Ads/ (todos com coluna 'shopping')

Limites da API TikTok:
  - stat_time_day: max 30 dias por request → chunking automatico
  - stat_time_hour: max 1 dia por request → chunking automatico
  - Sem dimensao temporal: sem limite de range
  - Paginacao: max 1000 rows por pagina

Requer:
  - requests
  - TIKTOK_ADS_CONFIG (JSON): {"NS": {"token": "x", "advertiser_id": "y"}, ...}

Uso:
  python scripts/extrair_tiktok_ads.py [--dias 365]
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

# Colunas que nao devem ser convertidas para numerico
COLS_NAO_NUMERICAS = {
    'campaign_name', 'campaign_id', 'adgroup_name', 'adgroup_id',
    'stat_time_day', 'stat_time_hour', 'gender', 'age',
    'country_code', 'province_id', 'ac', 'platform',
    'interest_category', 'interest_category_v2',
    'shopping', 'shopping_sigla', 'objective_type', 'status',
}

# --- Limites da API por tipo de dimensao temporal ---
MAX_DAYS_DAILY = 30    # stat_time_day: max 30 dias
MAX_DAYS_HOURLY = 1    # stat_time_hour: max 1 dia
MAX_DAYS_NO_TIME = 365  # sem dimensao temporal: sem limite real


def get_config():
    """Carrega configuracao multi-shopping do env."""
    config_json = os.environ.get("TIKTOK_ADS_CONFIG", "")
    if not config_json:
        token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
        adv_id = os.environ.get("TIKTOK_ADVERTISER_ID", "")
        if token and adv_id:
            return {"GERAL": {"token": token, "advertiser_id": adv_id}}
        print("[ERRO] TIKTOK_ADS_CONFIG ou TIKTOK_ACCESS_TOKEN nao configurados")
        sys.exit(1)
    return json.loads(config_json)


def _gerar_chunks(data_inicio, data_fim, max_dias):
    """Gera pares (inicio, fim) respeitando o limite maximo de dias por request."""
    start = datetime.strptime(data_inicio, '%Y-%m-%d')
    end = datetime.strptime(data_fim, '%Y-%m-%d')
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=max_dias - 1), end)
        chunks.append((current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
        current = chunk_end + timedelta(days=1)
    return chunks


def _detectar_max_dias(dimensions):
    """Detecta o limite maximo de dias baseado nas dimensoes usadas."""
    if 'stat_time_hour' in dimensions:
        return MAX_DAYS_HOURLY
    if 'stat_time_day' in dimensions:
        return MAX_DAYS_DAILY
    return MAX_DAYS_NO_TIME


def fetch_report_single(token, advertiser_id, data_inicio, data_fim,
                         dimensions, metrics, shopping_sigla,
                         data_level="AUCTION_CAMPAIGN", report_type="BASIC"):
    """Busca 1 request (sem chunking) via TikTok Reporting API. Pagina automaticamente."""
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

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            data = resp.json()
        except Exception as e:
            print(f"    [TikTok/{shopping_sigla}] Request falhou: {e}")
            break

        code = data.get("code", -1)
        if code != 0:
            msg = data.get("message", "Desconhecido")
            # Nao logar "No data" como erro (apenas sem dados no periodo)
            if "no data" not in msg.lower():
                print(f"    [TikTok/{shopping_sigla}] API erro (code={code}): {msg}")
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

    return all_rows


def fetch_report(token, advertiser_id, data_inicio, data_fim,
                  dimensions, metrics, shopping_sigla,
                  data_level="AUCTION_CAMPAIGN", report_type="BASIC"):
    """Busca relatorio com chunking automatico baseado no tipo de dimensao temporal."""
    max_dias = _detectar_max_dias(dimensions)
    chunks = _gerar_chunks(data_inicio, data_fim, max_dias)

    all_rows = []
    for chunk_inicio, chunk_fim in chunks:
        rows = fetch_report_single(
            token, advertiser_id, chunk_inicio, chunk_fim,
            dimensions, metrics, shopping_sigla, data_level, report_type
        )
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)

    # Converter colunas numericas
    if not df.empty:
        for col in df.columns:
            if col not in COLS_NAO_NUMERICAS:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                except (ValueError, TypeError):
                    pass

    return df


def fetch_campaigns(token, advertiser_id, shopping_sigla):
    """Busca metadados de campanhas (nome, objetivo, status) via Campaign Management API."""
    url = f"{API_BASE}/campaign/get/"
    headers = {"Access-Token": token}

    all_campaigns = []
    page = 1
    page_size = 1000

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "page": page,
            "page_size": page_size,
        }

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            data = resp.json()
        except Exception as e:
            print(f"    [TikTok/{shopping_sigla}] Erro campanhas: {e}")
            break

        if data.get("code") != 0:
            print(f"    [TikTok/{shopping_sigla}] Erro campanhas: {data.get('message', 'Desconhecido')}")
            break

        campaigns = data.get("data", {}).get("list", [])
        if not campaigns:
            break

        for c in campaigns:
            all_campaigns.append({
                'campaign_id': str(c.get('campaign_id', '')),
                'campaign_name': c.get('campaign_name', ''),
                'objective_type': c.get('objective_type', ''),
                'status': c.get('operation_status', c.get('status', '')),
                'budget': c.get('budget', 0),
                'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
                'shopping_sigla': shopping_sigla,
            })

        page_info = data.get("data", {}).get("page_info", {})
        total = page_info.get("total_number", 0)
        if page * page_size >= total:
            break
        page += 1

    return pd.DataFrame(all_campaigns)


def fetch_adgroups(token, advertiser_id, shopping_sigla):
    """Busca metadados de ad groups (nome, targeting) via AdGroup Management API."""
    url = f"{API_BASE}/adgroup/get/"
    headers = {"Access-Token": token}

    all_adgroups = []
    page = 1
    page_size = 1000

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "page": page,
            "page_size": page_size,
        }

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            data = resp.json()
        except Exception as e:
            print(f"    [TikTok/{shopping_sigla}] Erro adgroups: {e}")
            break

        if data.get("code") != 0:
            print(f"    [TikTok/{shopping_sigla}] Erro adgroups: {data.get('message', 'Desconhecido')}")
            break

        adgroups = data.get("data", {}).get("list", [])
        if not adgroups:
            break

        for ag in adgroups:
            all_adgroups.append({
                'adgroup_id': str(ag.get('adgroup_id', '')),
                'adgroup_name': ag.get('adgroup_name', ''),
                'campaign_id': str(ag.get('campaign_id', '')),
                'status': ag.get('operation_status', ag.get('status', '')),
                'budget': ag.get('budget', 0),
                'placement_type': ag.get('placement_type', ''),
                'optimization_goal': ag.get('optimization_goal', ''),
                'bid_type': ag.get('bid_type', ''),
                'shopping': SHOPPING_NOMES.get(shopping_sigla, shopping_sigla),
                'shopping_sigla': shopping_sigla,
            })

        page_info = data.get("data", {}).get("page_info", {})
        total = page_info.get("total_number", 0)
        if page * page_size >= total:
            break
        page += 1

    return pd.DataFrame(all_adgroups)


def enriquecer_csv_seguro(csv_path, df_meta, merge_on, merge_cols, label=""):
    """Enriquece CSV existente com colunas de metadados. Nao crashea se vazio."""
    if not csv_path.exists():
        print(f"    [TikTok] {label}: arquivo nao encontrado, pulando")
        return
    try:
        df = pd.read_csv(csv_path, dtype={merge_on: str}, encoding='utf-8-sig')
    except Exception:
        print(f"    [TikTok] {label}: arquivo vazio ou invalido, pulando")
        return
    if df.empty:
        print(f"    [TikTok] {label}: sem dados para enriquecer")
        return

    meta = df_meta[merge_cols].drop_duplicates()
    df[merge_on] = df[merge_on].astype(str)

    # Remover colunas que ja existem (evitar _x _y no merge)
    cols_to_add = [c for c in merge_cols if c != merge_on and c not in df.columns]
    if not cols_to_add:
        print(f"    [TikTok] {label}: ja enriquecido, pulando")
        return

    df = df.merge(meta[[merge_on] + cols_to_add], on=merge_on, how='left')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"    [TikTok] {label}: enriquecido com {len(cols_to_add)} colunas")


def main():
    parser = argparse.ArgumentParser(description="Extrator TikTok Ads (Multi-Shopping)")
    parser.add_argument("--dias", type=int, default=365, help="Dias para extrair (default 365)")
    args = parser.parse_args()

    data_fim = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=args.dias)).strftime('%Y-%m-%d')

    config = get_config()
    n_contas = len(config)
    print(f"[TikTok Ads] Extraindo de {data_inicio} a {data_fim} ({args.dias} dias) para {n_contas} conta(s)...", flush=True)

    # --- Metricas ---
    metrics_base = [
        "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
        "conversion", "cost_per_conversion", "conversion_rate",
        "total_complete_payment_rate",
    ]
    metrics_audience = [
        "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
        "conversion", "cost_per_conversion", "conversion_rate",
    ]
    metrics_audience_reach = metrics_audience + ["reach", "frequency"]
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

    # --- Relatorios ---
    # Cada um com: (nome_csv, dimensions, metrics, data_level, report_type)
    # O chunking e automatico baseado nas dimensoes (stat_time_day=30d, stat_time_hour=1d)
    relatorios = [
        ("campanhas",          ["campaign_id", "stat_time_day"], metrics_base + metrics_engagement,                "AUCTION_CAMPAIGN",   "BASIC"),
        ("video_engagement",   ["campaign_id", "stat_time_day"], metrics_base + metrics_video,                     "AUCTION_CAMPAIGN",   "BASIC"),
        ("demografico_idade",  ["stat_time_day", "age"],         metrics_audience,                                  "AUCTION_ADVERTISER", "AUDIENCE"),
        ("demografico_genero", ["stat_time_day", "gender"],      metrics_audience,                                  "AUCTION_ADVERTISER", "AUDIENCE"),
        ("diario",             ["stat_time_day"],                 metrics_base + metrics_video + metrics_engagement, "AUCTION_CAMPAIGN",   "BASIC"),
        ("geografico",         ["country_code"],                  metrics_audience,                                  "AUCTION_ADVERTISER", "AUDIENCE"),
        ("plataforma",         ["stat_time_day", "platform"],    metrics_audience,                                  "AUCTION_ADVERTISER", "AUDIENCE"),
        ("adgroup_diario",     ["adgroup_id", "stat_time_day"],  metrics_base + metrics_engagement,                "AUCTION_ADGROUP",    "BASIC"),
    ]

    total_linhas = 0
    total_erros = 0

    for nome_arquivo, dimensions, metrics, data_level, report_type in relatorios:
        max_dias = _detectar_max_dias(dimensions)
        n_chunks = len(_gerar_chunks(data_inicio, data_fim, max_dias))

        dfs = []
        for sigla, creds in config.items():
            print(f"  [TikTok/{sigla}] {nome_arquivo} ({n_chunks} chunks de {max_dias}d)...", flush=True)
            df = fetch_report(
                creds["token"], creds["advertiser_id"],
                data_inicio, data_fim,
                dimensions, metrics, sigla, data_level, report_type
            )
            if not df.empty:
                dfs.append(df)

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
        else:
            df_final = pd.DataFrame()
            total_erros += 1

        df_final.to_csv(OUTPUT_DIR / f"{nome_arquivo}.csv", index=False, encoding='utf-8-sig')
        total_linhas += len(df_final)
        print(f"  [TikTok] {nome_arquivo}.csv: {len(df_final)} linhas", flush=True)

    # --- Hora do Dia (stat_time_hour = max 1 dia, mas chunking ja trata) ---
    # Porem 365 chunks de 1 dia = 365 requests. Limitar a 90 dias para hora.
    dias_hora = min(args.dias, 90)
    hora_inicio = (datetime.now() - timedelta(days=dias_hora)).strftime('%Y-%m-%d')
    print(f"  [TikTok] Extraindo hora_dia ({dias_hora} dias, chunked 1d)...", flush=True)
    dfs_hora = []
    for sigla, creds in config.items():
        print(f"  [TikTok/{sigla}] hora_dia...", flush=True)
        df = fetch_report(
            creds["token"], creds["advertiser_id"],
            hora_inicio, data_fim,
            ["stat_time_hour"], metrics_base + metrics_engagement, sigla,
            "AUCTION_CAMPAIGN", "BASIC"
        )
        if not df.empty:
            dfs_hora.append(df)

    df_hora = pd.concat(dfs_hora, ignore_index=True) if dfs_hora else pd.DataFrame()
    if not df_hora.empty and 'stat_time_hour' in df_hora.columns:
        df_hora['hora'] = pd.to_datetime(df_hora['stat_time_hour']).dt.hour
        num_cols = [c for c in df_hora.columns if c not in COLS_NAO_NUMERICAS and c != 'hora']
        df_hora_agg = df_hora.groupby(['hora', 'shopping', 'shopping_sigla'], as_index=False)[num_cols].sum()
        df_hora_agg.to_csv(OUTPUT_DIR / "hora_dia.csv", index=False, encoding='utf-8-sig')
        print(f"  [TikTok] hora_dia.csv: {len(df_hora_agg)} linhas", flush=True)
    else:
        pd.DataFrame().to_csv(OUTPUT_DIR / "hora_dia.csv", index=False, encoding='utf-8-sig')
        print(f"  [TikTok] hora_dia.csv: 0 linhas", flush=True)

    # --- Alcance e Frequencia (sem dimensao temporal → sem limite de range) ---
    print("  [TikTok] Extraindo alcance_frequencia...", flush=True)
    dfs_reach = []
    for sigla, creds in config.items():
        df = fetch_report(
            creds["token"], creds["advertiser_id"],
            data_inicio, data_fim,
            ["campaign_id"], metrics_audience_reach, sigla,
            "AUCTION_CAMPAIGN", "BASIC"
        )
        if not df.empty:
            dfs_reach.append(df)
    df_reach = pd.concat(dfs_reach, ignore_index=True) if dfs_reach else pd.DataFrame()
    df_reach.to_csv(OUTPUT_DIR / "alcance_frequencia.csv", index=False, encoding='utf-8-sig')
    print(f"  [TikTok] alcance_frequencia.csv: {len(df_reach)} linhas", flush=True)

    # --- Metadados: campanhas e ad groups (Management API, sem date range) ---
    print("  [TikTok] Extraindo metadados...", flush=True)
    dfs_camp_meta = []
    dfs_ag_meta = []
    for sigla, creds in config.items():
        df_c = fetch_campaigns(creds["token"], creds["advertiser_id"], sigla)
        if not df_c.empty:
            dfs_camp_meta.append(df_c)
        df_ag = fetch_adgroups(creds["token"], creds["advertiser_id"], sigla)
        if not df_ag.empty:
            dfs_ag_meta.append(df_ag)

    df_camp_meta = pd.concat(dfs_camp_meta, ignore_index=True) if dfs_camp_meta else pd.DataFrame()
    df_camp_meta.to_csv(OUTPUT_DIR / "campanhas_metadata.csv", index=False, encoding='utf-8-sig')
    print(f"  [TikTok] campanhas_metadata.csv: {len(df_camp_meta)} linhas", flush=True)

    df_ag_meta = pd.concat(dfs_ag_meta, ignore_index=True) if dfs_ag_meta else pd.DataFrame()
    df_ag_meta.to_csv(OUTPUT_DIR / "adgroups_metadata.csv", index=False, encoding='utf-8-sig')
    print(f"  [TikTok] adgroups_metadata.csv: {len(df_ag_meta)} linhas", flush=True)

    # --- Enriquecer CSVs com metadados (seguro, nao crashea) ---
    if not df_camp_meta.empty:
        camp_cols = ['campaign_id', 'campaign_name', 'objective_type']
        enriquecer_csv_seguro(
            OUTPUT_DIR / "campanhas.csv", df_camp_meta,
            'campaign_id', camp_cols, "campanhas.csv"
        )
        enriquecer_csv_seguro(
            OUTPUT_DIR / "video_engagement.csv", df_camp_meta,
            'campaign_id', camp_cols, "video_engagement.csv"
        )

    if not df_ag_meta.empty:
        ag_cols = ['adgroup_id', 'adgroup_name', 'campaign_id', 'optimization_goal']
        enriquecer_csv_seguro(
            OUTPUT_DIR / "adgroup_diario.csv", df_ag_meta,
            'adgroup_id', ag_cols, "adgroup_diario.csv"
        )
        # Adicionar nome de campanha no adgroup_diario
        if not df_camp_meta.empty:
            enriquecer_csv_seguro(
                OUTPUT_DIR / "adgroup_diario.csv", df_camp_meta,
                'campaign_id', ['campaign_id', 'campaign_name'], "adgroup_diario.csv (camp names)"
            )

    # Enriquecer alcance_frequencia com nomes de campanha
    if not df_camp_meta.empty:
        enriquecer_csv_seguro(
            OUTPUT_DIR / "alcance_frequencia.csv", df_camp_meta,
            'campaign_id', ['campaign_id', 'campaign_name', 'objective_type'],
            "alcance_frequencia.csv"
        )

    # --- Resumo final ---
    print(f"\n[TikTok Ads] Extracao concluida!", flush=True)
    print(f"  Total: {total_linhas} linhas em {len(relatorios) + 4} CSVs", flush=True)
    if total_erros > 0:
        print(f"  AVISO: {total_erros} relatorio(s) sem dados", flush=True)


if __name__ == "__main__":
    main()
