"""
Consolidador Cross-Platform
Unifica dados de Google Ads, Meta Ads e TikTok Ads em CSVs consolidados.

Gera CSVs em Dados/Consolidado/:
  - cross_platform_diario.csv
  - cross_platform_semanal.csv
  - cross_platform_mensal.csv
  - funil_por_plataforma.csv
  - demografico_cross.csv

Uso:
  python scripts/consolidar_cross_platform.py
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
DADOS_DIR = BASE_DIR / "Dados"
OUTPUT_DIR = DADOS_DIR / "Consolidado"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def carregar_csv_seguro(caminho):
    """Carrega CSV se existir, senao retorna DataFrame vazio."""
    if caminho.exists():
        try:
            return pd.read_csv(caminho, encoding='utf-8-sig')
        except Exception:
            try:
                return pd.read_csv(caminho, encoding='utf-8')
            except Exception as e:
                print(f"  [AVISO] Erro ao ler {caminho.name}: {e}")
    return pd.DataFrame()


def consolidar_diario():
    """Consolida metricas diarias de todas as plataformas."""
    registros = []

    # Google Ads
    df = carregar_csv_seguro(DADOS_DIR / "Google_Ads" / "diario.csv")
    if not df.empty:
        for _, r in df.iterrows():
            registros.append({
                'data': r.get('data', ''),
                'plataforma': 'Google Ads',
                'impressoes': r.get('impressoes', 0),
                'cliques': r.get('cliques', 0),
                'custo': r.get('custo', 0),
                'conversoes': r.get('conversoes', 0),
                'receita': r.get('valor_conversoes', 0),
                'ctr': r.get('ctr', 0),
                'cpc': r.get('cpc_medio', 0),
                'cpm': r.get('custo', 0) / max(r.get('impressoes', 1), 1) * 1000,
            })

    # Meta Ads
    df = carregar_csv_seguro(DADOS_DIR / "Meta_Ads" / "campanhas.csv")
    if not df.empty:
        for data, grupo in df.groupby('data'):
            registros.append({
                'data': data,
                'plataforma': 'Meta Ads',
                'impressoes': grupo['impressoes'].sum(),
                'cliques': grupo['cliques'].sum(),
                'custo': grupo['custo'].sum(),
                'conversoes': grupo.get('purchase', pd.Series([0])).sum(),
                'receita': grupo.get('valor_purchase', pd.Series([0])).sum(),
                'ctr': grupo['ctr'].mean(),
                'cpc': grupo['cpc'].mean(),
                'cpm': grupo['cpm'].mean(),
            })

    # TikTok Ads
    df = carregar_csv_seguro(DADOS_DIR / "TikTok_Ads" / "diario.csv")
    if not df.empty:
        col_data = 'stat_time_day' if 'stat_time_day' in df.columns else 'data'
        for _, r in df.iterrows():
            registros.append({
                'data': r.get(col_data, ''),
                'plataforma': 'TikTok Ads',
                'impressoes': r.get('impressions', 0),
                'cliques': r.get('clicks', 0),
                'custo': r.get('spend', 0),
                'conversoes': r.get('conversion', 0),
                'receita': 0,  # TikTok nao retorna receita diretamente
                'ctr': r.get('ctr', 0),
                'cpc': r.get('cpc', 0),
                'cpm': r.get('cpm', 0),
            })

    df_consolidado = pd.DataFrame(registros)
    if df_consolidado.empty:
        print("  [Consolidar] Nenhum dado diario encontrado")
        return df_consolidado

    df_consolidado['data'] = pd.to_datetime(df_consolidado['data'])
    df_consolidado = df_consolidado.sort_values('data')

    # Calcular ROAS e CPA
    df_consolidado['roas'] = np.where(
        df_consolidado['custo'] > 0,
        df_consolidado['receita'] / df_consolidado['custo'],
        0
    )
    df_consolidado['cpa'] = np.where(
        df_consolidado['conversoes'] > 0,
        df_consolidado['custo'] / df_consolidado['conversoes'],
        0
    )

    df_consolidado.to_csv(OUTPUT_DIR / "cross_platform_diario.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] cross_platform_diario.csv: {len(df_consolidado)} linhas")

    # Agregacao semanal
    df_consolidado['semana'] = df_consolidado['data'].dt.isocalendar().week.astype(int)
    df_consolidado['ano'] = df_consolidado['data'].dt.year
    df_semanal = df_consolidado.groupby(['ano', 'semana', 'plataforma']).agg({
        'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum',
        'conversoes': 'sum', 'receita': 'sum',
    }).reset_index()
    df_semanal['roas'] = np.where(df_semanal['custo'] > 0, df_semanal['receita'] / df_semanal['custo'], 0)
    df_semanal['cpa'] = np.where(df_semanal['conversoes'] > 0, df_semanal['custo'] / df_semanal['conversoes'], 0)
    df_semanal['ctr'] = np.where(df_semanal['impressoes'] > 0, df_semanal['cliques'] / df_semanal['impressoes'] * 100, 0)
    df_semanal.to_csv(OUTPUT_DIR / "cross_platform_semanal.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] cross_platform_semanal.csv: {len(df_semanal)} linhas")

    # Agregacao mensal
    df_consolidado['mes'] = df_consolidado['data'].dt.to_period('M').astype(str)
    df_mensal = df_consolidado.groupby(['mes', 'plataforma']).agg({
        'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum',
        'conversoes': 'sum', 'receita': 'sum',
    }).reset_index()
    df_mensal['roas'] = np.where(df_mensal['custo'] > 0, df_mensal['receita'] / df_mensal['custo'], 0)
    df_mensal['cpa'] = np.where(df_mensal['conversoes'] > 0, df_mensal['custo'] / df_mensal['conversoes'], 0)
    df_mensal['ctr'] = np.where(df_mensal['impressoes'] > 0, df_mensal['cliques'] / df_mensal['impressoes'] * 100, 0)
    df_mensal.to_csv(OUTPUT_DIR / "cross_platform_mensal.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] cross_platform_mensal.csv: {len(df_mensal)} linhas")

    return df_consolidado


def gerar_funil():
    """Gera funil integrado: Impressoes → Cliques → LPV → Sessoes → Conversoes → Receita."""
    registros = []

    for plataforma in ['Google Ads', 'Meta Ads', 'TikTok Ads']:
        impressoes = 0
        cliques = 0
        lpv = 0
        sessoes = 0
        conversoes = 0
        receita = 0

        if plataforma == 'Google Ads':
            df = carregar_csv_seguro(DADOS_DIR / "Google_Ads" / "diario.csv")
            if not df.empty:
                impressoes = df['impressoes'].sum()
                cliques = df['cliques'].sum()
                conversoes = df['conversoes'].sum()
                receita = df['valor_conversoes'].sum()

        elif plataforma == 'Meta Ads':
            df = carregar_csv_seguro(DADOS_DIR / "Meta_Ads" / "campanhas.csv")
            if not df.empty:
                impressoes = df['impressoes'].sum()
                cliques = df['cliques'].sum()
                lpv = df['landing_page_view'].sum() if 'landing_page_view' in df.columns else 0
                conversoes = df['purchase'].sum() if 'purchase' in df.columns else 0
                receita = df['valor_purchase'].sum() if 'valor_purchase' in df.columns else 0

        elif plataforma == 'TikTok Ads':
            df = carregar_csv_seguro(DADOS_DIR / "TikTok_Ads" / "diario.csv")
            if not df.empty:
                impressoes = df.get('impressions', pd.Series([0])).sum()
                cliques = df.get('clicks', pd.Series([0])).sum()
                conversoes = df.get('conversion', pd.Series([0])).sum()

        # GA4 sessoes por fonte
        df_ga4 = carregar_csv_seguro(DADOS_DIR / "GA4" / "sessoes_por_fonte.csv")
        if not df_ga4.empty:
            fontes_map = {
                'Google Ads': ['google', 'cpc'],
                'Meta Ads': ['facebook', 'instagram', 'fb', 'ig'],
                'TikTok Ads': ['tiktok'],
            }
            fontes = fontes_map.get(plataforma, [])
            mask = df_ga4['sessionSource'].str.lower().isin(fontes) | \
                   df_ga4['sessionMedium'].str.lower().isin(['cpc', 'paid', 'paidsocial'])
            if fontes:
                mask = df_ga4['sessionSource'].str.lower().str.contains('|'.join(fontes), na=False)
            sessoes_ga4 = df_ga4.loc[mask, 'sessions'].sum()
            if sessoes_ga4 > 0:
                sessoes = sessoes_ga4

        if lpv == 0:
            lpv = cliques * 0.85  # estimativa se nao disponivel

        registros.append({
            'plataforma': plataforma,
            'impressoes': impressoes,
            'cliques': cliques,
            'landing_page_views': lpv,
            'sessoes_ga4': sessoes,
            'conversoes': conversoes,
            'receita': receita,
        })

    df_funil = pd.DataFrame(registros)
    df_funil.to_csv(OUTPUT_DIR / "funil_por_plataforma.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] funil_por_plataforma.csv: {len(df_funil)} linhas")
    return df_funil


def consolidar_demografico():
    """Consolida dados demograficos de todas as plataformas."""
    registros = []

    # Google Ads
    df = carregar_csv_seguro(DADOS_DIR / "Google_Ads" / "demografico.csv")
    if not df.empty:
        for _, r in df.iterrows():
            registros.append({
                'plataforma': 'Google Ads',
                'tipo': r.get('tipo', ''),
                'segmento': r.get('segmento', ''),
                'impressoes': r.get('impressoes', 0),
                'cliques': r.get('cliques', 0),
                'custo': r.get('custo', 0),
                'conversoes': r.get('conversoes', 0),
            })

    # Meta Ads
    for arquivo, tipo in [('demografico_idade.csv', 'faixa_etaria'), ('demografico_genero.csv', 'genero')]:
        df = carregar_csv_seguro(DADOS_DIR / "Meta_Ads" / arquivo)
        if not df.empty:
            col_seg = 'age' if tipo == 'faixa_etaria' else 'gender'
            for _, r in df.iterrows():
                registros.append({
                    'plataforma': 'Meta Ads',
                    'tipo': tipo,
                    'segmento': r.get(col_seg, ''),
                    'impressoes': r.get('impressoes', 0),
                    'cliques': r.get('cliques', 0),
                    'custo': r.get('custo', 0),
                    'conversoes': r.get('purchase', 0),
                })

    # TikTok Ads
    for arquivo, tipo in [('demografico_idade.csv', 'faixa_etaria'), ('demografico_genero.csv', 'genero')]:
        df = carregar_csv_seguro(DADOS_DIR / "TikTok_Ads" / arquivo)
        if not df.empty:
            col_seg = 'age' if tipo == 'faixa_etaria' else 'gender'
            for _, r in df.iterrows():
                registros.append({
                    'plataforma': 'TikTok Ads',
                    'tipo': tipo,
                    'segmento': r.get(col_seg, ''),
                    'impressoes': r.get('impressions', 0),
                    'cliques': r.get('clicks', 0),
                    'custo': r.get('spend', 0),
                    'conversoes': r.get('conversion', 0),
                })

    df_demo = pd.DataFrame(registros)
    if not df_demo.empty:
        df_demo['cpa'] = np.where(df_demo['conversoes'] > 0, df_demo['custo'] / df_demo['conversoes'], 0)
        df_demo['ctr'] = np.where(df_demo['impressoes'] > 0, df_demo['cliques'] / df_demo['impressoes'] * 100, 0)

    df_demo.to_csv(OUTPUT_DIR / "demografico_cross.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] demografico_cross.csv: {len(df_demo)} linhas")
    return df_demo


def consolidar_por_shopping():
    """Consolida metricas diarias agrupadas por shopping e plataforma."""
    registros = []

    # Google Ads (campanhas tem shopping_sigla)
    df = carregar_csv_seguro(DADOS_DIR / "Google_Ads" / "campanhas.csv")
    if not df.empty and 'shopping_sigla' in df.columns:
        for (data, sigla), grupo in df.groupby(['data', 'shopping_sigla']):
            registros.append({
                'data': data,
                'shopping': grupo['shopping'].iloc[0] if 'shopping' in grupo.columns else sigla,
                'shopping_sigla': sigla,
                'plataforma': 'Google Ads',
                'impressoes': grupo['impressoes'].sum(),
                'cliques': grupo['cliques'].sum(),
                'custo': grupo['custo'].sum(),
                'conversoes': grupo['conversoes'].sum(),
                'receita': grupo['valor_conversoes'].sum(),
            })

    # Meta Ads (campanhas tem shopping)
    df = carregar_csv_seguro(DADOS_DIR / "Meta_Ads" / "campanhas.csv")
    if not df.empty and 'shopping' in df.columns:
        for (data, sigla), grupo in df.groupby(['data', 'shopping']):
            registros.append({
                'data': data,
                'shopping': sigla,
                'shopping_sigla': sigla,
                'plataforma': 'Meta Ads',
                'impressoes': grupo['impressoes'].sum(),
                'cliques': grupo['cliques'].sum(),
                'custo': grupo['custo'].sum(),
                'conversoes': grupo.get('purchase', pd.Series([0])).sum(),
                'receita': grupo.get('valor_purchase', pd.Series([0])).sum(),
            })

    # TikTok Ads (campanhas tem shopping)
    df = carregar_csv_seguro(DADOS_DIR / "TikTok_Ads" / "campanhas.csv")
    if not df.empty and 'shopping' in df.columns:
        col_data = 'stat_time_day' if 'stat_time_day' in df.columns else 'data'
        for (data, sigla), grupo in df.groupby([col_data, 'shopping']):
            registros.append({
                'data': data,
                'shopping': sigla,
                'shopping_sigla': sigla,
                'plataforma': 'TikTok Ads',
                'impressoes': grupo.get('impressions', pd.Series([0])).sum(),
                'cliques': grupo.get('clicks', pd.Series([0])).sum(),
                'custo': grupo.get('spend', pd.Series([0])).sum(),
                'conversoes': grupo.get('conversion', pd.Series([0])).sum(),
                'receita': 0,
            })

    df_shopping = pd.DataFrame(registros)
    if df_shopping.empty:
        print("  [Consolidar] Nenhum dado por shopping encontrado")
        return df_shopping

    df_shopping['data'] = pd.to_datetime(df_shopping['data'])
    df_shopping = df_shopping.sort_values('data')
    df_shopping['roas'] = np.where(df_shopping['custo'] > 0, df_shopping['receita'] / df_shopping['custo'], 0)
    df_shopping['cpa'] = np.where(df_shopping['conversoes'] > 0, df_shopping['custo'] / df_shopping['conversoes'], 0)
    df_shopping['ctr'] = np.where(df_shopping['impressoes'] > 0, df_shopping['cliques'] / df_shopping['impressoes'] * 100, 0)

    # Diario por shopping
    df_shopping.to_csv(OUTPUT_DIR / "cross_platform_shopping_diario.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] cross_platform_shopping_diario.csv: {len(df_shopping)} linhas")

    # Mensal por shopping
    df_shopping['mes'] = df_shopping['data'].dt.to_period('M').astype(str)
    df_mensal = df_shopping.groupby(['mes', 'shopping', 'shopping_sigla', 'plataforma']).agg({
        'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum',
        'conversoes': 'sum', 'receita': 'sum',
    }).reset_index()
    df_mensal['roas'] = np.where(df_mensal['custo'] > 0, df_mensal['receita'] / df_mensal['custo'], 0)
    df_mensal['cpa'] = np.where(df_mensal['conversoes'] > 0, df_mensal['custo'] / df_mensal['conversoes'], 0)
    df_mensal['ctr'] = np.where(df_mensal['impressoes'] > 0, df_mensal['cliques'] / df_mensal['impressoes'] * 100, 0)
    df_mensal.to_csv(OUTPUT_DIR / "cross_platform_shopping_mensal.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] cross_platform_shopping_mensal.csv: {len(df_mensal)} linhas")

    return df_shopping


def consolidar_dispositivos():
    """Consolida dados de dispositivos de todas as plataformas."""
    registros = []

    # Google Ads
    df = carregar_csv_seguro(DADOS_DIR / "Google_Ads" / "dispositivos.csv")
    if not df.empty:
        for _, r in df.iterrows():
            registros.append({
                'plataforma': 'Google Ads',
                'dispositivo': r.get('dispositivo', ''),
                'impressoes': r.get('impressoes', 0),
                'cliques': r.get('cliques', 0),
                'custo': r.get('custo', 0),
                'conversoes': r.get('conversoes', 0),
            })

    # Meta Ads
    df = carregar_csv_seguro(DADOS_DIR / "Meta_Ads" / "dispositivo.csv")
    if not df.empty:
        for _, r in df.iterrows():
            registros.append({
                'plataforma': 'Meta Ads',
                'dispositivo': r.get('device_platform', ''),
                'impressoes': r.get('impressoes', 0),
                'cliques': r.get('cliques', 0),
                'custo': r.get('custo', 0),
                'conversoes': r.get('purchase', 0),
            })

    # GA4
    df = carregar_csv_seguro(DADOS_DIR / "GA4" / "dispositivos.csv")
    if not df.empty:
        for dev, grupo in df.groupby('deviceCategory'):
            registros.append({
                'plataforma': 'GA4',
                'dispositivo': dev,
                'impressoes': 0,
                'cliques': 0,
                'custo': 0,
                'conversoes': grupo['conversions'].sum(),
                'sessoes': grupo['sessions'].sum(),
                'usuarios': grupo['totalUsers'].sum(),
            })

    df_devices = pd.DataFrame(registros)
    if not df_devices.empty:
        df_devices['ctr'] = np.where(df_devices['impressoes'] > 0, df_devices['cliques'] / df_devices['impressoes'] * 100, 0)
    df_devices.to_csv(OUTPUT_DIR / "dispositivos_cross.csv", index=False, encoding='utf-8-sig')
    print(f"  [Consolidar] dispositivos_cross.csv: {len(df_devices)} linhas")
    return df_devices


def main():
    print("[Consolidar] Processando dados cross-platform...")
    consolidar_diario()
    gerar_funil()
    consolidar_demografico()
    consolidar_por_shopping()
    consolidar_dispositivos()
    print("[Consolidar] Concluido!")


if __name__ == "__main__":
    main()
