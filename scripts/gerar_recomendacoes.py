"""
Engine de Otimizacao de Verba
Calcula ROAS marginal, detecta saturacao, gera recomendacoes.

Gera CSVs em Dados/Consolidado/:
  - recomendacoes_verba.csv
  - alertas.csv

Uso:
  python scripts/gerar_recomendacoes.py
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
    if caminho.exists():
        try:
            return pd.read_csv(caminho, encoding='utf-8-sig')
        except Exception:
            try:
                return pd.read_csv(caminho, encoding='utf-8')
            except Exception:
                pass
    return pd.DataFrame()


def calcular_roas_marginal(df_diario):
    """
    Calcula ROAS marginal por plataforma.
    ROAS marginal = variacao incremental de receita / variacao incremental de custo.
    """
    resultados = []

    for plataforma in df_diario['plataforma'].unique():
        df_plt = df_diario[df_diario['plataforma'] == plataforma].sort_values('data').copy()
        if len(df_plt) < 7:
            continue

        # ROAS ultimos 7 dias
        df_7d = df_plt.tail(7)
        custo_7d = df_7d['custo'].sum()
        receita_7d = df_7d['receita'].sum()
        roas_7d = receita_7d / custo_7d if custo_7d > 0 else 0

        # ROAS ultimos 30 dias
        df_30d = df_plt.tail(30)
        custo_30d = df_30d['custo'].sum()
        receita_30d = df_30d['receita'].sum()
        roas_30d = receita_30d / custo_30d if custo_30d > 0 else 0

        # ROAS ultimos 90 dias (ou total)
        custo_total = df_plt['custo'].sum()
        receita_total = df_plt['receita'].sum()
        roas_total = receita_total / custo_total if custo_total > 0 else 0

        # Tendencia: comparar 7d vs 30d
        tendencia = (roas_7d / roas_30d - 1) if roas_30d > 0 else 0

        # CPA
        conv_total = df_plt['conversoes'].sum()
        cpa_total = custo_total / conv_total if conv_total > 0 else 0

        # Classificacao
        roas_medio_geral = df_diario['receita'].sum() / max(df_diario['custo'].sum(), 1)
        if roas_total >= roas_medio_geral * 1.2 and tendencia >= -0.05:
            acao = 'AUMENTAR'
            motivo = f'ROAS {roas_total:.1f}x acima da media ({roas_medio_geral:.1f}x), tendencia {"positiva" if tendencia >= 0 else "estavel"}'
        elif roas_total <= roas_medio_geral * 0.8 or tendencia < -0.15:
            acao = 'DIMINUIR'
            if tendencia < -0.15:
                motivo = f'ROAS em queda ({tendencia*100:+.0f}% nos ultimos 7d vs 30d)'
            else:
                motivo = f'ROAS {roas_total:.1f}x abaixo da media ({roas_medio_geral:.1f}x)'
        else:
            acao = 'MANTER'
            motivo = f'ROAS {roas_total:.1f}x dentro da faixa esperada'

        resultados.append({
            'plataforma': plataforma,
            'custo_total': custo_total,
            'receita_total': receita_total,
            'roas_7d': roas_7d,
            'roas_30d': roas_30d,
            'roas_total': roas_total,
            'tendencia_7d_vs_30d': tendencia,
            'cpa_total': cpa_total,
            'conversoes_total': conv_total,
            'acao': acao,
            'motivo': motivo,
        })

    return pd.DataFrame(resultados)


def detectar_saturacao(df_diario):
    """Detecta saturacao: frequencia crescente + CTR decrescente."""
    alertas = []

    for plataforma in df_diario['plataforma'].unique():
        df_plt = df_diario[df_diario['plataforma'] == plataforma].sort_values('data')
        if len(df_plt) < 14:
            continue

        # CTR tendencia (regressao linear simples)
        if 'ctr' in df_plt.columns:
            ctr_vals = df_plt['ctr'].values
            x = np.arange(len(ctr_vals))
            if len(ctr_vals) > 1 and np.std(ctr_vals) > 0:
                slope = np.polyfit(x, ctr_vals, 1)[0]
                ctr_media = ctr_vals.mean()
                if slope < 0 and abs(slope * len(ctr_vals)) / max(ctr_media, 0.001) > 0.1:
                    alertas.append({
                        'plataforma': plataforma,
                        'tipo': 'saturacao_ctr',
                        'severidade': 'Media',
                        'mensagem': f'CTR em queda consistente ({ctr_media:.2f}% medio, tendencia negativa)',
                        'metrica': 'ctr',
                        'valor_atual': float(ctr_vals[-1]),
                        'valor_media': float(ctr_media),
                    })

    return alertas


def detectar_anomalias_custo(df_diario, desvios=2.0):
    """Detecta anomalias de custo (spikes ou quedas)."""
    alertas = []

    for plataforma in df_diario['plataforma'].unique():
        df_plt = df_diario[df_diario['plataforma'] == plataforma].sort_values('data')
        if len(df_plt) < 7:
            continue

        for metrica in ['custo', 'cpa', 'ctr']:
            if metrica not in df_plt.columns:
                continue

            vals = df_plt[metrica].values
            media_movel = pd.Series(vals).rolling(7, min_periods=3).mean()
            std_movel = pd.Series(vals).rolling(7, min_periods=3).std()

            ultimo = vals[-1]
            media = media_movel.iloc[-2] if len(media_movel) > 1 else media_movel.iloc[-1]
            std = std_movel.iloc[-2] if len(std_movel) > 1 else std_movel.iloc[-1]

            if pd.isna(media) or pd.isna(std) or std == 0:
                continue

            desvio = (ultimo - media) / std
            if abs(desvio) > desvios:
                variacao_pct = (ultimo - media) / max(abs(media), 0.001) * 100
                direcao = 'subiu' if desvio > 0 else 'caiu'
                severidade = 'Alta' if abs(desvio) > 3 else 'Media'

                alertas.append({
                    'plataforma': plataforma,
                    'tipo': f'anomalia_{metrica}',
                    'severidade': severidade,
                    'mensagem': f'{metrica.upper()} {direcao} {abs(variacao_pct):.0f}% vs media movel 7d',
                    'metrica': metrica,
                    'valor_atual': float(ultimo),
                    'valor_media': float(media),
                })

    return alertas


def main():
    print("[Recomendacoes] Gerando analises...")

    df_diario = carregar_csv_seguro(OUTPUT_DIR / "cross_platform_diario.csv")
    if df_diario.empty:
        print("[Recomendacoes] Sem dados diarios consolidados. Execute consolidar_cross_platform.py primeiro.")
        return

    df_diario['data'] = pd.to_datetime(df_diario['data'])

    # 1. Recomendacoes de verba
    df_rec = calcular_roas_marginal(df_diario)
    df_rec.to_csv(OUTPUT_DIR / "recomendacoes_verba.csv", index=False, encoding='utf-8-sig')
    print(f"  [Recomendacoes] recomendacoes_verba.csv: {len(df_rec)} linhas")

    # 2. Alertas
    alertas = []
    alertas.extend(detectar_saturacao(df_diario))
    alertas.extend(detectar_anomalias_custo(df_diario))

    df_alertas = pd.DataFrame(alertas)
    df_alertas.to_csv(OUTPUT_DIR / "alertas.csv", index=False, encoding='utf-8-sig')
    print(f"  [Recomendacoes] alertas.csv: {len(df_alertas)} linhas")

    print("[Recomendacoes] Concluido!")


if __name__ == "__main__":
    main()
