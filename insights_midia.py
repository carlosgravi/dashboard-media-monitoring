"""
INSIGHTS MIDIA - Modulo auxiliar para insights automaticos do Dashboard de Media
Funcoes: semaforos, insight boxes, recomendacoes, alertas
"""

import pandas as pd
import numpy as np

# =============================================================================
# CORES SEMAFORO
# =============================================================================
COR_VERDE = '#2ECC71'
COR_AMARELO = '#F39C12'
COR_VERMELHO = '#E74C3C'

# =============================================================================
# A. SEMAFOROS — badges coloridos para KPIs de midia
# =============================================================================

def semaforo_roas(valor):
    """ROAS — Return on Ad Spend."""
    if valor >= 4.0:
        return COR_VERDE, f'ROAS {valor:.1f}x — Excelente retorno'
    elif valor >= 2.0:
        return COR_AMARELO, f'ROAS {valor:.1f}x — Retorno moderado'
    elif valor >= 1.0:
        return COR_AMARELO, f'ROAS {valor:.1f}x — Retorno baixo'
    else:
        return COR_VERMELHO, f'ROAS {valor:.1f}x — Prejuizo'


def semaforo_ctr(valor):
    """CTR — Click-Through Rate (%)."""
    if valor >= 2.0:
        return COR_VERDE, f'CTR {valor:.2f}% — Acima da media'
    elif valor >= 1.0:
        return COR_AMARELO, f'CTR {valor:.2f}% — Na media'
    else:
        return COR_VERMELHO, f'CTR {valor:.2f}% — Abaixo da media'


def semaforo_cpa(valor, benchmark=None):
    """CPA — Cost Per Acquisition."""
    if benchmark:
        ratio = valor / max(benchmark, 0.01)
        if ratio <= 0.8:
            return COR_VERDE, f'CPA R$ {valor:,.2f} — Abaixo do benchmark'
        elif ratio <= 1.2:
            return COR_AMARELO, f'CPA R$ {valor:,.2f} — Na media'
        else:
            return COR_VERMELHO, f'CPA R$ {valor:,.2f} — Acima do benchmark'
    else:
        return COR_AMARELO, f'CPA R$ {valor:,.2f}'


def semaforo_frequencia(valor):
    """Frequencia media de impressao por usuario."""
    if valor <= 3.0:
        return COR_VERDE, f'Freq {valor:.1f} — Saudavel'
    elif valor <= 5.0:
        return COR_AMARELO, f'Freq {valor:.1f} — Atencao a fadiga'
    else:
        return COR_VERMELHO, f'Freq {valor:.1f} — Fadiga de anuncio'


def semaforo_bounce_rate(valor):
    """Bounce Rate (%) — GA4."""
    if valor <= 40:
        return COR_VERDE, f'Bounce {valor:.0f}% — Bom engajamento'
    elif valor <= 60:
        return COR_AMARELO, f'Bounce {valor:.0f}% — Moderado'
    else:
        return COR_VERMELHO, f'Bounce {valor:.0f}% — Alto abandono'


def render_semaforo(cor, msg):
    """Retorna HTML do badge semaforo."""
    return f'<span class="semaforo-badge" style="color:{cor}; font-size:0.85rem;">\u25cf {msg}</span>'


# =============================================================================
# B. INSIGHT BOXES — caixas "E dai?" para graficos
# =============================================================================

def gerar_insight_box(tipo, dados):
    """Gera texto de insight para caixas 'E dai?' abaixo dos graficos."""

    if tipo == 'distribuicao_verba':
        maior = dados.get('maior_plataforma', '')
        pct = dados.get('pct_maior', 0)
        roas = dados.get('roas_maior', 0)
        return (
            f"**{maior}** concentra **{pct:.0f}%** do investimento total "
            f"com ROAS de **{roas:.1f}x**. "
            f"{'Considere redistribuir verba se outras plataformas mostram ROAS superior.' if roas < 2.0 else 'O retorno justifica a concentracao.'}"
        )

    elif tipo == 'cpa_comparativo':
        melhor = dados.get('melhor_plataforma', '')
        cpa_melhor = dados.get('cpa_melhor', 0)
        pior = dados.get('pior_plataforma', '')
        cpa_pior = dados.get('cpa_pior', 0)
        return (
            f"**{melhor}** tem o menor CPA (R$ {cpa_melhor:,.2f}), "
            f"enquanto **{pior}** tem o maior (R$ {cpa_pior:,.2f}). "
            f"Diferenca de **{((cpa_pior/max(cpa_melhor,0.01))-1)*100:.0f}%** no custo por aquisicao."
        )

    elif tipo == 'tendencia_roas':
        plataforma = dados.get('plataforma', '')
        roas_7d = dados.get('roas_7d', 0)
        roas_30d = dados.get('roas_30d', 0)
        variacao = ((roas_7d / max(roas_30d, 0.01)) - 1) * 100
        direcao = 'subindo' if variacao > 0 else 'caindo'
        return (
            f"ROAS de **{plataforma}** esta **{direcao}** "
            f"({roas_30d:.1f}x nos ultimos 30d vs {roas_7d:.1f}x nos ultimos 7d, "
            f"variacao de **{variacao:+.1f}%**)."
        )

    elif tipo == 'saturacao':
        plataforma = dados.get('plataforma', '')
        freq = dados.get('frequencia', 0)
        ctr_var = dados.get('ctr_variacao_pct', 0)
        return (
            f"\u26a0\ufe0f **{plataforma}** apresenta sinais de saturacao: "
            f"frequencia em **{freq:.1f}** e CTR caiu **{abs(ctr_var):.1f}%** no periodo. "
            f"Considere renovar criativos ou pausar campanhas de baixo desempenho."
        )

    elif tipo == 'funil':
        etapa_perda = dados.get('etapa_maior_perda', '')
        pct_perda = dados.get('pct_perda', 0)
        return (
            f"A maior queda no funil ocorre entre **{etapa_perda}** "
            f"(**{pct_perda:.0f}%** de perda). "
            f"Otimizar essa etapa pode ter o maior impacto no resultado final."
        )

    elif tipo == 'melhor_segmento':
        segmento = dados.get('segmento', '')
        plataforma = dados.get('plataforma', '')
        roas = dados.get('roas', 0)
        return (
            f"O segmento **{segmento}** no **{plataforma}** tem o melhor ROAS "
            f"(**{roas:.1f}x**). Considere aumentar investimento neste publico."
        )

    return ''


def explicacao_grafico(titulo, texto):
    """Retorna HTML de caixa de explicacao abaixo do grafico."""
    return f"""
    <div style="background-color:#f0f7ff; border-left:4px solid #226275; padding:12px 16px;
                margin:8px 0 16px 0; border-radius:0 8px 8px 0; font-size:0.85rem;">
        <strong>{titulo}</strong><br>
        <span style="color:#444;">{texto}</span>
    </div>
    """


# =============================================================================
# C. RECOMENDACOES DE VERBA
# =============================================================================

def classificar_recomendacao(roas_atual, roas_medio, tendencia_7d_vs_30d):
    """Classifica plataforma em AUMENTAR / MANTER / DIMINUIR."""
    if roas_atual >= roas_medio * 1.2 and tendencia_7d_vs_30d >= 0:
        return 'AUMENTAR', COR_VERDE
    elif roas_atual <= roas_medio * 0.8 or tendencia_7d_vs_30d < -0.15:
        return 'DIMINUIR', COR_VERMELHO
    else:
        return 'MANTER', COR_AMARELO


def render_card_recomendacao(plataforma, acao, cor, motivo, roas):
    """Retorna HTML de card de recomendacao."""
    icones = {'AUMENTAR': '\u2b06\ufe0f', 'MANTER': '\u27a1\ufe0f', 'DIMINUIR': '\u2b07\ufe0f'}
    icone = icones.get(acao, '')
    return f"""
    <div style="border:2px solid {cor}; border-radius:12px; padding:16px; margin:8px 0;
                background:linear-gradient(135deg, {cor}10, white);">
        <div style="font-size:1.1rem; font-weight:bold; color:{cor};">
            {icone} {acao} — {plataforma}
        </div>
        <div style="font-size:0.9rem; color:#555; margin-top:4px;">
            ROAS: <strong>{roas:.1f}x</strong> | {motivo}
        </div>
    </div>
    """


# =============================================================================
# D. DETECCAO DE ANOMALIAS
# =============================================================================

def detectar_anomalias(df, coluna_valor, coluna_data='data', janela=7, desvios=2.0):
    """
    Detecta anomalias usando desvio da media movel.
    Retorna DataFrame com anomalias detectadas.
    """
    if df.empty or coluna_valor not in df.columns or coluna_data not in df.columns:
        return pd.DataFrame()

    df = df.sort_values(coluna_data).copy()
    df['media_movel'] = df[coluna_valor].rolling(window=janela, min_periods=3).mean()
    df['std_movel'] = df[coluna_valor].rolling(window=janela, min_periods=3).std()

    df['limite_superior'] = df['media_movel'] + desvios * df['std_movel']
    df['limite_inferior'] = df['media_movel'] - desvios * df['std_movel']

    anomalias = df[
        (df[coluna_valor] > df['limite_superior']) |
        (df[coluna_valor] < df['limite_inferior'])
    ].copy()

    if anomalias.empty:
        return pd.DataFrame()

    anomalias['desvio_pct'] = (
        (anomalias[coluna_valor] - anomalias['media_movel']) / anomalias['media_movel'] * 100
    )
    anomalias['severidade'] = anomalias['desvio_pct'].abs().apply(
        lambda x: 'Alta' if x > 50 else ('Media' if x > 25 else 'Baixa')
    )

    return anomalias[[coluna_data, coluna_valor, 'media_movel', 'desvio_pct', 'severidade']]


def render_alerta(data, metrica, valor, desvio_pct, severidade):
    """Retorna HTML de alerta de anomalia."""
    cores_sev = {'Alta': COR_VERMELHO, 'Media': COR_AMARELO, 'Baixa': '#3498DB'}
    cor = cores_sev.get(severidade, COR_AMARELO)
    direcao = '\u2b06\ufe0f' if desvio_pct > 0 else '\u2b07\ufe0f'
    return f"""
    <div style="border-left:4px solid {cor}; padding:10px 14px; margin:6px 0;
                background-color:{cor}10; border-radius:0 8px 8px 0;">
        <div style="font-size:0.8rem; color:#777;">{data}</div>
        <div style="font-size:0.95rem;">
            <strong>{metrica}</strong>: {valor:,.2f}
            <span style="color:{cor}; font-weight:bold;">
                {direcao} {abs(desvio_pct):.1f}% da media
            </span>
            <span style="background:{cor}; color:white; padding:2px 8px;
                         border-radius:10px; font-size:0.75rem; margin-left:8px;">
                {severidade}
            </span>
        </div>
    </div>
    """
