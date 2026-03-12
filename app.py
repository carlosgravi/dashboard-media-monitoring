"""
Dashboard Media Monitoring — Almeida Junior
Painel unificado de performance de midia digital (Google Ads, Meta Ads, TikTok Ads, GA4, Search Console).
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from pathlib import Path
from datetime import datetime, timedelta
import bcrypt
import time
import os

from insights_midia import (
    semaforo_roas, semaforo_ctr, semaforo_cpa, semaforo_frequencia,
    semaforo_bounce_rate, render_semaforo,
    gerar_insight_box, explicacao_grafico,
    classificar_recomendacao, render_card_recomendacao,
    detectar_anomalias, render_alerta,
    COR_VERDE, COR_AMARELO, COR_VERMELHO,
)
from explicacoes_graficos import render_explicacao, EXPLICACOES

# =============================================================================
# CONFIG
# =============================================================================
st.set_page_config(
    page_title="Media Monitoring — Almeida Junior",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = Path(__file__).resolve().parent
DADOS_DIR = BASE_DIR / "Dados"

# Cores identidade visual AJ
AZUL_NAVY = "#031835"
CINZA_GELO = "#dfe2e6"
ACCENT = "#226275"
ACCENT2 = "#0f3643"

CORES_PLATAFORMA = {
    'Google Ads': '#4285F4',
    'Meta Ads': '#1877F2',
    'TikTok Ads': '#000000',
    'GA4': '#E37400',
    'Organico': '#34A853',
}

# Paginas do dashboard
PAGINAS = {
    "Visao Geral": ["Resumo Executivo", "Tendencias"],
    "Por Plataforma": ["Google Ads", "Meta Ads", "TikTok Ads", "GA4 / Search Console", "Organico"],
    "Cross-Platform": ["Comparativo", "Funil Integrado", "Audiencia"],
    "Otimizacao": ["Onde Investir", "Alertas e Anomalias"],
    "Ferramentas": ["Documentacao"],
}

TODAS_PAGINAS = [p for grupo in PAGINAS.values() for p in grupo]


# =============================================================================
# PLOTLY TEMPLATE
# =============================================================================
pio.templates["dashboard_media"] = go.layout.Template(layout=go.Layout(
    font=dict(family="Inter, Helvetica Neue, Arial, sans-serif", size=12, color=AZUL_NAVY),
    margin=dict(t=40, b=30, l=40, r=20),
    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5, font=dict(size=10)),
    xaxis=dict(tickfont=dict(size=10), title=dict(font=dict(size=11)), automargin=True, gridcolor="#ebedf0"),
    yaxis=dict(tickfont=dict(size=10), title=dict(font=dict(size=11)), automargin=True, gridcolor="#ebedf0"),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    autosize=True,
))
pio.templates.default = "plotly+dashboard_media"


def render_chart(fig, key=None):
    st.plotly_chart(fig, use_container_width=True, config={'responsive': True, 'displayModeBar': False}, key=key)


# =============================================================================
# CSS
# =============================================================================
def injetar_css():
    st.markdown("""
    <style>
    /* KPI cards */
    .kpi-card {
        background: white;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border-left: 4px solid #226275;
        text-align: center;
    }
    .kpi-card .valor {
        font-size: 1.8rem;
        font-weight: 700;
        color: #031835;
    }
    .kpi-card .label {
        font-size: 0.85rem;
        color: #666;
        margin-top: 4px;
    }

    /* Semaforo badge */
    .semaforo-badge {
        display: inline-block;
        font-size: 0.85rem;
        font-weight: 500;
    }

    /* Navegacao sidebar */
    [data-testid="stSidebar"] {
        background-color: #031835;
    }
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] h4,
    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] summary,
    [data-testid="stSidebar"] summary * {
        color: white !important;
    }
    [data-testid="stSidebar"] .stSelectbox label {
        color: #dfe2e6 !important;
    }
    /* Botoes de navegacao no sidebar */
    [data-testid="stSidebar"] button[kind="secondary"] {
        background-color: transparent !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        color: white !important;
    }
    [data-testid="stSidebar"] button[kind="secondary"]:hover {
        background-color: rgba(255,255,255,0.1) !important;
        border-color: rgba(255,255,255,0.5) !important;
    }
    /* Expander no sidebar */
    [data-testid="stSidebar"] [data-testid="stExpander"] {
        background-color: transparent !important;
        border: none !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] details {
        background-color: transparent !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary {
        background-color: rgba(255,255,255,0.1) !important;
        border-radius: 8px;
        padding: 8px 12px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary:hover {
        background-color: rgba(255,255,255,0.15) !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] button {
        background-color: transparent !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        color: white !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] button:hover {
        background-color: rgba(255,255,255,0.1) !important;
        border-color: rgba(255,255,255,0.5) !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] button p {
        color: white !important;
    }

    /* Recomendacao cards */
    .rec-card {
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
    }

    /* Responsive */
    @media (max-width: 768px) {
        .kpi-card .valor { font-size: 1.3rem; }
        .kpi-card .label { font-size: 0.75rem; }
    }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# AUTENTICACAO (bcrypt)
# =============================================================================
def validar_credenciais(username, password):
    """Valida credenciais contra secrets.toml."""
    try:
        usuarios = st.secrets.get("usuarios", {})
        if username not in usuarios:
            return False, None, None, None

        user = usuarios[username]
        senha_hash = user.get("senha_hash", "")
        if bcrypt.checkpw(password.encode('utf-8'), senha_hash.encode('utf-8')):
            return True, user.get("nome", username), user.get("role", "viewer"), user
        return False, None, None, None
    except Exception:
        return False, None, None, None


def tela_login():
    """Renderiza tela de login."""
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        logo_path = BASE_DIR / "AJ.jpg"
        if logo_path.exists():
            st.image(str(logo_path), width=200)
        st.markdown("### Media Monitoring")
        st.markdown("Painel de performance de midia digital")

        with st.form("login_form"):
            username = st.text_input("Usuario")
            password = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar", use_container_width=True)

        if submitted and username and password:
            sucesso, nome, role, user_config = validar_credenciais(username, password)
            if sucesso:
                st.session_state['autenticado'] = True
                st.session_state['usuario'] = username
                st.session_state['nome'] = nome
                st.session_state['role'] = role
                st.session_state['user_config'] = user_config or {}
                st.rerun()
            else:
                st.error("Usuario ou senha incorretos.")


def get_paginas_permitidas():
    """Retorna lista de paginas que o usuario pode acessar."""
    user_config = st.session_state.get('user_config', {})
    paginas = user_config.get('paginas', None)
    if paginas:
        return [p for p in paginas if p in TODAS_PAGINAS]
    return TODAS_PAGINAS


# =============================================================================
# CARREGAMENTO DE DADOS
# =============================================================================
@st.cache_data(ttl=3600)
def carregar_csv(caminho_relativo):
    """Carrega CSV com cache de 1h."""
    caminho = DADOS_DIR / caminho_relativo
    if not caminho.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(caminho, encoding='utf-8-sig')
    except Exception:
        try:
            df = pd.read_csv(caminho, encoding='utf-8')
        except Exception:
            return pd.DataFrame()
    # Converter coluna data se existir
    for col in ['data', 'date', 'date_start', 'stat_time_day']:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass
    return df


def carregar_dados_plataforma(plataforma):
    """Carrega todos os CSVs de uma plataforma."""
    dados = {}
    pasta = DADOS_DIR / plataforma
    if pasta.exists():
        for csv in pasta.glob("*.csv"):
            nome = csv.stem
            dados[nome] = carregar_csv(f"{plataforma}/{csv.name}")
    return dados


# =============================================================================
# COMPONENTES REUTILIZAVEIS
# =============================================================================
def render_kpi(label, valor, formato="numero", delta=None):
    """Renderiza card KPI."""
    if formato == "moeda":
        valor_fmt = f"R$ {valor:,.2f}"
    elif formato == "pct":
        valor_fmt = f"{valor:.2f}%"
    elif formato == "multiplicador":
        valor_fmt = f"{valor:.1f}x"
    elif formato == "inteiro":
        valor_fmt = f"{valor:,.0f}"
    else:
        valor_fmt = f"{valor:,.2f}"

    delta_html = ""
    if delta is not None:
        cor_delta = COR_VERDE if delta >= 0 else COR_VERMELHO
        seta = "\u2191" if delta >= 0 else "\u2193"
        delta_html = f'<div style="color:{cor_delta}; font-size:0.8rem;">{seta} {abs(delta):.1f}%</div>'

    st.markdown(f"""
    <div class="kpi-card">
        <div class="valor">{valor_fmt}</div>
        <div class="label">{label}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def filtro_periodo_sidebar(df, col_data='data'):
    """Adiciona filtro de periodo no sidebar e retorna df filtrado."""
    if df.empty or col_data not in df.columns:
        return df

    data_min = df[col_data].min()
    data_max = df[col_data].max()

    periodo = st.sidebar.selectbox("Periodo", [
        "Ultimos 7 dias", "Ultimos 30 dias", "Ultimos 90 dias", "Personalizado"
    ], index=1)

    if periodo == "Ultimos 7 dias":
        inicio = data_max - timedelta(days=7)
    elif periodo == "Ultimos 30 dias":
        inicio = data_max - timedelta(days=30)
    elif periodo == "Ultimos 90 dias":
        inicio = data_max - timedelta(days=90)
    else:
        datas = st.sidebar.date_input("Selecione o periodo",
                                       value=(data_min.date(), data_max.date()),
                                       min_value=data_min.date(), max_value=data_max.date())
        if isinstance(datas, tuple) and len(datas) == 2:
            inicio = pd.Timestamp(datas[0])
            data_max = pd.Timestamp(datas[1])
        else:
            inicio = data_min

    return df[(df[col_data] >= inicio) & (df[col_data] <= data_max)]


def filtro_shopping_sidebar(df, col='shopping', key='shopping_filter'):
    """Adiciona filtro de shopping no sidebar e retorna df filtrado."""
    if df.empty or col not in df.columns:
        return df

    shoppings = sorted(df[col].dropna().unique().tolist())
    if not shoppings:
        return df

    opcoes = ["Todos"] + shoppings
    shopping_sel = st.sidebar.selectbox("Shopping", opcoes, index=0, key=key)

    if shopping_sel != "Todos":
        return df[df[col] == shopping_sel]
    return df


def _aplicar_filtro_shopping(df, shopping_sel, col='shopping'):
    """Aplica filtro de shopping ja selecionado (sem criar widget)."""
    if shopping_sel and shopping_sel != "Todos" and col in df.columns:
        return df[df[col] == shopping_sel]
    return df


# =============================================================================
# PAGINA: RESUMO EXECUTIVO
# =============================================================================
def pagina_resumo_executivo():
    st.title("Resumo Executivo")

    # Tentar carregar dados com shopping; fallback para diario sem shopping
    df_shopping = carregar_csv("Consolidado/cross_platform_shopping_diario.csv")
    df_diario = carregar_csv("Consolidado/cross_platform_diario.csv")

    if df_diario.empty and df_shopping.empty:
        st.info("Sem dados consolidados disponiveis. Execute os scripts de extracao.")
        return

    # Usar dados com shopping se disponiveis, senao fallback
    if not df_shopping.empty and 'shopping' in df_shopping.columns:
        df = filtro_periodo_sidebar(df_shopping)
        df = filtro_shopping_sidebar(df, key="resumo_shopping")
    else:
        df = filtro_periodo_sidebar(df_diario)

    # KPIs
    investimento = df['custo'].sum()
    conversoes = df['conversoes'].sum()
    receita = df['receita'].sum()
    roas_blended = receita / investimento if investimento > 0 else 0
    cpa_medio = investimento / conversoes if conversoes > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_kpi("Investimento Total", investimento, "moeda")
    with c2:
        render_kpi("Conversoes", conversoes, "inteiro")
    with c3:
        render_kpi("ROAS Blended", roas_blended, "multiplicador")
    with c4:
        render_kpi("CPA Medio", cpa_medio, "moeda")

    render_explicacao(EXPLICACOES['resumo']['kpis'])

    # Semaforos
    st.markdown("---")
    s1, s2, s3 = st.columns(3)
    with s1:
        cor, msg = semaforo_roas(roas_blended)
        st.markdown(render_semaforo(cor, msg), unsafe_allow_html=True)
    with s2:
        ctr_medio = (df['cliques'].sum() / max(df['impressoes'].sum(), 1)) * 100
        cor, msg = semaforo_ctr(ctr_medio)
        st.markdown(render_semaforo(cor, msg), unsafe_allow_html=True)
    with s3:
        cor, msg = semaforo_cpa(cpa_medio)
        st.markdown(render_semaforo(cor, msg), unsafe_allow_html=True)

    st.markdown("---")

    # Distribuicao de verba por plataforma
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribuicao de Verba por Plataforma")
        df_verba = df.groupby('plataforma')['custo'].sum().reset_index()
        df_verba['pct'] = df_verba['custo'] / df_verba['custo'].sum() * 100
        fig = px.pie(df_verba, values='custo', names='plataforma',
                     color='plataforma', color_discrete_map=CORES_PLATAFORMA,
                     hole=0.4)
        fig.update_traces(textinfo='label+percent', textposition='outside')
        render_chart(fig, key="verba_pie")

        render_explicacao(EXPLICACOES['resumo']['distribuicao_verba'])

        # Insight box
        if not df_verba.empty:
            maior = df_verba.loc[df_verba['custo'].idxmax()]
            roas_maior = df[df['plataforma'] == maior['plataforma']]
            roas_val = roas_maior['receita'].sum() / max(roas_maior['custo'].sum(), 1)
            st.markdown(gerar_insight_box('distribuicao_verba', {
                'maior_plataforma': maior['plataforma'],
                'pct_maior': maior['pct'],
                'roas_maior': roas_val,
            }))

    with col2:
        st.subheader("Investimento x Conversoes")
        df_evol = df.groupby('data').agg({'custo': 'sum', 'conversoes': 'sum'}).reset_index()
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df_evol['data'], y=df_evol['custo'], name='Investimento',
                             marker_color=ACCENT, opacity=0.6))
        fig.add_trace(go.Scatter(x=df_evol['data'], y=df_evol['conversoes'], name='Conversoes',
                                 yaxis='y2', line=dict(color=COR_VERDE, width=2)))
        fig.update_layout(
            yaxis=dict(title='Investimento (R$)'),
            yaxis2=dict(title='Conversoes', overlaying='y', side='right'),
            barmode='stack',
        )
        render_chart(fig, key="invest_conv")

    # Treemap campanhas
    st.subheader("Treemap — Investimento por Campanha")
    # Combinar campanhas de todas as plataformas
    dfs_camp = []
    for plataforma, pasta in [('Google Ads', 'Google_Ads'), ('Meta Ads', 'Meta_Ads'), ('TikTok Ads', 'TikTok_Ads')]:
        df_c = carregar_csv(f"{pasta}/campanhas.csv")
        if not df_c.empty:
            col_camp = 'campanha' if 'campanha' in df_c.columns else 'campaign_name'
            col_custo = 'custo' if 'custo' in df_c.columns else 'spend'
            if col_camp in df_c.columns and col_custo in df_c.columns:
                df_agg = df_c.groupby(col_camp)[col_custo].sum().reset_index()
                df_agg.columns = ['campanha', 'custo']
                df_agg['plataforma'] = plataforma
                dfs_camp.append(df_agg)

    if dfs_camp:
        df_treemap = pd.concat(dfs_camp)
        df_treemap = df_treemap[df_treemap['custo'] > 0]
        if not df_treemap.empty:
            fig = px.treemap(df_treemap, path=['plataforma', 'campanha'], values='custo',
                             color='plataforma', color_discrete_map=CORES_PLATAFORMA)
            fig.update_layout(margin=dict(t=30, l=10, r=10, b=10))
            render_chart(fig, key="treemap_camp")
            render_explicacao(EXPLICACOES['resumo']['treemap'])
    else:
        st.info("Sem dados de campanhas disponiveis.")


# =============================================================================
# PAGINA: TENDENCIAS
# =============================================================================
def pagina_tendencias():
    st.title("Tendencias")

    df_diario = carregar_csv("Consolidado/cross_platform_diario.csv")
    if df_diario.empty:
        st.info("Sem dados consolidados disponiveis.")
        return

    df = filtro_periodo_sidebar(df_diario)

    # Evolucao mensal ROAS por plataforma
    st.subheader("ROAS por Plataforma — Evolucao Mensal")
    df['mes'] = df['data'].dt.to_period('M').astype(str)
    df_mensal = df.groupby(['mes', 'plataforma']).agg(
        custo=('custo', 'sum'), receita=('receita', 'sum')
    ).reset_index()
    df_mensal['roas'] = np.where(df_mensal['custo'] > 0, df_mensal['receita'] / df_mensal['custo'], 0)

    fig = px.line(df_mensal, x='mes', y='roas', color='plataforma',
                  color_discrete_map=CORES_PLATAFORMA, markers=True)
    fig.update_layout(yaxis_title='ROAS', xaxis_title='Mes')
    render_chart(fig, key="roas_mensal")
    render_explicacao(EXPLICACOES['tendencias']['roas_cpa'])

    # CPA por plataforma
    st.subheader("CPA por Plataforma — Evolucao Mensal")
    df_mensal['cpa'] = np.where(df_mensal['custo'] > 0,
                                 df_mensal['custo'] / df_mensal.get('conversoes', pd.Series([1]*len(df_mensal))),
                                 0)
    # Recalcular com conversoes
    df_mensal_conv = df.groupby(['mes', 'plataforma']).agg(
        custo=('custo', 'sum'), conversoes=('conversoes', 'sum')
    ).reset_index()
    df_mensal_conv['cpa'] = np.where(df_mensal_conv['conversoes'] > 0,
                                      df_mensal_conv['custo'] / df_mensal_conv['conversoes'], 0)

    fig = px.line(df_mensal_conv, x='mes', y='cpa', color='plataforma',
                  color_discrete_map=CORES_PLATAFORMA, markers=True)
    fig.update_layout(yaxis_title='CPA (R$)', xaxis_title='Mes')
    render_chart(fig, key="cpa_mensal")

    # Area empilhada investimento
    st.subheader("Investimento por Plataforma — Area Empilhada")
    df_area = df.groupby(['data', 'plataforma'])['custo'].sum().reset_index()
    fig = px.area(df_area, x='data', y='custo', color='plataforma',
                  color_discrete_map=CORES_PLATAFORMA)
    fig.update_layout(yaxis_title='Investimento (R$)', xaxis_title='Data')
    render_chart(fig, key="area_invest")
    render_explicacao(EXPLICACOES['tendencias']['area_empilhada'])

    # Heatmap dia da semana x hora (se houver dados horarios)
    st.subheader("Performance por Dia da Semana")
    df['dia_semana'] = df['data'].dt.day_name()
    dias_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df_dia = df.groupby('dia_semana').agg(
        custo=('custo', 'sum'), conversoes=('conversoes', 'sum'), cliques=('cliques', 'sum')
    ).reindex(dias_ordem).reset_index()
    df_dia['cpa'] = np.where(df_dia['conversoes'] > 0, df_dia['custo'] / df_dia['conversoes'], 0)

    fig = go.Figure(data=[
        go.Bar(x=df_dia['dia_semana'], y=df_dia['conversoes'], name='Conversoes', marker_color=COR_VERDE),
        go.Bar(x=df_dia['dia_semana'], y=df_dia['custo'], name='Custo (R$)', marker_color=ACCENT),
    ])
    fig.update_layout(barmode='group', xaxis_title='Dia da Semana')
    render_chart(fig, key="heatmap_dia")
    render_explicacao(EXPLICACOES['tendencias']['dia_semana'])


# =============================================================================
# PAGINA: GOOGLE ADS
# =============================================================================
def pagina_google_ads():
    st.title("Google Ads")

    dados = carregar_dados_plataforma("Google_Ads")
    if not dados:
        st.info("Sem dados do Google Ads. Execute `scripts/extrair_google_ads.py`.")
        return

    # Filtros unicos (antes dos tabs) — periodo e shopping
    df_ref = dados.get('campanhas', pd.DataFrame())
    shopping_sel = None
    if not df_ref.empty and 'shopping' in df_ref.columns:
        shoppings = sorted(df_ref['shopping'].dropna().unique().tolist())
        if shoppings:
            opcoes = ["Todos"] + shoppings
            shopping_sel = st.sidebar.selectbox("Shopping", opcoes, index=0, key="gads_shopping")

    # Filtro periodo unico (aplicado em todas as tabs que tem coluna 'data')
    if not df_ref.empty:
        df_ref = filtro_periodo_sidebar(df_ref)
        _gads_periodo_inicio = df_ref['data'].min() if 'data' in df_ref.columns and not df_ref.empty else None
        _gads_periodo_fim = df_ref['data'].max() if 'data' in df_ref.columns and not df_ref.empty else None
    else:
        _gads_periodo_inicio = None
        _gads_periodo_fim = None

    def _aplicar_periodo_gads(df):
        """Aplica o mesmo filtro de periodo selecionado no sidebar."""
        if _gads_periodo_inicio is not None and 'data' in df.columns:
            df['data'] = pd.to_datetime(df['data'], errors='coerce')
            return df[(df['data'] >= _gads_periodo_inicio) & (df['data'] <= _gads_periodo_fim)]
        return df

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "Campanhas", "Palavras-Chave", "Demografico", "Geografico", "Dispositivos",
        "Search Terms", "Hora / Dia", "Alcance / Frequencia"
    ])

    with tab1:
        df = dados.get('campanhas', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de campanhas.")
        else:
            df = _aplicar_periodo_gads(df)
            df = _aplicar_filtro_shopping(df, shopping_sel)
            # KPIs
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                render_kpi("Investimento", df['custo'].sum(), "moeda")
            with c2:
                render_kpi("Cliques", df['cliques'].sum(), "inteiro")
            with c3:
                render_kpi("Conversoes", df['conversoes'].sum(), "inteiro")
            with c4:
                custo_t = df['custo'].sum()
                valor_t = df['valor_conversoes'].sum()
                render_kpi("ROAS", valor_t / custo_t if custo_t > 0 else 0, "multiplicador")

            st.markdown("---")

            # Tabela campanhas
            df_camp = df.groupby('campanha').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum',
                'conversoes': 'sum', 'valor_conversoes': 'sum'
            }).reset_index()
            df_camp['ctr'] = np.where(df_camp['impressoes'] > 0, df_camp['cliques'] / df_camp['impressoes'] * 100, 0)
            df_camp['roas'] = np.where(df_camp['custo'] > 0, df_camp['valor_conversoes'] / df_camp['custo'], 0)
            df_camp['cpa'] = np.where(df_camp['conversoes'] > 0, df_camp['custo'] / df_camp['conversoes'], 0)
            df_camp = df_camp.sort_values('custo', ascending=False)
            st.dataframe(df_camp.style.format({
                'custo': 'R$ {:.2f}', 'valor_conversoes': 'R$ {:.2f}',
                'ctr': '{:.2f}%', 'roas': '{:.1f}x', 'cpa': 'R$ {:.2f}'
            }), use_container_width=True)
            render_explicacao(EXPLICACOES['google_ads']['campanhas'])

    with tab2:
        df = dados.get('keywords', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de keywords.")
        else:
            df = _aplicar_filtro_shopping(df, shopping_sel)
            st.subheader("Top Keywords por Custo")
            df_kw = df.groupby('keyword').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum',
                'conversoes': 'sum', 'quality_score': 'mean'
            }).reset_index().sort_values('custo', ascending=False).head(50)
            df_kw['ctr'] = np.where(df_kw['impressoes'] > 0, df_kw['cliques'] / df_kw['impressoes'] * 100, 0)
            df_kw['cpa'] = np.where(df_kw['conversoes'] > 0, df_kw['custo'] / df_kw['conversoes'], 0)
            st.dataframe(df_kw, use_container_width=True)

            # Quality Score distribution
            if 'quality_score' in df_kw.columns and df_kw['quality_score'].notna().any():
                st.subheader("Distribuicao Quality Score")
                fig = px.histogram(df_kw.dropna(subset=['quality_score']),
                                   x='quality_score', nbins=10, color_discrete_sequence=[ACCENT])
                fig.update_layout(xaxis_title='Quality Score (1-10)', yaxis_title='Quantidade')
                render_chart(fig, key="qs_hist")
            render_explicacao(EXPLICACOES['google_ads']['keywords'])

    with tab3:
        df = dados.get('demografico', pd.DataFrame())
        if df.empty:
            st.info("Sem dados demograficos.")
        else:
            df = _aplicar_filtro_shopping(df, shopping_sel)
            st.subheader("Performance por Faixa Etaria")
            df_age = df[df['tipo'] == 'faixa_etaria'].groupby('segmento').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum', 'conversoes': 'sum'
            }).reset_index()
            if not df_age.empty:
                fig = px.bar(df_age, x='segmento', y='conversoes', color_discrete_sequence=[ACCENT])
                render_chart(fig, key="ga_age")

            st.subheader("Performance por Genero")
            df_gen = df[df['tipo'] == 'genero'].groupby('segmento').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum', 'conversoes': 'sum'
            }).reset_index()
            if not df_gen.empty:
                fig = px.bar(df_gen, x='segmento', y='conversoes', color_discrete_sequence=[ACCENT2])
                render_chart(fig, key="ga_gen")
            render_explicacao(EXPLICACOES['google_ads']['demografico'])

    with tab4:
        df = dados.get('geografico', pd.DataFrame())
        if df.empty:
            st.info("Sem dados geograficos.")
        else:
            df = _aplicar_filtro_shopping(df, shopping_sel)
            st.subheader("Top Cidades por Investimento")
            df_geo = df.groupby('cidade').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum', 'conversoes': 'sum'
            }).reset_index().sort_values('custo', ascending=False).head(20)
            fig = px.bar(df_geo, x='custo', y='cidade', orientation='h',
                         color_discrete_sequence=[ACCENT])
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            render_chart(fig, key="ga_geo")
            render_explicacao(EXPLICACOES['google_ads']['geografico'])

    with tab5:
        df = dados.get('dispositivos', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de dispositivos.")
        else:
            df = _aplicar_filtro_shopping(df, shopping_sel)
            st.subheader("Performance por Dispositivo")
            df_dev = df.groupby('dispositivo').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum', 'conversoes': 'sum'
            }).reset_index()
            if not df_dev.empty:
                fig = px.pie(df_dev, values='custo', names='dispositivo', hole=0.4,
                             color_discrete_sequence=[ACCENT, ACCENT2, COR_VERDE, COR_AMARELO])
                render_chart(fig, key="ga_dev")
            render_explicacao(EXPLICACOES['google_ads']['dispositivos'])

    with tab6:
        df = dados.get('search_terms', pd.DataFrame())
        if df.empty:
            df = carregar_csv("Google_Ads/search_terms.csv")
        if df.empty:
            st.info("Sem dados de search terms.")
        else:
            df = _aplicar_filtro_shopping(df, shopping_sel)
            st.subheader("Top 50 Termos de Busca")
            col_termo = 'termo_busca' if 'termo_busca' in df.columns else 'search_term'
            col_imp = 'impressoes' if 'impressoes' in df.columns else 'impressions'
            col_cli = 'cliques' if 'cliques' in df.columns else 'clicks'
            col_custo = 'custo' if 'custo' in df.columns else 'cost'
            col_conv = 'conversoes' if 'conversoes' in df.columns else 'conversions'
            col_ctr = 'ctr' if 'ctr' in df.columns else None

            agg_cols = {}
            for c in [col_imp, col_cli, col_custo, col_conv]:
                if c in df.columns:
                    agg_cols[c] = 'sum'
            if col_ctr and col_ctr in df.columns:
                agg_cols[col_ctr] = 'mean'

            if col_termo in df.columns and agg_cols:
                df_st = df.groupby(col_termo).agg(agg_cols).reset_index()
                # Recalcular CTR se temos impressoes e cliques
                if col_imp in df_st.columns and col_cli in df_st.columns:
                    df_st['ctr_calc'] = np.where(df_st[col_imp] > 0,
                                                  df_st[col_cli] / df_st[col_imp] * 100, 0)
                df_st = df_st.sort_values(col_custo if col_custo in df_st.columns else col_imp,
                                           ascending=False).head(50)
                st.dataframe(df_st, use_container_width=True)
            else:
                st.dataframe(df.head(50), use_container_width=True)
            render_explicacao(EXPLICACOES['google_ads']['search_terms'])

    with tab7:
        df = dados.get('hora_dia', pd.DataFrame())
        if df.empty:
            df = carregar_csv("Google_Ads/hora_dia.csv")
        if df.empty:
            st.info("Sem dados de hora/dia.")
        else:
            df = _aplicar_filtro_shopping(df, shopping_sel)
            st.subheader("Heatmap — Performance por Hora x Dia da Semana")

            col_hora = 'hora' if 'hora' in df.columns else 'hour'
            col_dia = 'dia_semana' if 'dia_semana' in df.columns else 'day_of_week'
            col_metrica = None
            for c in ['conversoes', 'conversions', 'cliques', 'clicks', 'impressoes', 'impressions']:
                if c in df.columns:
                    col_metrica = c
                    break

            if col_hora in df.columns and col_dia in df.columns and col_metrica:
                pivot = df.pivot_table(index=col_hora, columns=col_dia, values=col_metrica,
                                        aggfunc='sum', fill_value=0)
                # Reordenar dias se possivel
                dias_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                dias_pt = ['Segunda', 'Terca', 'Quarta', 'Quinta', 'Sexta', 'Sabado', 'Domingo']
                cols_existentes = [d for d in dias_ordem + dias_pt if d in pivot.columns]
                if cols_existentes:
                    pivot = pivot[cols_existentes]

                fig = px.imshow(pivot, text_auto=True, aspect='auto',
                                color_continuous_scale='YlOrRd',
                                labels=dict(x='Dia da Semana', y='Hora', color=col_metrica.capitalize()))
                fig.update_layout(xaxis_title='Dia da Semana', yaxis_title='Hora')
                render_chart(fig, key="ga_hora_dia")
            else:
                st.info("Dados de hora/dia nao possuem as colunas esperadas (hora, dia_semana).")
                st.dataframe(df.head(20), use_container_width=True)
            render_explicacao(EXPLICACOES['google_ads']['hora_dia'])

    with tab8:
        df = dados.get('alcance_frequencia', pd.DataFrame())
        if df.empty:
            df = carregar_csv("Google_Ads/alcance_frequencia.csv")
        if df.empty:
            st.info("Sem dados de alcance e frequencia. Disponivel apenas para campanhas Display, Video e Demand Gen com 10k+ impressoes.")
        else:
            df = _aplicar_periodo_gads(df)
            df = _aplicar_filtro_shopping(df, shopping_sel)

            # KPIs
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                render_kpi("Alcance Total", df['alcance'].sum(), "inteiro")
            with c2:
                alcance_t = df.groupby('data')['alcance'].sum()
                freq_media = df['frequencia'].mean() if not df.empty else 0
                render_kpi("Frequencia Media", freq_media, "decimal")
            with c3:
                render_kpi("Impressoes", df['impressoes'].sum(), "inteiro")
            with c4:
                render_kpi("Investimento", df['custo'].sum(), "moeda")

            st.markdown("---")

            # Evolucao alcance e frequencia diaria
            st.subheader("Evolucao Diaria — Alcance e Frequencia")
            if 'data' in df.columns:
                df_daily = df.groupby('data').agg({
                    'alcance': 'sum', 'frequencia': 'mean', 'impressoes': 'sum'
                }).reset_index()
                df_daily = df_daily.sort_values('data')

                fig = go.Figure()
                fig.add_trace(go.Bar(x=df_daily['data'], y=df_daily['alcance'],
                                      name='Alcance (pessoas)', marker_color=ACCENT,
                                      yaxis='y'))
                fig.add_trace(go.Scatter(x=df_daily['data'], y=df_daily['frequencia'],
                                          name='Frequencia Media', mode='lines+markers',
                                          marker_color='#FF6B6B', line_width=2,
                                          yaxis='y2'))
                fig.update_layout(
                    yaxis=dict(title='Alcance (pessoas)', side='left'),
                    yaxis2=dict(title='Frequencia Media', side='right', overlaying='y'),
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                    hovermode='x unified',
                )
                render_chart(fig, key="ga_reach_daily")

            # Por campanha
            st.subheader("Alcance por Campanha")
            df_camp = df.groupby('campanha').agg({
                'alcance': 'sum', 'impressoes': 'sum', 'frequencia': 'mean',
                'custo': 'sum', 'cliques': 'sum', 'conversoes': 'sum'
            }).reset_index()
            df_camp['cpm_alcance'] = np.where(df_camp['alcance'] > 0,
                                                df_camp['custo'] / df_camp['alcance'] * 1000, 0)
            df_camp = df_camp.sort_values('alcance', ascending=False)

            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(df_camp, x='alcance', y='campanha', orientation='h',
                             color_discrete_sequence=[ACCENT],
                             title="Alcance por Campanha")
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                render_chart(fig, key="ga_reach_camp")
            with c2:
                fig = px.bar(df_camp, x='frequencia', y='campanha', orientation='h',
                             color_discrete_sequence=['#FF6B6B'],
                             title="Frequencia Media por Campanha")
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                render_chart(fig, key="ga_freq_camp")

            # Tabela detalhada
            st.subheader("Tabela Detalhada")
            st.dataframe(df_camp.rename(columns={
                'campanha': 'Campanha', 'alcance': 'Alcance', 'impressoes': 'Impressoes',
                'frequencia': 'Freq. Media', 'custo': 'Investimento', 'cliques': 'Cliques',
                'conversoes': 'Conversoes', 'cpm_alcance': 'CPM Alcance'
            }).style.format({
                'Investimento': 'R$ {:.2f}', 'Freq. Media': '{:.1f}',
                'CPM Alcance': 'R$ {:.2f}'
            }), use_container_width=True)

            # Por tipo de canal
            if 'tipo_canal' in df.columns and df['tipo_canal'].nunique() > 1:
                st.subheader("Alcance por Tipo de Campanha")
                df_tipo = df.groupby('tipo_canal').agg({
                    'alcance': 'sum', 'frequencia': 'mean', 'custo': 'sum'
                }).reset_index()
                fig = px.bar(df_tipo, x='tipo_canal', y='alcance', color='tipo_canal',
                             text='alcance', title="Alcance por Tipo de Canal")
                fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                render_chart(fig, key="ga_reach_tipo")

            render_explicacao(EXPLICACOES['google_ads']['alcance_frequencia'])


# =============================================================================
# PAGINA: META ADS
# =============================================================================
def pagina_meta_ads():
    st.title("Meta Ads (Facebook + Instagram)")

    dados = carregar_dados_plataforma("Meta_Ads")
    if not dados:
        st.info("Sem dados do Meta Ads. Execute `scripts/extrair_meta_ads.py`.")
        return

    # Filtro shopping unico (antes dos tabs)
    df_ref = dados.get('campanhas', pd.DataFrame())
    meta_shopping_sel = None
    if not df_ref.empty and 'shopping' in df_ref.columns:
        shoppings = sorted(df_ref['shopping'].dropna().unique().tolist())
        if shoppings:
            opcoes = ["Todos"] + shoppings
            meta_shopping_sel = st.sidebar.selectbox("Shopping", opcoes, index=0, key="meta_shopping")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Campanhas", "Plataformas", "Posicionamento", "Video", "Demografico"])

    with tab1:
        df = dados.get('campanhas', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de campanhas.")
        else:
            df = filtro_periodo_sidebar(df)
            df = _aplicar_filtro_shopping(df, meta_shopping_sel)
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                render_kpi("Investimento", df['custo'].sum(), "moeda")
            with c2:
                render_kpi("Alcance", df['alcance'].sum() if 'alcance' in df.columns else 0, "inteiro")
            with c3:
                freq_media = df['frequencia'].mean() if 'frequencia' in df.columns else 0
                render_kpi("Frequencia Media", freq_media, "numero")
                cor, msg = semaforo_frequencia(freq_media)
                st.markdown(render_semaforo(cor, msg), unsafe_allow_html=True)
            with c4:
                render_kpi("CPM Medio", df['cpm'].mean() if 'cpm' in df.columns else 0, "moeda")

            st.markdown("---")
            df_camp = df.groupby('campanha').agg({
                'impressoes': 'sum', 'alcance': 'sum', 'cliques': 'sum',
                'custo': 'sum', 'ctr': 'mean', 'cpm': 'mean',
            }).reset_index().sort_values('custo', ascending=False)
            st.dataframe(df_camp.style.format({
                'custo': 'R$ {:.2f}', 'ctr': '{:.2f}%', 'cpm': 'R$ {:.2f}'
            }), use_container_width=True)
            render_explicacao(EXPLICACOES['meta_ads']['campanhas'])

    with tab2:
        df = dados.get('plataforma', pd.DataFrame())
        if df.empty:
            st.info("Sem dados por plataforma.")
        else:
            st.subheader("Facebook vs Instagram vs Messenger")
            col_plt = 'publisher_platform' if 'publisher_platform' in df.columns else 'plataforma'
            if col_plt in df.columns:
                df_plt = df.groupby(col_plt).agg({
                    'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum'
                }).reset_index()
                fig = px.bar(df_plt, x=col_plt, y=['impressoes', 'cliques', 'custo'],
                             barmode='group', color_discrete_sequence=[ACCENT, COR_VERDE, COR_VERMELHO])
                render_chart(fig, key="meta_plt")
                render_explicacao(EXPLICACOES['meta_ads']['plataformas'])

    with tab3:
        df = dados.get('posicionamento', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de posicionamento.")
        else:
            st.subheader("Performance por Posicionamento (Feed, Stories, Reels)")
            col_pos = 'platform_position' if 'platform_position' in df.columns else 'posicionamento'
            if col_pos in df.columns:
                df_pos = df.groupby(col_pos).agg({
                    'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum'
                }).reset_index()
                df_pos['ctr'] = np.where(df_pos['impressoes'] > 0,
                                          df_pos['cliques'] / df_pos['impressoes'] * 100, 0)
                fig = px.bar(df_pos, x=col_pos, y='custo', color='ctr',
                             color_continuous_scale='RdYlGn')
                render_chart(fig, key="meta_pos")
                render_explicacao(EXPLICACOES['meta_ads']['posicionamento'])

    with tab4:
        df = dados.get('video', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de video.")
        else:
            st.subheader("Metricas de Video")
            video_cols = [c for c in df.columns if c.startswith('video_p')]
            if video_cols:
                totais = {c: df[c].sum() for c in video_cols}
                df_video = pd.DataFrame([
                    {'quartil': k.replace('video_', '').upper(), 'views': v}
                    for k, v in totais.items()
                ])
                fig = px.funnel(df_video, x='views', y='quartil')
                render_chart(fig, key="meta_video")
                render_explicacao(EXPLICACOES['meta_ads']['video'])

    with tab5:
        df_idade = dados.get('demografico_idade', pd.DataFrame())
        df_genero = dados.get('demografico_genero', pd.DataFrame())

        if not df_idade.empty and 'age' in df_idade.columns:
            st.subheader("Performance por Faixa Etaria")
            df_age = df_idade.groupby('age').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum'
            }).reset_index()
            fig = px.bar(df_age, x='age', y='custo', color_discrete_sequence=['#1877F2'])
            render_chart(fig, key="meta_age")

        if not df_genero.empty and 'gender' in df_genero.columns:
            st.subheader("Performance por Genero")
            df_gen = df_genero.groupby('gender').agg({
                'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum'
            }).reset_index()
            fig = px.pie(df_gen, values='custo', names='gender', hole=0.4,
                         color_discrete_sequence=['#1877F2', '#E1306C', '#999'])
            render_chart(fig, key="meta_gen")
        render_explicacao(EXPLICACOES['meta_ads']['demografico'])


# =============================================================================
# PAGINA: TIKTOK ADS
# =============================================================================
def pagina_tiktok_ads():
    st.title("TikTok Ads")

    dados = carregar_dados_plataforma("TikTok_Ads")
    if not dados:
        st.info("Sem dados do TikTok Ads. Execute `scripts/extrair_tiktok_ads.py`.")
        return

    tab1, tab2, tab3 = st.tabs(["Campanhas", "Video Engagement", "Demografico"])

    with tab1:
        df = dados.get('campanhas', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de campanhas.")
        else:
            col_custo = 'spend' if 'spend' in df.columns else 'custo'
            col_imp = 'impressions' if 'impressions' in df.columns else 'impressoes'
            col_cli = 'clicks' if 'clicks' in df.columns else 'cliques'
            col_conv = 'conversion' if 'conversion' in df.columns else 'conversoes'

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                render_kpi("Investimento", df[col_custo].sum() if col_custo in df.columns else 0, "moeda")
            with c2:
                render_kpi("Impressoes", df[col_imp].sum() if col_imp in df.columns else 0, "inteiro")
            with c3:
                render_kpi("Cliques", df[col_cli].sum() if col_cli in df.columns else 0, "inteiro")
            with c4:
                render_kpi("Conversoes", df[col_conv].sum() if col_conv in df.columns else 0, "inteiro")

            # Engagement metrics
            st.markdown("---")
            st.subheader("Engajamento")
            eng_cols = ['likes', 'comments', 'shares', 'follows', 'profile_visits']
            eng_data = {c: df[c].sum() for c in eng_cols if c in df.columns}
            if eng_data:
                cols = st.columns(len(eng_data))
                for i, (k, v) in enumerate(eng_data.items()):
                    with cols[i]:
                        render_kpi(k.capitalize(), v, "inteiro")
            render_explicacao(EXPLICACOES['tiktok_ads']['campanhas'])

    with tab2:
        df = dados.get('video_engagement', pd.DataFrame())
        if df.empty:
            st.info("Sem dados de video engagement.")
        else:
            st.subheader("Funil de Retencao de Video")
            quartis = ['video_views_p25', 'video_views_p50', 'video_views_p75', 'video_views_p100']
            labels = ['25%', '50%', '75%', '100%']
            vals = [df[q].sum() if q in df.columns else 0 for q in quartis]

            if any(v > 0 for v in vals):
                df_funil = pd.DataFrame({'Quartil': labels, 'Views': vals})
                fig = px.funnel(df_funil, x='Views', y='Quartil',
                                color_discrete_sequence=['#000000'])
                render_chart(fig, key="tt_video")
                render_explicacao(EXPLICACOES['tiktok_ads']['video'])

    with tab3:
        df_idade = dados.get('demografico_idade', pd.DataFrame())
        df_genero = dados.get('demografico_genero', pd.DataFrame())

        if not df_idade.empty and 'age' in df_idade.columns:
            st.subheader("Performance por Faixa Etaria")
            col_custo = 'spend' if 'spend' in df_idade.columns else 'custo'
            df_age = df_idade.groupby('age')[col_custo].sum().reset_index()
            fig = px.bar(df_age, x='age', y=col_custo, color_discrete_sequence=['#000000'])
            render_chart(fig, key="tt_age")

        if not df_genero.empty and 'gender' in df_genero.columns:
            st.subheader("Performance por Genero")
            col_custo = 'spend' if 'spend' in df_genero.columns else 'custo'
            df_gen = df_genero.groupby('gender')[col_custo].sum().reset_index()
            fig = px.pie(df_gen, values=col_custo, names='gender', hole=0.4,
                         color_discrete_sequence=['#000000', '#666', '#ccc'])
            render_chart(fig, key="tt_gen")


# =============================================================================
# PAGINA: GA4 / SEARCH CONSOLE
# =============================================================================
def pagina_ga4_search_console():
    st.title("GA4 / Search Console")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Fontes de Trafego", "Landing Pages", "Consultas Organicas", "Dispositivos", "Geografico"
    ])

    with tab1:
        df = carregar_csv("GA4/sessoes_por_fonte.csv")
        if df.empty:
            st.info("Sem dados de GA4.")
        else:
            st.subheader("Sessoes por Fonte/Medio")
            df_fonte = df.groupby(['sessionSource', 'sessionMedium']).agg({
                'sessions': 'sum', 'totalUsers': 'sum',
                'bounceRate': 'mean', 'totalRevenue': 'sum'
            }).reset_index().sort_values('sessions', ascending=False).head(20)

            fig = px.bar(df_fonte, x='sessions', y='sessionSource', color='sessionMedium',
                         orientation='h', color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            render_chart(fig, key="ga4_fontes")

            # Bounce Rate semaforo
            br_medio = df['bounceRate'].mean() * 100 if df['bounceRate'].max() <= 1 else df['bounceRate'].mean()
            cor, msg = semaforo_bounce_rate(br_medio)
            st.markdown(render_semaforo(cor, msg), unsafe_allow_html=True)
            render_explicacao(EXPLICACOES['ga4']['fontes'])

    with tab2:
        df = carregar_csv("GA4/landing_pages.csv")
        if df.empty:
            st.info("Sem dados de landing pages.")
        else:
            st.subheader("Landing Pages — Sessoes e Conversoes")
            df_lp = df.groupby('landingPage').agg({
                'sessions': 'sum', 'conversions': 'sum',
                'bounceRate': 'mean', 'totalRevenue': 'sum'
            }).reset_index().sort_values('sessions', ascending=False).head(20)
            df_lp['taxa_conversao'] = np.where(df_lp['sessions'] > 0,
                                                df_lp['conversions'] / df_lp['sessions'] * 100, 0)
            st.dataframe(df_lp.style.format({
                'bounceRate': '{:.1f}%', 'taxa_conversao': '{:.2f}%', 'totalRevenue': 'R$ {:.2f}'
            }), use_container_width=True)
            render_explicacao(EXPLICACOES['ga4']['landing_pages'])

    with tab3:
        df = carregar_csv("Search_Console/oportunidades_seo.csv")
        if df.empty:
            df = carregar_csv("Search_Console/consultas.csv")
        if df.empty:
            st.info("Sem dados do Search Console.")
        else:
            st.subheader("Consultas Organicas")
            df_q = df.groupby('query').agg({
                'cliques': 'sum', 'impressoes': 'sum', 'ctr': 'mean', 'posicao': 'mean'
            }).reset_index().sort_values('impressoes', ascending=False).head(30)

            st.dataframe(df_q.style.format({
                'ctr': '{:.2f}%', 'posicao': '{:.1f}'
            }), use_container_width=True)
            render_explicacao(EXPLICACOES['ga4']['consultas'])

            # Oportunidades SEO
            if 'oportunidade_seo' in df.columns:
                oportunidades = df[df['oportunidade_seo'] == True]
                if not oportunidades.empty:
                    st.subheader("Oportunidades SEO (Alto Impressoes + Baixo CTR)")
                    st.dataframe(oportunidades[['query', 'impressoes', 'cliques', 'ctr', 'posicao']]
                                 .sort_values('impressoes', ascending=False).head(20),
                                 use_container_width=True)
                    render_explicacao(EXPLICACOES['ga4']['oportunidades'])

    with tab4:
        df = carregar_csv("GA4/dispositivos.csv")
        if df.empty:
            st.info("Sem dados de dispositivos GA4.")
        else:
            st.subheader("Sessoes por Dispositivo")
            col_device = 'deviceCategory' if 'deviceCategory' in df.columns else 'dispositivo'
            col_sessions = 'sessions' if 'sessions' in df.columns else 'sessoes'

            if col_device in df.columns and col_sessions in df.columns:
                df_dev = df.groupby(col_device)[col_sessions].sum().reset_index()
                fig = px.pie(df_dev, values=col_sessions, names=col_device, hole=0.4,
                             color_discrete_sequence=['#E37400', '#F9AB00', '#34A853', '#4285F4'])
                fig.update_traces(textinfo='label+percent', textposition='outside')
                render_chart(fig, key="ga4_dispositivos")
            else:
                st.info("Colunas esperadas nao encontradas nos dados de dispositivos.")
                st.dataframe(df.head(20), use_container_width=True)
            render_explicacao(EXPLICACOES['ga4']['dispositivos'])

    with tab5:
        df = carregar_csv("GA4/geografico.csv")
        if df.empty:
            st.info("Sem dados geograficos GA4.")
        else:
            st.subheader("Top 20 Cidades por Sessoes")
            col_city = 'city' if 'city' in df.columns else 'cidade'
            col_sessions = 'sessions' if 'sessions' in df.columns else 'sessoes'

            if col_city in df.columns and col_sessions in df.columns:
                df_geo = df.groupby(col_city)[col_sessions].sum().reset_index()
                df_geo = df_geo.sort_values(col_sessions, ascending=False).head(20)
                fig = px.bar(df_geo, x=col_sessions, y=col_city, orientation='h',
                             color_discrete_sequence=['#E37400'])
                fig.update_layout(yaxis={'categoryorder': 'total ascending'},
                                  xaxis_title='Sessoes', yaxis_title='Cidade')
                render_chart(fig, key="ga4_geografico")
            else:
                st.info("Colunas esperadas nao encontradas nos dados geograficos.")
                st.dataframe(df.head(20), use_container_width=True)
            render_explicacao(EXPLICACOES['ga4']['geografico'])


# =============================================================================
# PAGINA: ORGANICO
# =============================================================================
def pagina_organico():
    st.title("Organico (Instagram + Facebook)")

    df_posts = carregar_csv("Organico/posts.csv")
    df_diario = carregar_csv("Organico/diario.csv")
    df_tipo = carregar_csv("Organico/por_tipo.csv")
    df_ig_conta = carregar_csv("Organico/instagram_conta.csv")

    if df_posts.empty:
        st.info("Sem dados organicos. Execute `scripts/extrair_organico.py`.")
        return

    # Filtro shopping
    org_shopping_sel = None
    if 'shopping' in df_posts.columns:
        shoppings = sorted(df_posts['shopping'].dropna().unique().tolist())
        if shoppings:
            opcoes = ["Todos"] + shoppings
            org_shopping_sel = st.sidebar.selectbox("Shopping", opcoes, index=0, key="org_shopping")

    tab1, tab2, tab3, tab4 = st.tabs(["Resumo", "Por Tipo de Post", "Evolucao Diaria", "Por Shopping"])

    with tab1:
        df = _aplicar_filtro_shopping(df_posts, org_shopping_sel)

        # KPIs
        total_posts = len(df)
        total_engajamento = df['engajamento'].sum() if 'engajamento' in df.columns else 0
        total_likes = df['likes'].sum() if 'likes' in df.columns else 0
        total_comentarios = df['comentarios'].sum() if 'comentarios' in df.columns else 0
        total_salvos = df['salvos'].sum() if 'salvos' in df.columns else 0
        total_compartilhamentos = df['compartilhamentos'].sum() if 'compartilhamentos' in df.columns else 0
        total_alcance = df['alcance'].sum() if 'alcance' in df.columns else 0
        taxa_eng = (total_engajamento / total_alcance * 100) if total_alcance > 0 else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            render_kpi("Total de Posts", total_posts, "inteiro")
        with c2:
            render_kpi("Engajamento Total", total_engajamento, "inteiro")
        with c3:
            render_kpi("Alcance Total", total_alcance, "inteiro")
        with c4:
            render_kpi("Taxa Engajamento", taxa_eng, "percentual")

        st.markdown("---")

        # Split por plataforma
        c1, c2 = st.columns(2)
        with c1:
            if 'plataforma' in df.columns:
                df_plat = df.groupby('plataforma').agg({
                    'likes': 'sum', 'comentarios': 'sum', 'salvos': 'sum',
                    'compartilhamentos': 'sum', 'engajamento': 'sum',
                }).reset_index()
                fig = px.bar(df_plat, x='plataforma', y='engajamento',
                             color='plataforma', color_discrete_map={'Instagram': '#E1306C', 'Facebook': '#1877F2'},
                             title="Engajamento por Plataforma")
                render_chart(fig, key="org_plat_eng")

        with c2:
            if 'plataforma' in df.columns:
                df_plat_count = df.groupby('plataforma').size().reset_index(name='qtd_posts')
                fig = px.pie(df_plat_count, values='qtd_posts', names='plataforma', hole=0.4,
                             color='plataforma', color_discrete_map={'Instagram': '#E1306C', 'Facebook': '#1877F2'},
                             title="Volume de Posts por Plataforma")
                render_chart(fig, key="org_plat_vol")

        # Detalhamento engajamento
        st.subheader("Detalhamento do Engajamento")
        eng_data = pd.DataFrame({
            'Metrica': ['Curtidas', 'Comentarios', 'Salvamentos', 'Compartilhamentos'],
            'Total': [total_likes, total_comentarios, total_salvos, total_compartilhamentos],
        })
        fig = px.bar(eng_data, x='Metrica', y='Total', color_discrete_sequence=['#34A853'],
                     title="Composicao do Engajamento")
        render_chart(fig, key="org_eng_comp")
        render_explicacao(EXPLICACOES['organico']['engajamento'])

    with tab2:
        df = _aplicar_filtro_shopping(df_posts, org_shopping_sel)

        if 'tipo_post' in df.columns:
            df_tp = df.groupby('tipo_post').agg({
                'likes': 'sum', 'comentarios': 'sum', 'salvos': 'sum',
                'compartilhamentos': 'sum', 'engajamento': 'sum', 'alcance': 'sum',
            }).reset_index()
            df_tp['qtd_posts'] = df.groupby('tipo_post').size().values
            df_tp['eng_por_post'] = np.where(df_tp['qtd_posts'] > 0,
                                              df_tp['engajamento'] / df_tp['qtd_posts'], 0)
            df_tp = df_tp.sort_values('engajamento', ascending=False)

            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(df_tp, x='tipo_post', y='engajamento',
                             color_discrete_sequence=['#34A853'],
                             title="Engajamento Total por Tipo")
                render_chart(fig, key="org_tipo_eng")
            with c2:
                fig = px.bar(df_tp, x='tipo_post', y='eng_por_post',
                             color_discrete_sequence=[ACCENT],
                             title="Engajamento Medio por Post")
                render_chart(fig, key="org_tipo_medio")

            st.subheader("Tabela Detalhada por Tipo")
            st.dataframe(df_tp.style.format({
                'eng_por_post': '{:.1f}',
            }), use_container_width=True)
        render_explicacao(EXPLICACOES['organico']['tipo_post'])

    with tab3:
        df = _aplicar_filtro_shopping(df_diario if not df_diario.empty else df_posts, org_shopping_sel)

        if not df.empty and 'data' in df.columns:
            df['data'] = pd.to_datetime(df['data'])

            col_eng = 'engajamento' if 'engajamento' in df.columns else 'likes'
            col_posts = 'qtd_posts' if 'qtd_posts' in df.columns else None

            if 'plataforma' in df.columns:
                df_daily = df.groupby(['data', 'plataforma']).agg({
                    col_eng: 'sum',
                }).reset_index()
                fig = px.line(df_daily, x='data', y=col_eng, color='plataforma',
                              color_discrete_map={'Instagram': '#E1306C', 'Facebook': '#1877F2'},
                              title="Engajamento Diario por Plataforma")
            else:
                df_daily = df.groupby('data').agg({col_eng: 'sum'}).reset_index()
                fig = px.line(df_daily, x='data', y=col_eng,
                              color_discrete_sequence=['#34A853'],
                              title="Engajamento Diario")

            render_chart(fig, key="org_evo_eng")

            # Volume de posts por dia
            if col_posts and col_posts in df.columns:
                df_vol = df.groupby('data').agg({col_posts: 'sum'}).reset_index()
                fig2 = px.bar(df_vol, x='data', y=col_posts,
                              color_discrete_sequence=[ACCENT],
                              title="Volume de Posts por Dia")
                render_chart(fig2, key="org_evo_vol")
        render_explicacao(EXPLICACOES['organico']['evolucao'])

    with tab4:
        df = df_posts.copy()  # Sem filtro de shopping aqui
        if 'shopping' in df.columns:
            df_shop = df.groupby('shopping').agg({
                'likes': 'sum', 'comentarios': 'sum', 'salvos': 'sum',
                'compartilhamentos': 'sum', 'engajamento': 'sum', 'alcance': 'sum',
            }).reset_index()
            df_shop['qtd_posts'] = df.groupby('shopping').size().values
            df_shop['eng_por_post'] = np.where(df_shop['qtd_posts'] > 0,
                                                df_shop['engajamento'] / df_shop['qtd_posts'], 0)
            df_shop = df_shop.sort_values('engajamento', ascending=False)

            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(df_shop, x='shopping', y='engajamento',
                             color_discrete_sequence=['#34A853'],
                             title="Engajamento Total por Shopping")
                render_chart(fig, key="org_shop_eng")
            with c2:
                fig = px.bar(df_shop, x='shopping', y='eng_por_post',
                             color_discrete_sequence=[ACCENT],
                             title="Engajamento Medio por Post")
                render_chart(fig, key="org_shop_medio")

            st.subheader("Tabela Comparativa")
            st.dataframe(df_shop, use_container_width=True)
        render_explicacao(EXPLICACOES['organico']['volume'])


# =============================================================================
# PAGINA: COMPARATIVO
# =============================================================================
def pagina_comparativo():
    st.title("Comparativo Cross-Platform")

    df_diario = carregar_csv("Consolidado/cross_platform_diario.csv")
    if df_diario.empty:
        st.info("Sem dados consolidados.")
        return

    df = filtro_periodo_sidebar(df_diario)

    # Resumo por plataforma
    df_plt = df.groupby('plataforma').agg({
        'impressoes': 'sum', 'cliques': 'sum', 'custo': 'sum',
        'conversoes': 'sum', 'receita': 'sum'
    }).reset_index()
    df_plt['ctr'] = np.where(df_plt['impressoes'] > 0, df_plt['cliques'] / df_plt['impressoes'] * 100, 0)
    df_plt['roas'] = np.where(df_plt['custo'] > 0, df_plt['receita'] / df_plt['custo'], 0)
    df_plt['cpa'] = np.where(df_plt['conversoes'] > 0, df_plt['custo'] / df_plt['conversoes'], 0)
    df_plt['cpm'] = np.where(df_plt['impressoes'] > 0, df_plt['custo'] / df_plt['impressoes'] * 1000, 0)

    # CPA e ROAS lado a lado
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("CPA por Plataforma")
        fig = px.bar(df_plt, x='plataforma', y='cpa', color='plataforma',
                     color_discrete_map=CORES_PLATAFORMA, text_auto='.2f')
        fig.update_layout(yaxis_title='CPA (R$)', showlegend=False)
        render_chart(fig, key="comp_cpa")

    with col2:
        st.subheader("ROAS por Plataforma")
        fig = px.bar(df_plt, x='plataforma', y='roas', color='plataforma',
                     color_discrete_map=CORES_PLATAFORMA, text_auto='.1f')
        fig.update_layout(yaxis_title='ROAS', showlegend=False)
        render_chart(fig, key="comp_roas")

    render_explicacao(EXPLICACOES['comparativo']['cpa_roas'])

    # Insight CPA
    if len(df_plt) >= 2:
        melhor = df_plt.loc[df_plt['cpa'].idxmin()]
        pior = df_plt.loc[df_plt[df_plt['cpa'] > 0]['cpa'].idxmax()] if (df_plt['cpa'] > 0).any() else melhor
        st.markdown(gerar_insight_box('cpa_comparativo', {
            'melhor_plataforma': melhor['plataforma'],
            'cpa_melhor': melhor['cpa'],
            'pior_plataforma': pior['plataforma'],
            'cpa_pior': pior['cpa'],
        }))

    # Radar chart 5 dimensoes
    st.subheader("Radar — 5 Dimensoes de Performance")
    if not df_plt.empty:
        # Normalizar metricas 0-100
        metricas = ['ctr', 'roas', 'conversoes', 'impressoes', 'custo']
        df_norm = df_plt[['plataforma'] + metricas].copy()
        for m in metricas:
            max_val = df_norm[m].max()
            df_norm[m] = df_norm[m] / max_val * 100 if max_val > 0 else 0

        fig = go.Figure()
        for _, row in df_norm.iterrows():
            fig.add_trace(go.Scatterpolar(
                r=[row[m] for m in metricas],
                theta=['CTR', 'ROAS', 'Conversoes', 'Impressoes', 'Investimento'],
                fill='toself',
                name=row['plataforma'],
                line=dict(color=CORES_PLATAFORMA.get(row['plataforma'], '#999')),
            ))
        fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100])))
        render_chart(fig, key="radar_comp")
        render_explicacao(EXPLICACOES['comparativo']['radar'])

    # Ranking automatico
    st.subheader("Ranking de Plataformas")
    df_rank = df_plt[['plataforma', 'roas', 'cpa', 'ctr', 'conversoes']].copy()
    df_rank['score'] = (
        df_rank['roas'].rank(ascending=True) +
        df_rank['cpa'].rank(ascending=False) +  # menor CPA = melhor
        df_rank['ctr'].rank(ascending=True) +
        df_rank['conversoes'].rank(ascending=True)
    )
    df_rank = df_rank.sort_values('score', ascending=False)
    for i, (_, row) in enumerate(df_rank.iterrows()):
        medal = ['🥇', '🥈', '🥉'][i] if i < 3 else f'{i+1}.'
        st.markdown(f"**{medal} {row['plataforma']}** — ROAS {row['roas']:.1f}x | CPA R$ {row['cpa']:.2f} | CTR {row['ctr']:.2f}%")


# =============================================================================
# PAGINA: FUNIL INTEGRADO
# =============================================================================
def pagina_funil_integrado():
    st.title("Funil Integrado")

    df_funil = carregar_csv("Consolidado/funil_por_plataforma.csv")
    if df_funil.empty:
        st.info("Sem dados de funil. Execute `scripts/consolidar_cross_platform.py`.")
        return

    etapas = ['impressoes', 'cliques', 'landing_page_views', 'sessoes_ga4', 'conversoes']
    etapas_label = ['Impressoes', 'Cliques', 'Landing Page Views', 'Sessoes GA4', 'Conversoes']

    # Funil por plataforma
    for _, row in df_funil.iterrows():
        st.subheader(row['plataforma'])
        valores = [row.get(e, 0) for e in etapas]
        valores = [v for v in valores if v > 0]
        labels = etapas_label[:len(valores)]

        if valores:
            fig = go.Figure(go.Funnel(
                y=labels, x=valores,
                textinfo="value+percent previous",
                marker=dict(color=CORES_PLATAFORMA.get(row['plataforma'], ACCENT)),
            ))
            fig.update_layout(margin=dict(t=20, b=20))
            render_chart(fig, key=f"funil_{row['plataforma']}")

            # Detectar maior perda
            perdas = []
            for i in range(1, len(valores)):
                if valores[i-1] > 0:
                    perda_pct = (1 - valores[i] / valores[i-1]) * 100
                    perdas.append((f"{labels[i-1]} → {labels[i]}", perda_pct))
            if perdas:
                maior_perda = max(perdas, key=lambda x: x[1])
                st.markdown(gerar_insight_box('funil', {
                    'etapa_maior_perda': maior_perda[0],
                    'pct_perda': maior_perda[1],
                }))

    # Funil consolidado
    st.markdown("---")
    st.subheader("Funil Consolidado (Todas as Plataformas)")
    totais = {e: df_funil[e].sum() for e in etapas if e in df_funil.columns}
    if totais:
        vals = [v for v in totais.values() if v > 0]
        labs = [etapas_label[i] for i, e in enumerate(etapas) if e in totais and totais[e] > 0]
        fig = go.Figure(go.Funnel(
            y=labs, x=vals,
            textinfo="value+percent previous",
            marker=dict(color=ACCENT),
        ))
        render_chart(fig, key="funil_total")
        render_explicacao(EXPLICACOES['funil']['integrado'])


# =============================================================================
# PAGINA: AUDIENCIA
# =============================================================================
def pagina_audiencia():
    st.title("Audiencia Cross-Platform")

    df_demo = carregar_csv("Consolidado/demografico_cross.csv")
    if df_demo.empty:
        st.info("Sem dados demograficos consolidados.")
        return

    # Faixa Etaria x Plataforma
    st.subheader("Investimento por Faixa Etaria x Plataforma")
    df_age = df_demo[df_demo['tipo'] == 'faixa_etaria']
    if not df_age.empty:
        df_age_plt = df_age.groupby(['segmento', 'plataforma'])['custo'].sum().reset_index()
        fig = px.bar(df_age_plt, x='segmento', y='custo', color='plataforma',
                     barmode='group', color_discrete_map=CORES_PLATAFORMA)
        fig.update_layout(xaxis_title='Faixa Etaria', yaxis_title='Investimento (R$)')
        render_chart(fig, key="aud_age")

    # Heatmap CPA por segmento
    st.subheader("Heatmap — CPA por Faixa Etaria x Plataforma")
    if not df_age.empty:
        df_age_cpa = df_age.groupby(['segmento', 'plataforma']).agg({
            'custo': 'sum', 'conversoes': 'sum'
        }).reset_index()
        df_age_cpa['cpa'] = np.where(df_age_cpa['conversoes'] > 0,
                                      df_age_cpa['custo'] / df_age_cpa['conversoes'], 0)
        pivot = df_age_cpa.pivot_table(index='segmento', columns='plataforma', values='cpa', fill_value=0)
        if not pivot.empty:
            fig = px.imshow(pivot, text_auto='.2f', aspect='auto',
                            color_continuous_scale='RdYlGn_r',
                            labels=dict(color='CPA (R$)'))
            render_chart(fig, key="aud_heatmap")
            render_explicacao(EXPLICACOES['audiencia']['demografico_cruzado'])

    # Genero
    st.subheader("Investimento por Genero x Plataforma")
    df_gen = df_demo[df_demo['tipo'] == 'genero']
    if not df_gen.empty:
        df_gen_plt = df_gen.groupby(['segmento', 'plataforma'])['custo'].sum().reset_index()
        fig = px.bar(df_gen_plt, x='segmento', y='custo', color='plataforma',
                     barmode='group', color_discrete_map=CORES_PLATAFORMA)
        fig.update_layout(xaxis_title='Genero', yaxis_title='Investimento (R$)')
        render_chart(fig, key="aud_gen")

    # Melhor segmento
    if not df_age.empty:
        df_best = df_age.groupby(['segmento', 'plataforma']).agg({
            'custo': 'sum', 'conversoes': 'sum'
        }).reset_index()
        df_best['roas_proxy'] = np.where(df_best['custo'] > 0, df_best['conversoes'] / df_best['custo'] * 100, 0)
        if not df_best.empty and df_best['roas_proxy'].max() > 0:
            best = df_best.loc[df_best['roas_proxy'].idxmax()]
            st.markdown(gerar_insight_box('melhor_segmento', {
                'segmento': best['segmento'],
                'plataforma': best['plataforma'],
                'roas': best['roas_proxy'],
            }))


# =============================================================================
# PAGINA: ONDE INVESTIR
# =============================================================================
def pagina_onde_investir():
    st.title("Onde Investir")

    df_rec = carregar_csv("Consolidado/recomendacoes_verba.csv")
    df_diario = carregar_csv("Consolidado/cross_platform_diario.csv")

    if df_rec.empty:
        st.info("Sem dados de recomendacoes. Execute `scripts/gerar_recomendacoes.py`.")
        return

    # Distribuicao atual vs recomendada
    st.subheader("Distribuicao Atual de Verba")
    col1, col2 = st.columns(2)

    with col1:
        fig = px.pie(df_rec, values='custo_total', names='plataforma',
                     color='plataforma', color_discrete_map=CORES_PLATAFORMA,
                     hole=0.5, title="Atual")
        fig.update_traces(textinfo='label+percent')
        render_chart(fig, key="oi_atual")

    with col2:
        # Recomendacao baseada no ROAS
        df_rec_calc = df_rec.copy()
        df_rec_calc['peso'] = df_rec_calc['roas_total'].clip(lower=0.1)
        df_rec_calc['pct_recomendado'] = df_rec_calc['peso'] / df_rec_calc['peso'].sum() * 100
        fig = px.pie(df_rec_calc, values='pct_recomendado', names='plataforma',
                     color='plataforma', color_discrete_map=CORES_PLATAFORMA,
                     hole=0.5, title="Recomendada (baseada em ROAS)")
        fig.update_traces(textinfo='label+percent')
        render_chart(fig, key="oi_recomendada")

    render_explicacao(EXPLICACOES['onde_investir']['distribuicao'])

    st.markdown("---")

    # Cards de recomendacao
    st.subheader("Recomendacoes por Plataforma")
    for _, row in df_rec.iterrows():
        acao = row.get('acao', 'MANTER')
        cores = {'AUMENTAR': COR_VERDE, 'MANTER': COR_AMARELO, 'DIMINUIR': COR_VERMELHO}
        cor = cores.get(acao, COR_AMARELO)
        st.markdown(render_card_recomendacao(
            row['plataforma'], acao, cor,
            row.get('motivo', ''), row.get('roas_total', 0)
        ), unsafe_allow_html=True)

    st.markdown("---")

    # Simulador de cenarios
    st.subheader("Simulador de Cenarios")
    st.markdown("Ajuste a distribuicao de verba e veja o impacto estimado:")

    orcamento_total = st.number_input("Orcamento mensal total (R$)",
                                       min_value=1000.0, value=50000.0, step=5000.0)

    plataformas = df_rec['plataforma'].tolist()
    sliders = {}
    cols = st.columns(len(plataformas))
    for i, plt in enumerate(plataformas):
        pct_atual = df_rec[df_rec['plataforma'] == plt]['custo_total'].values[0]
        pct_atual = pct_atual / df_rec['custo_total'].sum() * 100 if df_rec['custo_total'].sum() > 0 else 33
        with cols[i]:
            sliders[plt] = st.slider(plt, 0, 100, int(pct_atual), key=f"slider_{plt}")

    total_pct = sum(sliders.values())
    if total_pct > 0:
        st.markdown("**Resultado Estimado:**")
        cols = st.columns(len(plataformas))
        for i, plt in enumerate(plataformas):
            investimento = orcamento_total * sliders[plt] / total_pct
            roas = df_rec[df_rec['plataforma'] == plt]['roas_total'].values[0]
            receita_est = investimento * roas
            with cols[i]:
                st.metric(plt, f"R$ {investimento:,.0f}",
                          delta=f"Receita est: R$ {receita_est:,.0f}")
    else:
        st.warning("Ajuste os sliders para pelo menos uma plataforma.")

    render_explicacao(EXPLICACOES['onde_investir']['simulador'])


# =============================================================================
# PAGINA: ALERTAS E ANOMALIAS
# =============================================================================
def pagina_alertas():
    st.title("Alertas e Anomalias")

    df_alertas = carregar_csv("Consolidado/alertas.csv")

    if df_alertas.empty:
        st.success("Nenhum alerta detectado. Todas as metricas estao dentro dos padroes normais.")
        render_explicacao(EXPLICACOES['alertas']['anomalias'])
        return

    # Filtro severidade
    severidades = df_alertas['severidade'].unique().tolist()
    sev_sel = st.multiselect("Filtrar por severidade", severidades, default=severidades)
    df_filtrado = df_alertas[df_alertas['severidade'].isin(sev_sel)]

    # Contadores
    c1, c2, c3 = st.columns(3)
    with c1:
        n_alta = len(df_filtrado[df_filtrado['severidade'] == 'Alta'])
        st.markdown(f"<div class='kpi-card' style='border-left-color:{COR_VERMELHO}'>"
                    f"<div class='valor' style='color:{COR_VERMELHO}'>{n_alta}</div>"
                    f"<div class='label'>Alta Severidade</div></div>", unsafe_allow_html=True)
    with c2:
        n_media = len(df_filtrado[df_filtrado['severidade'] == 'Media'])
        st.markdown(f"<div class='kpi-card' style='border-left-color:{COR_AMARELO}'>"
                    f"<div class='valor' style='color:{COR_AMARELO}'>{n_media}</div>"
                    f"<div class='label'>Media Severidade</div></div>", unsafe_allow_html=True)
    with c3:
        n_baixa = len(df_filtrado[df_filtrado['severidade'] == 'Baixa'])
        st.markdown(f"<div class='kpi-card' style='border-left-color:#3498DB'>"
                    f"<div class='valor' style='color:#3498DB'>{n_baixa}</div>"
                    f"<div class='label'>Baixa Severidade</div></div>", unsafe_allow_html=True)

    render_explicacao(EXPLICACOES['alertas']['anomalias'])

    st.markdown("---")

    # Lista de alertas
    for _, alerta in df_filtrado.iterrows():
        st.markdown(render_alerta(
            alerta.get('plataforma', ''),
            alerta.get('mensagem', alerta.get('tipo', '')),
            alerta.get('valor_atual', 0),
            0,  # desvio_pct
            alerta.get('severidade', 'Media'),
        ), unsafe_allow_html=True)

    # Anomalias em tempo real dos dados diarios
    st.markdown("---")
    st.subheader("Deteccao em Tempo Real")
    df_diario = carregar_csv("Consolidado/cross_platform_diario.csv")
    if not df_diario.empty:
        for plataforma in df_diario['plataforma'].unique():
            df_plt = df_diario[df_diario['plataforma'] == plataforma]
            for metrica in ['custo', 'cpa']:
                if metrica in df_plt.columns:
                    anomalias = detectar_anomalias(df_plt, metrica, 'data')
                    if not anomalias.empty:
                        st.markdown(f"**{plataforma} — {metrica.upper()}**")
                        for _, a in anomalias.tail(5).iterrows():
                            st.markdown(render_alerta(
                                str(a['data'].date()) if hasattr(a['data'], 'date') else str(a['data']),
                                f"{plataforma} {metrica}",
                                a[metrica],
                                a['desvio_pct'],
                                a['severidade'],
                            ), unsafe_allow_html=True)
    render_explicacao(EXPLICACOES['alertas']['saturacao'])


# =============================================================================
# PAGINA: DOCUMENTACAO
# =============================================================================
def pagina_documentacao():
    st.title("Documentacao")

    tab1, tab2 = st.tabs(["Glossario", "Referencia de Dados"])

    with tab1:
        st.subheader("Glossario de Metricas")
        glossario = {
            "Impressoes": "Numero de vezes que o anuncio foi exibido.",
            "Cliques": "Numero de cliques no anuncio.",
            "CTR (Click-Through Rate)": "Taxa de cliques = Cliques / Impressoes x 100. Mede a atratividade do anuncio.",
            "CPC (Custo por Clique)": "Custo medio por cada clique recebido.",
            "CPM (Custo por Mil)": "Custo para cada 1.000 impressoes. Usado principalmente em campanhas de awareness.",
            "CPA (Custo por Aquisicao)": "Custo medio para cada conversao. CPA = Custo Total / Conversoes.",
            "ROAS (Return on Ad Spend)": "Retorno sobre investimento em midia. ROAS = Receita / Custo. "
                                          "Ex: ROAS 4x significa que cada R$1 investido gerou R$4 de receita.",
            "ROAS Blended": "ROAS calculado sobre TODAS as plataformas juntas. Receita Total / Custo Total.",
            "Conversoes": "Acoes valiosas completadas (compra, lead, cadastro) atribuidas ao anuncio.",
            "Alcance": "Numero de pessoas unicas que viram o anuncio (diferente de impressoes, que conta repeticoes).",
            "Frequencia": "Media de vezes que cada pessoa viu o anuncio. Freq = Impressoes / Alcance.",
            "Quality Score": "Nota de 1-10 do Google Ads que avalia qualidade da keyword, anuncio e landing page.",
            "Search Impression Share": "% de impressoes que voce recebeu vs total disponivel no Google Search.",
            "Bounce Rate": "% de sessoes com uma unica pagina visualizada (sem interacao).",
            "Landing Page View": "Visualizacao da pagina de destino apos clique no anuncio (confirma que carregou).",
            "Ad Recall Lift": "Estimativa de quantas pessoas lembrariam do anuncio se perguntadas em 2 dias.",
        }
        for termo, definicao in glossario.items():
            st.markdown(f"**{termo}**")
            st.markdown(f"  {definicao}")
            st.markdown("")

    with tab2:
        st.subheader("Fontes de Dados")
        st.markdown("""
        | Plataforma | API | Frequencia | Dados |
        |-----------|-----|------------|-------|
        | Google Ads | Google Ads API v23 | Diaria | Campanhas, keywords, demo, geo, dispositivos |
        | Meta Ads | Marketing API v22 | Diaria | Campanhas, plataforma, posicionamento, video, demo |
        | TikTok Ads | Business API v1.3 | Diaria | Campanhas, video engagement, demo |
        | GA4 | Data API v1beta | Diaria | Sessoes, conversoes, landing pages |
        | Search Console | Search Console API | Diaria (3d delay) | Consultas, paginas, posicoes |
        | Organico | Graph API v22 | Diaria | Posts IG+FB, engajamento, alcance |
        """)

        st.subheader("Estrutura de Arquivos")
        st.code("""
Dados/
├── Google_Ads/       (6 CSVs)
├── Meta_Ads/         (7 CSVs)
├── TikTok_Ads/       (5 CSVs)
├── GA4/              (4 CSVs)
├── Search_Console/   (3 CSVs)
├── Organico/         (4 CSVs)
└── Consolidado/      (7 CSVs)
        """)

        st.subheader("Metodologia — ROAS e Recomendacoes")
        st.markdown("""
        **ROAS Marginal:**
        - Compara ROAS dos ultimos 7 dias vs 30 dias vs total
        - Plataformas com ROAS >20% acima da media e tendencia estavel/positiva → AUMENTAR
        - Plataformas com ROAS <20% abaixo da media ou tendencia negativa forte → DIMINUIR
        - Demais → MANTER

        **Deteccao de Anomalias:**
        - Media movel de 7 dias para cada metrica (CPA, CTR, custo)
        - Alerta quando valor desvia >2 desvios-padrao da media movel
        - Severidade: Alta (>3 std), Media (>2 std)

        **Deteccao de Saturacao:**
        - Frequencia crescente + CTR em queda consistente = fadiga de anuncio
        - Recomendacao: renovar criativos ou redistribuir verba
        """)


# =============================================================================
# SIDEBAR + NAVEGACAO
# =============================================================================
def render_sidebar():
    """Renderiza sidebar com navegacao agrupada."""
    with st.sidebar:
        logo_path = BASE_DIR / "AJ.jpg"
        if logo_path.exists():
            st.image(str(logo_path), width=150)
        st.markdown("### Media Monitoring")
        st.markdown(f"*{st.session_state.get('nome', '')}*")
        st.markdown("---")

        paginas_permitidas = get_paginas_permitidas()

        # Navegacao agrupada
        pagina_selecionada = None
        for grupo, paginas in PAGINAS.items():
            paginas_grupo = [p for p in paginas if p in paginas_permitidas]
            if not paginas_grupo:
                continue

            with st.expander(grupo, expanded=True):
                for pagina in paginas_grupo:
                    if st.button(pagina, key=f"nav_{pagina}", use_container_width=True):
                        st.session_state['pagina'] = pagina

        st.markdown("---")

        # Data ultima atualizacao
        df_check = carregar_csv("Consolidado/cross_platform_diario.csv")
        if not df_check.empty and 'data' in df_check.columns:
            ultima = df_check['data'].max()
            if hasattr(ultima, 'strftime'):
                st.caption(f"Dados ate: {ultima.strftime('%d/%m/%Y')}")

        if st.button("Sair", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

    return st.session_state.get('pagina', 'Resumo Executivo')


# =============================================================================
# ROTEADOR DE PAGINAS
# =============================================================================
ROTEADOR = {
    "Resumo Executivo": pagina_resumo_executivo,
    "Tendencias": pagina_tendencias,
    "Google Ads": pagina_google_ads,
    "Meta Ads": pagina_meta_ads,
    "TikTok Ads": pagina_tiktok_ads,
    "GA4 / Search Console": pagina_ga4_search_console,
    "Organico": pagina_organico,
    "Comparativo": pagina_comparativo,
    "Funil Integrado": pagina_funil_integrado,
    "Audiencia": pagina_audiencia,
    "Onde Investir": pagina_onde_investir,
    "Alertas e Anomalias": pagina_alertas,
    "Documentacao": pagina_documentacao,
}


# =============================================================================
# MAIN
# =============================================================================
def main():
    injetar_css()

    # Verificar autenticacao
    if not st.session_state.get('autenticado', False):
        tela_login()
        return

    # Sidebar e navegacao
    pagina = render_sidebar()

    # Renderizar pagina
    if pagina in ROTEADOR:
        ROTEADOR[pagina]()
    else:
        st.error(f"Pagina '{pagina}' nao encontrada.")


if __name__ == "__main__":
    main()
