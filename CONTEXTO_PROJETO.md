# Dashboard Media Monitoring — Almeida Junior

## Visao Geral
Painel unificado de performance de midia digital que consolida dados de Google Ads, Meta Ads (Facebook/Instagram), TikTok Ads, GA4 e Google Search Console.

## Repositorio
- **GitHub:** https://github.com/carlosgravi/dashboard-media-monitoring
- **Deploy:** https://dashboard-media-monitoring.streamlit.app

## Estrutura
```
Dashboard_Media/
├── app.py                  # Dashboard principal (12 paginas, 5 grupos)
├── insights_midia.py       # Semaforos, insights, recomendacoes, anomalias
├── requirements.txt
├── AJ.jpg                  # Logo
├── .streamlit/config.toml  # Tema AJ
├── .github/workflows/      # Pipeline diario (7h BRT)
├── scripts/
│   ├── extrair_google_ads.py
│   ├── extrair_meta_ads.py
│   ├── extrair_tiktok_ads.py
│   ├── extrair_ga4.py
│   ├── extrair_search_console.py
│   ├── consolidar_cross_platform.py
│   ├── gerar_recomendacoes.py
│   └── notificar_whatsapp.py
└── Dados/
    ├── Google_Ads/     (6 CSVs)
    ├── Meta_Ads/       (7 CSVs)
    ├── TikTok_Ads/     (5 CSVs)
    ├── GA4/            (4 CSVs)
    ├── Search_Console/ (3 CSVs)
    └── Consolidado/    (7 CSVs)
```

## Paginas (12)
### Grupo 1: Visao Geral
1. Resumo Executivo - KPIs, semaforos, distribuicao verba, treemap campanhas
2. Tendencias - Evolucao ROAS/CPA mensal, area empilhada, dia da semana

### Grupo 2: Por Plataforma
3. Google Ads - 5 tabs (Campanhas, Keywords, Demo, Geo, Dispositivos)
4. Meta Ads - 5 tabs (Campanhas, Plataformas, Posicionamento, Video, Demo)
5. TikTok Ads - 3 tabs (Campanhas, Video Engagement, Demo)
6. GA4 / Search Console - 3 tabs (Fontes, Landing Pages, Consultas)

### Grupo 3: Cross-Platform
7. Comparativo - CPA/ROAS lado a lado, radar 5 dimensoes, ranking
8. Funil Integrado - Impressoes → Cliques → LPV → Sessoes → Conversoes
9. Audiencia - Demografico cruzado, heatmap CPA por segmento

### Grupo 4: Otimizacao
10. Onde Investir - Distribuicao atual vs recomendada, simulador cenarios
11. Alertas e Anomalias - Deteccao automatica (media movel 7d, 2 std)

### Grupo 5: Ferramentas
12. Documentacao - Glossario, metodologia, referencia de dados

## Autenticacao
- bcrypt (mesmo padrao dos outros dashboards)
- secrets.toml com usuarios e roles
- Suporte a `paginas` para restringir acesso

## Pipeline
- Diario as 7h BRT via GitHub Actions
- 5 extratores independentes (continue-on-error)
- Consolidacao cross-platform + recomendacoes
- Notificacao WhatsApp (self-hosted runner)

## Credenciais Necessarias (GitHub Secrets)
- Google Ads: developer_token, client_id/secret, refresh_token, login_customer_id, customer_id
- Meta Ads: access_token, ad_account_id, app_id/secret
- TikTok Ads: access_token, advertiser_id
- GA4: property_id, service_account_json (base64)
- Search Console: site_url (usa mesma SA do GA4)

## Sessoes de Trabalho
- Sessao 1 (03/03/2026): Implementacao completa do projeto - estrutura, 7 scripts de extracao, insights_midia.py, app.py com 12 paginas, GitHub Actions workflow, notificacao WhatsApp
