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
├── explicacoes_graficos.py # Explicacoes para todos os graficos
├── requirements.txt
├── AJ.jpg                  # Logo
├── .streamlit/config.toml  # Tema AJ
├── .github/workflows/      # Pipeline diario (7h BRT)
├── scripts/
│   ├── extrair_google_ads.py       # 9 CSVs (campanhas, diario, keywords, demografico, geografico, dispositivos, hora_dia, search_terms, alcance_frequencia, ad_groups, conversion_actions, audiences)
│   ├── extrair_meta_ads.py         # 9 CSVs (campanhas, plataforma, posicionamento, demografico_idade/genero, dispositivo, video, geografico, hora_dia)
│   ├── extrair_tiktok_ads.py       # 12 CSVs
│   ├── extrair_ga4.py              # 8 CSVs (sessoes_por_fonte, conversoes, landing_pages, diario, dispositivos, geografico, new_vs_returning, paginas)
│   ├── extrair_search_console.py   # 8 CSVs (consultas, paginas, dispositivos, consultas_device, oportunidades_seo, paises, search_appearance, query_page)
│   ├── consolidar_cross_platform.py # 11 CSVs consolidados
│   ├── gerar_recomendacoes.py
│   └── notificar_whatsapp.py
└── Dados/
    ├── Google_Ads/     (9+ CSVs)
    ├── Meta_Ads/       (9 CSVs)
    ├── TikTok_Ads/     (12 CSVs)
    ├── GA4/            (8 CSVs)
    ├── Search_Console/ (8 CSVs)
    └── Consolidado/    (11 CSVs)
```

## Paginas (12)
### Grupo 1: Visao Geral
1. Resumo Executivo - KPIs, semaforos, distribuicao verba, treemap campanhas
2. Tendencias - Evolucao ROAS/CPA mensal, area empilhada, dia da semana

### Grupo 2: Por Plataforma
3. Google Ads - 10 tabs (Campanhas, Keywords, Demo, Geo, Dispositivos, Search Terms, Hora/Dia, Alcance/Frequencia, **Ad Groups**, **Conversoes**)
4. Meta Ads - 8 tabs (Campanhas, Plataformas, Posicionamento, Video, Demo, **Quality Rankings**, **Geografico**, **Hora/Dia**)
5. TikTok Ads - 8 tabs (Campanhas, Video Engagement, Demo, Hora/Dia, Plataforma, Ad Groups, Alcance/Frequencia, Metadados)
6. GA4 / Search Console - 10 tabs (Fontes, Landing Pages, Consultas, Dispositivos, Geografico, **Engajamento**, **Novos vs Recorrentes**, **Paginas**, **Paises SC**, **Search Appearance**)

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
- Sessao 1 (03-04/03/2026): Implementacao completa do projeto - estrutura, 7 scripts de extracao, insights_midia.py, app.py com 12 paginas, GitHub Actions workflow, notificacao WhatsApp
- Sessao 1 (cont.): Criacao repo GitHub (carlosgravi/dashboard-media-monitoring), push inicial
- Sessao 1 (cont.): Adaptacao multi-shopping: TikTok (tokens separados por BC) e Google Ads (customer IDs por MCC)
- Sessao 1 (cont.): Configuracao de credenciais - GA4 + Search Console (3 secrets OK), TikTok (app pendente aprovacao), Meta (app criado, token pendente)

## Status Credenciais (GitHub Secrets)
| Plataforma | Status | Secrets |
|-----------|--------|---------|
| GA4 + Search Console | ✅ OK | GA4_PROPERTY_ID, GA4_SERVICE_ACCOUNT_JSON, SEARCH_CONSOLE_SITE_URL |
| Meta Ads | ✅ OK (4 secrets) | META_ADS_APP_ID, META_ADS_APP_SECRET, META_ADS_ACCESS_TOKEN, META_ADS_CONFIG |
| Google Ads | ✅ OK (5 secrets) | GOOGLE_ADS_DEVELOPER_TOKEN, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, LOGIN_CUSTOMER_ID, CUSTOMER_ID |
| TikTok Ads | ✅ OK (NS) | TIKTOK_ADS_CONFIG (JSON com token+advertiser_id por shopping). App aprovado para NS |

## Self-Hosted Runner
- **Runner:** `media-runner` em `C:\actions-runner-media\`
- **Servico Windows:** `actions.runner.media` (auto-start)
- **Funcao:** executa job `notificar-whatsapp` (acesso localhost:3001)

## Sessoes de Trabalho
- Sessao 1 (03-04/03/2026): Implementacao completa do projeto - estrutura, 7 scripts de extracao, insights_midia.py, app.py com 12 paginas, GitHub Actions workflow, notificacao WhatsApp
- Sessao 1 (cont.): Criacao repo GitHub (carlosgravi/dashboard-media-monitoring), push inicial
- Sessao 1 (cont.): Adaptacao multi-shopping: TikTok (tokens separados por BC) e Google Ads (customer IDs por MCC)
- Sessao 1 (cont.): Configuracao de credenciais - GA4 + Search Console (3 secrets OK), TikTok (app pendente aprovacao), Meta (app criado, token pendente)
- Sessao 2 (06/03/2026): Configuracao completa de credenciais Meta Ads (multi-conta 8 ad accounts) e Google Ads (MCC + OAuth2)
- Sessao 2 (cont.): TikTok rejeitado (site invalido), resubmetido com almeidajunior.com.br
- Sessao 2 (cont.): Instalacao gh CLI, criacao runner media-runner, servico Windows
- Sessao 2 (cont.): Fix workflow (nomes secrets Meta, setup-python no job whatsapp)
- Sessao 2 (cont.): Pipeline testado com sucesso - extracao OK + notificacao WhatsApp OK
- Sessao 3 (11/03/2026): Alcance e frequencia Google Ads (unique_users + freq para Display/Video/Demand Gen/PMAX)
- Sessao 3 (cont.): Nova tab "Alcance / Frequencia" no dashboard (KPIs, evolucao diaria, por campanha, CPM alcance)
- Sessao 3 (cont.): Fix timeout Meta Ads — breakdowns mensais, 30 dias, signal.alarm, flush logs (6min vs 20min+)
- Sessao 3 (cont.): Fix pipeline: timeout-minutes em todos os steps (10-30min)
- Sessao 3 (cont.): Fix bug pagina Organico (Streamlit Cloud cache — reboot resolveu)

- Sessao 4 (12/03/2026): Configuracao TikTok Ads NS — OAuth2 token gerado, TIKTOK_ADS_CONFIG secret configurado
- Sessao 4 (cont.): Fix extrator TikTok — json.dumps params, AUDIENCE report type, metrics_audience sem total_complete_payment_rate
- Sessao 4 (cont.): 7 novos relatorios TikTok: hora_dia, geografico, plataforma, adgroup_diario, alcance_frequencia, campanhas_metadata, adgroups_metadata
- Sessao 4 (cont.): Enriquecimento com nomes — campanhas.csv + video_engagement.csv + adgroup_diario.csv (merge com metadata)
- Sessao 4 (cont.): Dashboard TikTok de 3 para 8 tabs (+ Hora/Dia, Plataforma, Ad Groups, Alcance/Frequencia, Metadados)
- Sessao 4 (cont.): Explicacoes de graficos para 5 novas sections TikTok
- Sessao 4 (cont.): Consolidador cross-platform atualizado com dispositivos TikTok

- Sessao 5 (12/03/2026): Auditoria completa de todas as plataformas — implementacao de dados faltantes
- Sessao 5 (cont.): Google Ads: +3 extratores (ad_groups, conversion_actions, audiences), bidding_strategy_type, QS components (qualidade_criativo, qualidade_landing, ctr_esperado)
- Sessao 5 (cont.): Meta Ads: quality_ranking/engagement_rate_ranking/conversion_rate_ranking, outbound_clicks, +3 breakdowns (geografico/hora_dia), expanded actions (pixel purchase/lead/view_content)
- Sessao 5 (cont.): GA4: +6 engagement metrics (engagementRate, engagedSessions, userEngagementDuration, sessionsPerUser, eventCount, activeUsers), +2 reports (new_vs_returning, paginas)
- Sessao 5 (cont.): Search Console: +3 reports (paises, search_appearance, query_page)
- Sessao 5 (cont.): Consolidador: +3 consolidacoes cross-platform (hora_dia_cross, geografico_cross, video_cross) — total 11 CSVs
- Sessao 5 (cont.): Dashboard: Google Ads 8→10 tabs (+Ad Groups, +Conversoes), Meta Ads 5→8 tabs (+Quality Rankings, +Geografico, +Hora/Dia), GA4/SC 5→10 tabs (+Engajamento, +Novos vs Recorrentes, +Paginas, +Paises, +Search Appearance)
- Sessao 5 (cont.): Explicacoes de graficos: +11 novas explicacoes para todos os novos graficos/tabs
- Sessao 5 (cont.): Funil integrado corrigido: Meta fallback pixel purchase, Google source+medium match, leads/add_to_cart
- Sessao 5 (cont.): TikTok Ads reescrito com chunking automatico (30 dias stat_time_day, 1 dia stat_time_hour, 365 dias sem temporal)
- Sessao 5 (cont.): TikTok --dias 365 para historico completo, hora_dia limitado a min(dias, 90) para evitar excesso de requests
- Sessao 5 (cont.): enriquecer_csv_seguro() para nao crashear em CSVs vazios
- Sessao 5 (cont.): Fix GA4 diario — split em 2 requests (max 10 metricas por request da API)
- Sessao 5 (cont.): Fix Google Ads conversion_actions — removidos campos incompativeis (metrics.conversions, segments.conversion_action_category)
- Sessao 5 (cont.): Fix consolidador — pd.to_datetime com format='mixed' para aceitar datas TikTok ("2026-02-22 00:00:00") e Google/Meta ("2026-02-22")
- Sessao 5 (cont.): Pipeline validado end-to-end — todos 7 extratores + consolidador + recomendacoes + commit com sucesso

- Sessao 6 (13/03/2026): Fix hidden pipeline errors — Search Console searchAppearance dimension, Organico IG 30-day limit + retry 500s
- Sessao 6 (cont.): Revisao pagina por pagina — Resumo Executivo (treemap per-platform, impressoes KPI, legenda spacing, modebar expand)
- Sessao 6 (cont.): Diagnostico ROAS/CPA — campanhas O2O (awareness/trafego), sem purchase tracking em nenhuma plataforma
- Sessao 6 (cont.): Google Ads: 428k conversoes sao store visits (310k) + rotas (102k) + app opens (10k), nao compras
- Sessao 6 (cont.): Consolidador reescrito — Meta usa link_click como conversao, TikTok usa clicks como fallback
- Sessao 6 (cont.): ROAS removido do Resumo/Tendencias (substituido por CPM), CPA ajustado para "Custo por Acao"
- Sessao 6 (cont.): Explicacoes e insights atualizados para metricas O2O (CPM, CPA, CTR em vez de ROAS)

- Sessao 7 (13/03/2026): Fix erros persistentes de pipeline — Meta Ads + Organico
- Sessao 7 (cont.): Meta Ads — retry 3x com backoff exponencial (30s/60s/120s) para erros transientes (rate limit, unknown error, too many rows)
- Sessao 7 (cont.): Meta Ads — chunks de 30 dias para diario, 90 dias para mensal (antes era 90 fixo)
- Sessao 7 (cont.): Meta Ads — delay 2s entre contas para evitar rate limiting. Timeout workflow 45→60 min
- Sessao 7 (cont.): Organico — delay 0.3s entre chamadas de insights por post para evitar throttling Graph API
- Sessao 7 (cont.): Organico — default workflow reduzido de 365 para 90 dias (IG account insights limitado a 30d, posts antigos nao mudam)
- Sessao 7 (cont.): Meta Ads — default workflow reduzido de 365 para 180 dias (365d causava timeout >60min com 8 contas)

## Proximos Passos (ao retomar)
1. **TikTok Ads:** Aprovar app para demais shoppings (BS, CS, NK, NR, GS) e adicionar ao TIKTOK_ADS_CONFIG
2. **secrets.toml:** Criar usuarios para login no dashboard
3. **Deploy Streamlit Cloud**

## Notas Tecnicas
- TikTok: cada shopping tem BC separado → TIKTOK_ADS_CONFIG (JSON) com token+advertiser_id por shopping
- Google Ads: conta unica (242-948-0394) sob MCC AJ Digital (672-994-0117) → GOOGLE_ADS_CUSTOMER_ID
- Meta Ads: 8 contas (6 shoppings + AJ Realty + CS Residences) → META_ADS_CONFIG (JSON)
- GA4: 7 sites (1 principal + 1 por shopping) - por enquanto extrai so o principal
- Service Account: projeto "dashboard-almeida-junior" no Google Cloud
- Self-hosted runners: normas-runner (C:\actions-runner-normas) + media-runner (C:\actions-runner-media)

### Alcance e Frequencia (Google Ads)
- Metricas: `metrics.unique_users` + `metrics.average_impression_frequency_per_user`
- Filtro: `campaign.advertising_channel_type != 'SEARCH'` (captura DEMAND_GEN, VIDEO, MULTI_CHANNEL, DISPLAY)
- Requisito: 10k+ impressoes na campanha (senao retorna 0)
- Max 92 dias por query, delay ~3 dias
- Dados: 49 campanhas, 4.7M alcance, freq media 1.43 (dados atuais)

### TikTok Ads — Detalhes Tecnicos
- API: TikTok Marketing API v1.3 (REST, requests direto)
- Auth: OAuth2 (app_id + secret → auth_code → access_token)
- 12 CSVs: campanhas, video_engagement, demografico_idade/genero, diario, hora_dia, geografico, plataforma, adgroup_diario, alcance_frequencia, campanhas_metadata, adgroups_metadata
- Demograficos: report_type=AUDIENCE, data_level=AUCTION_ADVERTISER (age/gender nao suportam BASIC)
- Chunking automatico: _gerar_chunks() divide ranges longos (30d para stat_time_day, 1d para stat_time_hour, 365d sem temporal)
- Hora do dia: stat_time_hour exige range max 1 dia → loop dia a dia, limitado a min(dias, 90)
- Alcance/frequencia: metricas reach+frequency no report BASIC por campaign_id
- Metadados: endpoints separados /campaign/get/ e /adgroup/get/ (nomes, objetivos, status)
- Enriquecimento: merge campaign_name em campanhas.csv, video_engagement.csv e adgroup_diario.csv
- NS Advertiser ID: 7322585255381352449
- App ID: 7615457506518188048
- Permissoes: ad_account_info, report_read, campaign_read (scopes 2, 4)

### Modelo de Conversoes (O2O — Online to Offline)
- Campanhas AJ sao awareness/trafego para shopping fisico, NAO e-commerce
- ROAS nao se aplica (sem tracking de compra/receita em nenhuma plataforma)
- KPIs primarios: CPM (custo alcance), CPA (custo por acao), CTR
- **Google Ads**: conversoes = store visits (310k) + rotas (102k) + app opens (10k) + website visits (9k) + calls (2k)
- **Meta Ads**: conversoes = link_click (27k) — sem purchase/pixel purchase
- **TikTok Ads**: conversoes = clicks (52k) — sem conversion tracking
- Consolidador: Meta usa `link_click`, TikTok usa `clicks` como fallback para `conversion`

### Meta Ads — Otimizacao Pipeline
- Breakdowns (plataforma, posicionamento, idade, genero, dispositivo, video) usam `time_increment: 'monthly'`
- Apenas `campanhas` usa `time_increment: 1` (diario)
- Workflow roda com `--dias 180` (6 meses — 365d causa timeout com 8 contas + retry)
- Chunking: 30 dias (diario) / 90 dias (mensal) para evitar "numero excessivo de linhas"
- Retry: 3 tentativas com backoff exponencial (30s/60s/120s) para erros transientes
- Delay: 2s entre contas para evitar rate limiting da API
- Timeout: 3min por conta via `signal.alarm` (Linux) ou ThreadPool (Windows), 60min workflow total
- `demografico_cruzado` removido (redundante — consolidador usa idade e genero separados)
