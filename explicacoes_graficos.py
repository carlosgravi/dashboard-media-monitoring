"""
Explicacoes detalhadas de cada grafico do Dashboard Media Monitoring.
Cada funcao retorna um dict com textos para o usuario entender, interpretar e agir.

Estrutura de cada explicacao:
  - titulo: nome do grafico
  - o_que_mostra: descricao simples do que o grafico apresenta
  - como_interpretar: como ler os dados, o que observar
  - o_que_fazer: decisoes praticas baseadas no que voce ve
  - alerta: sinais de atencao (quando algo esta ruim)
"""

import streamlit as st


def render_explicacao(explicacao):
    """Renderiza uma explicacao com expander padronizado."""
    with st.expander("💡 Entenda este gráfico", expanded=False):
        st.markdown(f"**O que mostra:** {explicacao['o_que_mostra']}")
        st.markdown(f"**Como interpretar:** {explicacao['como_interpretar']}")
        st.markdown(f"**O que fazer:** {explicacao['o_que_fazer']}")
        if 'alerta' in explicacao:
            st.markdown(f"⚠️ **Fique atento:** {explicacao['alerta']}")


# =============================================================================
# RESUMO EXECUTIVO
# =============================================================================

RESUMO_KPIS = {
    'o_que_mostra': 'Os principais indicadores de performance de todas as plataformas de midia (Google Ads, Meta Ads, TikTok Ads) em um unico painel.',
    'como_interpretar': 'Compare os numeros com o periodo anterior. ROAS acima de 1.0 significa que voce ganha mais do que gasta. CPA baixo e melhor (custa menos para converter). CTR alto indica que os anuncios sao relevantes para o publico.',
    'o_que_fazer': 'Se o ROAS esta abaixo de 1.0, reavalie as campanhas — voce esta perdendo dinheiro. Se o CPA subiu muito, verifique se o publico-alvo esta correto ou se os criativos precisam ser renovados.',
    'alerta': 'ROAS abaixo de 1.0 por mais de 2 semanas seguidas exige acao imediata: pausar campanhas de baixo retorno ou redistribuir verba.',
}

RESUMO_DISTRIBUICAO_VERBA = {
    'o_que_mostra': 'Como o orcamento de midia esta dividido entre as plataformas (Google Ads, Meta Ads, TikTok Ads).',
    'como_interpretar': 'Compare a fatia de cada plataforma com o retorno (ROAS) que ela entrega. Uma plataforma com muita verba e pouco retorno esta desperdicando dinheiro.',
    'o_que_fazer': 'Redistribua verba das plataformas com menor ROAS para as com maior ROAS. Por exemplo, se Google Ads tem ROAS 5.0 e Meta tem 1.2, considere aumentar Google e reduzir Meta.',
    'alerta': 'Se mais de 50% da verba esta em uma unica plataforma, voce corre risco de dependencia. Diversifique para reduzir riscos.',
}

RESUMO_TREEMAP_CAMPANHAS = {
    'o_que_mostra': 'Todas as campanhas ativas organizadas por tamanho (quanto gastam) e cor (performance). Quanto maior o retangulo, mais verba a campanha consome.',
    'como_interpretar': 'Retangulos grandes com cor VERDE sao campanhas que gastam muito e performam bem (boa combinacao). Retangulos grandes com cor VERMELHA sao campanhas caras com baixo retorno — principal alvo de otimizacao.',
    'o_que_fazer': 'Foque nas campanhas grandes e vermelhas: ou otimize-as (melhore criativos, publico, lances) ou pause-as e mova a verba para campanhas verdes.',
}

# =============================================================================
# TENDENCIAS
# =============================================================================

TENDENCIAS_ROAS_CPA = {
    'o_que_mostra': 'A evolucao do ROAS (retorno sobre investimento) e CPA (custo por conversao) ao longo do tempo, para cada plataforma.',
    'como_interpretar': 'ROAS subindo = campanhas melhorando. ROAS caindo = campanhas perdendo eficiencia. CPA subindo = cada conversao esta ficando mais cara. Ideal: ROAS subindo E CPA caindo ao mesmo tempo.',
    'o_que_fazer': 'Se o ROAS esta em tendencia de queda por 3+ semanas: 1) Renove os criativos (fadiga de anuncio). 2) Revise o publico-alvo. 3) Ajuste os lances. Se o CPA subiu mas o ROAS se manteve, o valor das conversoes pode ter aumentado — verifique o ticket medio.',
    'alerta': 'Quedas bruscas de ROAS geralmente indicam: sazonalidade, fadiga de criativos, ou aumento de concorrencia nos leiloes.',
}

TENDENCIAS_AREA_EMPILHADA = {
    'o_que_mostra': 'O investimento total ao longo do tempo, dividido por plataforma. A area de cada cor mostra quanto foi gasto em cada plataforma a cada periodo.',
    'como_interpretar': 'Veja como a proporcao de investimento muda ao longo do tempo. Se uma plataforma esta crescendo em participacao, verifique se o retorno justifica o aumento.',
    'o_que_fazer': 'Use este grafico para validar se a estrategia de distribuicao de verba esta sendo seguida. Se Meta cresceu mais que o planejado, pode indicar otimizacao automatica ou necessidade de revisao.',
}

TENDENCIAS_DIA_SEMANA = {
    'o_que_mostra': 'Performance media por dia da semana (segunda a domingo). Mostra em quais dias os anuncios performam melhor ou pior.',
    'como_interpretar': 'Dias com ROAS mais alto sao os melhores para investir. Dias com CPA mais alto sao os mais caros para converter. O padrao varia por setor — varejo geralmente performa melhor quinta a sabado.',
    'o_que_fazer': 'Aumente os lances nos dias de melhor performance. Reduza nos dias de pior performance. Configure ajustes de lance por dia da semana no Google Ads e Meta Ads.',
}

# =============================================================================
# GOOGLE ADS
# =============================================================================

GOOGLE_CAMPANHAS = {
    'o_que_mostra': 'Performance detalhada de cada campanha do Google Ads: custo, cliques, conversoes, ROAS e CPA.',
    'como_interpretar': 'Ordene por ROAS para ver as melhores campanhas no topo. Campanhas com muitas impressoes mas poucos cliques (CTR baixo) tem anuncios pouco atrativos. Campanhas com muitos cliques mas poucas conversoes tem problema na landing page.',
    'o_que_fazer': 'Pausar campanhas com ROAS < 0.5 por mais de 30 dias. Aumentar verba de campanhas com ROAS > 3.0. Para CTR baixo: melhorar titulos e descricoes dos anuncios. Para conversao baixa: melhorar a pagina de destino.',
    'alerta': 'Campanhas com Impression Share abaixo de 50% estao perdendo impressoes para concorrentes — considere aumentar o lance ou o orcamento.',
}

GOOGLE_KEYWORDS = {
    'o_que_mostra': 'Performance de cada palavra-chave: quanto custou, quantos cliques gerou e quantas conversoes trouxe.',
    'como_interpretar': 'Quality Score (nota de qualidade) vai de 1 a 10 — acima de 7 e bom. Keywords com QS baixo custam mais por clique. Keywords com muitos cliques mas zero conversoes estao desperdicando verba.',
    'o_que_fazer': 'Negative keywords: adicione como negativa qualquer keyword com muitos cliques e zero conversoes (ex: "gratis", "emprego"). Aumente lance de keywords com alto QS e boa conversao. Pause keywords com custo alto e zero conversoes ha 30+ dias.',
    'alerta': 'Quality Score abaixo de 5 significa que o Google acha seu anuncio irrelevante para aquela busca — voce paga mais caro por cada clique.',
}

GOOGLE_DEMOGRAFICO = {
    'o_que_mostra': 'Como os anuncios performam por faixa etaria e genero do publico.',
    'como_interpretar': 'Identifique qual faixa etaria e genero converte mais e a que custo. CPA mais baixo = publico mais eficiente. Se 25-34 feminino tem CPA de R$5 e 55-64 masculino tem CPA de R$50, o primeiro publico e 10x mais eficiente.',
    'o_que_fazer': 'Aumente lances para faixas de melhor performance. Reduza ou exclua faixas com CPA muito acima da media. Use esses dados para criar audiences similares no Meta Ads.',
}

GOOGLE_GEOGRAFICO = {
    'o_que_mostra': 'De quais cidades e regioes vem os cliques e conversoes.',
    'como_interpretar': 'Identifique as cidades com maior volume de conversao e menor CPA. Cidades com muitos cliques e zero conversoes podem indicar trafego irrelevante.',
    'o_que_fazer': 'Aumente o lance nas cidades de melhor performance. Considere excluir cidades distantes dos shoppings que geram custo sem retorno. Use para planejar campanhas locais.',
}

GOOGLE_DISPOSITIVOS = {
    'o_que_mostra': 'Performance por tipo de dispositivo: celular, computador e tablet.',
    'como_interpretar': 'Compare CTR e CPA entre dispositivos. Normalmente celular tem mais impressoes, mas computador converte melhor. Se mobile tem CPA 3x maior que desktop, a experiencia mobile pode estar ruim.',
    'o_que_fazer': 'Se mobile tem CPA muito alto: verifique se o site e rapido no celular, se o checkout e facil. Ajuste lances por dispositivo: aumente para o que converte melhor, reduza para o que converte pior.',
    'alerta': 'Se mais de 70% do trafego vem de mobile mas as conversoes sao todas desktop, ha um problema serio na experiencia mobile.',
}

GOOGLE_SEARCH_TERMS = {
    'o_que_mostra': 'Os termos de busca reais que os usuarios digitaram no Google e que acionaram seus anuncios. Diferente de keywords (que voce configura), search terms sao o que as pessoas realmente buscaram.',
    'como_interpretar': 'Termos com muitas impressoes e poucos cliques (CTR baixo) indicam que seu anuncio nao e relevante para essa busca. Termos com muitos cliques e zero conversoes estao desperdicando verba. Termos com conversoes e baixo custo sao oportunidades de ouro.',
    'o_que_fazer': 'Adicione como keyword exata os termos que convertem bem. Adicione como negativa os termos irrelevantes com alto custo e zero conversao. Use os melhores termos para inspirar novos titulos de anuncio.',
    'alerta': 'Se mais de 30% do custo vem de termos irrelevantes, a estrategia de match type esta muito ampla — considere usar mais correspondencia de frase e exata.',
}

GOOGLE_HORA_DIA = {
    'o_que_mostra': 'Heatmap com a performance dos anuncios cruzando hora do dia e dia da semana. Mostra em quais momentos seus anuncios performam melhor ou pior.',
    'como_interpretar': 'Celulas mais escuras indicam maior volume ou melhor performance naquele horario/dia. Identifique blocos de horario com alta conversao e baixo CPA — esses sao seus "horarios de ouro". Horarios com custo alto e zero conversoes sao candidatos a reducao de lance.',
    'o_que_fazer': 'Configure ajustes de lance por horario (Ad Scheduling) no Google Ads: aumente lances nos horarios de ouro e reduza nos piores. Considere pausar completamente horarios com zero conversoes e custo significativo.',
    'alerta': 'Se as conversoes se concentram em poucas horas do dia, voce pode economizar ate 30% do orcamento ajustando os horarios de exibicao.',
}

# =============================================================================
# META ADS
# =============================================================================

META_CAMPANHAS = {
    'o_que_mostra': 'Performance de cada campanha do Meta Ads (Facebook + Instagram): alcance, frequencia, cliques, conversoes e custo.',
    'como_interpretar': 'Frequencia alta (acima de 3) indica que as mesmas pessoas estao vendo o anuncio muitas vezes — pode causar fadiga. Alcance vs Impressoes: se impressoes >> alcance, a frequencia esta alta.',
    'o_que_fazer': 'Campanhas com frequencia > 4: renovar criativos ou expandir publico. Campanhas com CTR caindo: trocar imagens/videos. Campanhas com bom alcance mas poucas conversoes: revisar a oferta ou landing page.',
    'alerta': 'Frequencia acima de 5-6 quase sempre causa queda de performance. Renove os criativos antes de chegar nesse ponto.',
}

META_PLATAFORMAS = {
    'o_que_mostra': 'Comparacao de performance entre Facebook e Instagram (e Messenger, Audience Network se ativos).',
    'como_interpretar': 'Veja qual plataforma entrega melhor CPA e ROAS. Instagram costuma ter CPM mais alto mas engagement melhor para visual. Facebook costuma ter alcance maior e CPC mais barato.',
    'o_que_fazer': 'Se Instagram tem ROAS 2x maior que Facebook, mova mais verba para Instagram. Se Facebook tem CPA muito alto, verifique se o posicionamento (Feed vs Stories vs Reels) esta otimizado.',
}

META_POSICIONAMENTO = {
    'o_que_mostra': 'Performance por posicionamento: Feed, Stories, Reels, Explore, etc.',
    'como_interpretar': 'Cada posicionamento tem comportamento diferente. Feed: mais texto, mais cliques. Stories/Reels: mais visual, mais engajamento. Explore: descoberta, publico novo.',
    'o_que_fazer': 'Invista mais nos posicionamentos com menor CPA. Se Reels tem CPA 50% menor que Feed, crie mais conteudo em video curto. Stories com CPA alto: teste diferentes formatos (carrossel vs video vs imagem).',
}

META_VIDEO = {
    'o_que_mostra': 'Metricas de engajamento com video: quantas pessoas assistiram 25%, 50%, 75% e 100% dos videos.',
    'como_interpretar': 'A taxa de retencao (% que assiste ate o fim) indica a qualidade do criativo. Se muitos veem 25% mas poucos chegam a 75%, o video perde a atencao no meio. Taxa de 100% acima de 15% e boa.',
    'o_que_fazer': 'Videos com baixa retencao: coloque a mensagem principal nos primeiros 3 segundos. Crie versoes mais curtas (6-15s). Videos com alta retencao: use para criar publicos de remarketing (quem assistiu 75%+).',
    'alerta': 'Se menos de 30% assiste ate 25%, o inicio do video nao esta capturando atencao — mude o gancho inicial.',
}

META_DEMOGRAFICO = {
    'o_que_mostra': 'Performance dos anuncios por faixa etaria e genero no Facebook e Instagram.',
    'como_interpretar': 'Igual ao Google Ads: identifique os segmentos que convertem mais barato. O Meta geralmente tem dados demograficos mais precisos que o Google.',
    'o_que_fazer': 'Use para criar Lookalike Audiences baseados nos segmentos de melhor performance. Exclua faixas etarias com CPA muito alto para reduzir desperdicio.',
}

# =============================================================================
# TIKTOK ADS
# =============================================================================

TIKTOK_CAMPANHAS = {
    'o_que_mostra': 'Performance das campanhas no TikTok: impressoes, cliques, custo e engajamento (likes, comments, shares).',
    'como_interpretar': 'No TikTok, engajamento (likes + comments + shares) e tao importante quanto cliques. Videos com alto engajamento organico custam menos no leilao. CTR acima de 1% e bom para TikTok.',
    'o_que_fazer': 'Campanhas com alto engajamento mas poucas conversoes: o conteudo e bom mas a oferta/CTA precisa melhorar. Campanhas com pouco engajamento: o criativo nao esta adequado ao formato TikTok (precisa ser mais nativo, menos "propaganda").',
}

TIKTOK_VIDEO = {
    'o_que_mostra': 'Retencao dos videos por quartil (25%, 50%, 75%, 100%) e metricas de engajamento detalhadas.',
    'como_interpretar': 'TikTok penaliza videos com baixa retencao nos primeiros 2-6 segundos. Se "watched_2s" e muito menor que impressoes, o gancho inicial esta fraco. "Engaged views" (15s+) indicam interesse real.',
    'o_que_fazer': 'Priorize os primeiros 2 segundos do video: comece com movimento, texto chamativo ou pergunta. Videos com alta retencao: invista mais verba neles. Teste diferentes hooks (aberturas) para o mesmo conteudo.',
}

# =============================================================================
# GA4 / SEARCH CONSOLE
# =============================================================================

GA4_DISPOSITIVOS = {
    'o_que_mostra': 'Distribuicao de sessoes do site por tipo de dispositivo: desktop (computador), mobile (celular) e tablet.',
    'como_interpretar': 'Veja de onde vem a maioria das visitas. Se mobile domina (>60%) mas as conversoes acontecem no desktop, o site pode ter problemas de usabilidade no celular. Compare a proporcao de sessoes com a proporcao de conversoes por dispositivo.',
    'o_que_fazer': 'Se mobile tem muitas sessoes mas pouca conversao: priorize a otimizacao do site para celular (velocidade, formularios simples, checkout facil). Se tablet e insignificante, nao gaste tempo otimizando para ele.',
    'alerta': 'Se mobile representa mais de 70% das sessoes mas menos de 30% das conversoes, ha uma perda significativa de receita por experiencia mobile ruim.',
}

GA4_GEOGRAFICO = {
    'o_que_mostra': 'De quais cidades vem as visitas ao site, com volume de sessoes e usuarios.',
    'como_interpretar': 'Identifique as cidades com maior volume de trafego. Compare com as cidades onde estao os shoppings Almeida Junior — o trafego deveria se concentrar nessas regioes. Cidades distantes com muito trafego podem indicar campanhas mal segmentadas.',
    'o_que_fazer': 'Garanta que as campanhas de midia priorizem as cidades proximas aos shoppings. Se houver muito trafego de cidades onde nao ha shopping, revise a segmentacao geografica dos anuncios.',
    'alerta': 'Trafego significativo de fora da area de atuacao pode indicar desperdicio de verba em campanhas sem segmentacao geografica.',
}

GA4_FONTES = {
    'o_que_mostra': 'De onde vem o trafego do site: busca organica, anuncios pagos, redes sociais, direto, email, etc.',
    'como_interpretar': 'Compare o volume (sessoes) com a qualidade (bounce rate, duracao, conversoes). Uma fonte com muitas sessoes mas bounce rate alto traz trafego irrelevante. Fontes com poucas sessoes mas alta conversao sao as mais eficientes.',
    'o_que_fazer': 'Invista nas fontes com menor custo por conversao. Se organico converte bem, invista mais em SEO. Se email converte melhor que social, priorize campanhas de email marketing.',
    'alerta': 'Bounce rate acima de 70% em trafego pago indica problema na landing page ou no publico-alvo dos anuncios.',
}

GA4_LANDING_PAGES = {
    'o_que_mostra': 'Quais paginas do site recebem mais trafego e como performam: taxa de rejeicao, tempo na pagina e conversoes.',
    'como_interpretar': 'Landing pages com alto trafego e alta conversao sao as melhores — proteja-as. Paginas com alto trafego e alta rejeicao precisam de otimizacao urgente.',
    'o_que_fazer': 'Para paginas com alta rejeicao: 1) Verifique velocidade de carregamento. 2) Confirme que o conteudo corresponde ao anuncio. 3) Melhore o CTA (chamada para acao). Para paginas de alta conversao: crie mais anuncios apontando para elas.',
}

SEARCH_CONSOLE_CONSULTAS = {
    'o_que_mostra': 'Quais termos as pessoas buscam no Google que mostram seu site nos resultados (organico).',
    'como_interpretar': 'Impressoes altas + CTR baixo = seu site aparece mas ninguem clica (titulo/descricao pouco atrativo). Posicao media abaixo de 10 = voce esta na primeira pagina. Acima de 10 = segunda pagina ou pior.',
    'o_que_fazer': 'Para consultas com muitas impressoes e CTR < 3%: melhore o titulo e meta description da pagina. Para consultas com posicao 4-10: otimize o conteudo para subir no ranking. Para consultas na posicao 11-20: estao quase na primeira pagina — pequenas melhorias podem gerar grande impacto.',
}

SEARCH_CONSOLE_OPORTUNIDADES = {
    'o_que_mostra': 'Keywords com alto potencial de trafego: muitas impressoes mas poucas pessoas clicam (oportunidades de SEO).',
    'como_interpretar': 'Estas sao as keywords onde voce ja aparece no Google mas nao esta sendo clicado. Melhorar titulo e descricao dessas paginas pode trazer muito trafego extra sem nenhum custo adicional.',
    'o_que_fazer': '1) Reescreva o titulo da pagina (tag title) para ser mais atrativo. 2) Melhore a meta description com um CTA claro. 3) Adicione dados estruturados (Schema) para rich snippets. 4) Considere criar conteudo especifico para essas buscas.',
}

# =============================================================================
# COMPARATIVO CROSS-PLATFORM
# =============================================================================

COMPARATIVO_CPA_ROAS = {
    'o_que_mostra': 'CPA e ROAS de cada plataforma lado a lado, permitindo comparar eficiencia.',
    'como_interpretar': 'A plataforma com menor CPA e a mais eficiente para gerar conversoes. A com maior ROAS e a que mais retorna por real investido. Nem sempre sao a mesma — CPA baixo com ticket baixo pode ter ROAS ruim.',
    'o_que_fazer': 'Migre verba da plataforma com pior ROAS para a com melhor ROAS, mantendo um minimo em cada para nao perder aprendizado. Antes de cortar: verifique se a plataforma esta no inicio do funil (awareness) ou no final (conversao).',
}

COMPARATIVO_RADAR = {
    'o_que_mostra': 'Radar com 5 dimensoes comparando as plataformas: CTR, CPA, ROAS, Alcance e Conversoes. Quanto maior a area, melhor a performance geral.',
    'como_interpretar': 'Plataformas com area grande performam bem em multiplas dimensoes. Se uma plataforma tem CTR alto mas ROAS baixo, atrai cliques mas nao converte — problema pode estar na oferta ou na landing page.',
    'o_que_fazer': 'Use o radar para identificar pontos fortes e fracos de cada plataforma. Nao baseie decisoes em uma unica metrica — olhe o conjunto.',
}

# =============================================================================
# FUNIL INTEGRADO
# =============================================================================

FUNIL_INTEGRADO = {
    'o_que_mostra': 'O funil completo: de quantas impressoes foram geradas ate quantas conversoes aconteceram, passando por cliques, visitas a landing page e sessoes no site.',
    'como_interpretar': 'Cada etapa do funil tem uma taxa de conversao. Se de 1000 cliques apenas 100 viram sessoes, 90% esta se perdendo no caminho — pode ser lentidao do site, pagina fora do ar ou redirecionamento errado. A maior queda indica o gargalo.',
    'o_que_fazer': 'Foque na etapa com maior perda: 1) Impressao→Clique baixo: melhore os criativos/anuncios. 2) Clique→Sessao baixo: melhore a velocidade do site. 3) Sessao→Conversao baixo: melhore a landing page e oferta. 4) Cada 1% de melhoria no gargalo tem o maior impacto no resultado final.',
    'alerta': 'Se a taxa Clique→Sessao e menor que 70%, ha um problema tecnico: site lento, pagina 404, redirecionamento errado, ou bot traffic inflando cliques.',
}

# =============================================================================
# AUDIENCIA
# =============================================================================

AUDIENCIA_DEMOGRAFICO_CRUZADO = {
    'o_que_mostra': 'Heatmap com CPA e volume cruzando faixa etaria e genero em todas as plataformas.',
    'como_interpretar': 'Celulas com cor intensa mostram os segmentos com mais volume OU pior CPA (dependendo da metrica selecionada). Compare: o segmento que mais gasta e tambem o que mais converte?',
    'o_que_fazer': 'Identifique o "sweet spot": segmentos com alto volume E baixo CPA. Esses sao seu publico ideal. Crie campanhas especificas para eles. Exclua segmentos com volume insignificante e CPA muito alto.',
}

# =============================================================================
# ONDE INVESTIR
# =============================================================================

ONDE_INVESTIR_DISTRIBUICAO = {
    'o_que_mostra': 'Comparacao entre a distribuicao ATUAL de verba e a distribuicao RECOMENDADA pelo algoritmo, baseada em ROAS e tendencia.',
    'como_interpretar': 'Se a barra "Recomendado" mostra mais verba em Google Ads do que o "Atual", significa que Google Ads esta com melhor retorno e merece mais investimento. A diferenca entre as barras mostra o quanto voce poderia otimizar.',
    'o_que_fazer': 'Ajuste gradualmente (10-20% por semana) a distribuicao na direcao do recomendado. Nao mude tudo de uma vez — plataformas precisam de tempo para otimizar com nova verba. Reavalie semanalmente.',
    'alerta': 'Nunca zere completamente uma plataforma — mantenha pelo menos 10% para nao perder historico e aprendizado do algoritmo.',
}

ONDE_INVESTIR_SIMULADOR = {
    'o_que_mostra': 'Simulacao de cenarios: o que aconteceria se voce redistribuisse a verba conforme diferentes estrategias.',
    'como_interpretar': 'Compare o cenario "Atual" com os cenarios alternativos. Veja a estimativa de conversoes e ROAS para cada cenario. As estimativas sao baseadas em dados historicos — nao sao garantias.',
    'o_que_fazer': 'Escolha o cenario que mais se aproxima dos seus objetivos (mais conversoes? maior ROAS? menor CPA?) e implemente gradualmente.',
}

# =============================================================================
# ALERTAS E ANOMALIAS
# =============================================================================

ALERTAS_ANOMALIAS = {
    'o_que_mostra': 'Deteccao automatica de anomalias: metricas que se desviaram significativamente da media dos ultimos 7 dias (mais de 2 desvios padrao).',
    'como_interpretar': 'Alertas VERMELHOS indicam piora significativa (CPA subiu muito, CTR caiu muito). Alertas AMARELOS indicam mudanca moderada. Alertas VERDES indicam melhora inesperada (tambem vale investigar — pode ser sazonal).',
    'o_que_fazer': 'Para cada alerta vermelho: 1) Verifique se e um problema real ou sazonal. 2) Investigue a causa (mudanca de criativo? concorrente? bug?). 3) Tome acao corretiva se confirmado. Nao ignore alertas por mais de 48h.',
    'alerta': 'Alertas recorrentes na mesma metrica indicam tendencia de piora — acao urgente necessaria.',
}

ALERTAS_SATURACAO = {
    'o_que_mostra': 'Indicadores de saturacao: quando uma campanha ou plataforma esta mostrando sinais de fadiga (CTR caindo progressivamente).',
    'como_interpretar': 'CTR em queda continua por 14+ dias e sinal classico de fadiga de anuncio — o publico ja viu demais. Frequencia crescente confirma a saturacao.',
    'o_que_fazer': 'Renove os criativos (novas imagens, novos textos, novos videos). Expanda o publico-alvo. Teste novas abordagens de comunicacao. No Meta Ads, considere usar Dynamic Creative.',
}


# --- ORGANICO ---
ORGANICO_VOLUME = {
    'o_que_mostra': 'Volume total de publicacoes organicas por plataforma (Instagram e Facebook), separado por tipo (Imagem, Video, Reel, Carrossel, Story).',
    'como_interpretar': 'Mais publicacoes nao significa necessariamente mais resultado. Compare o volume com o engajamento para entender quais tipos geram mais interacao por post.',
    'o_que_fazer': 'Identifique os tipos de post com melhor taxa de engajamento e priorize-os. Reels e carrosseis costumam ter maior alcance organico no Instagram.',
}

ORGANICO_ENGAJAMENTO = {
    'o_que_mostra': 'Total de engajamentos organicos (curtidas, comentarios, salvamentos, compartilhamentos) por shopping e plataforma.',
    'como_interpretar': 'Engajamento alto indica conteudo relevante para o publico. Salvamentos e compartilhamentos sao sinais mais fortes que curtidas. Compare a taxa de engajamento (engajamento / alcance) entre shoppings.',
    'o_que_fazer': 'Replique os formatos que geram mais salvamentos e compartilhamentos. Conteudo util (dicas, promocoes, eventos) tende a ser mais salvo. Conteudo emocional tende a ser mais compartilhado.',
}

ORGANICO_EVOLUCAO = {
    'o_que_mostra': 'Evolucao diaria do volume de posts e engajamento ao longo do periodo selecionado.',
    'como_interpretar': 'Observe a consistencia: dias sem post = oportunidade perdida. Picos de engajamento indicam conteudo viral ou promocional de sucesso.',
    'o_que_fazer': 'Mantenha uma cadencia regular de postagem. Analise os picos para entender o que funcionou e replicar.',
}

ORGANICO_TIPO_POST = {
    'o_que_mostra': 'Comparativo de performance por tipo de publicacao (Imagem, Video, Reel, Carrossel, etc.).',
    'como_interpretar': 'A taxa de engajamento por post (engajamento / qtd_posts) revela quais formatos performam melhor. Reels costumam ter maior alcance, carrosseis maior engajamento.',
    'o_que_fazer': 'Aloque mais esforco criativo nos formatos com melhor taxa de engajamento. Teste novos formatos em baixo volume antes de escalar.',
}


# Dict centralizado para acesso facil por pagina
EXPLICACOES = {
    'resumo': {
        'kpis': RESUMO_KPIS,
        'distribuicao_verba': RESUMO_DISTRIBUICAO_VERBA,
        'treemap': RESUMO_TREEMAP_CAMPANHAS,
    },
    'tendencias': {
        'roas_cpa': TENDENCIAS_ROAS_CPA,
        'area_empilhada': TENDENCIAS_AREA_EMPILHADA,
        'dia_semana': TENDENCIAS_DIA_SEMANA,
    },
    'google_ads': {
        'campanhas': GOOGLE_CAMPANHAS,
        'keywords': GOOGLE_KEYWORDS,
        'demografico': GOOGLE_DEMOGRAFICO,
        'geografico': GOOGLE_GEOGRAFICO,
        'dispositivos': GOOGLE_DISPOSITIVOS,
        'search_terms': GOOGLE_SEARCH_TERMS,
        'hora_dia': GOOGLE_HORA_DIA,
    },
    'meta_ads': {
        'campanhas': META_CAMPANHAS,
        'plataformas': META_PLATAFORMAS,
        'posicionamento': META_POSICIONAMENTO,
        'video': META_VIDEO,
        'demografico': META_DEMOGRAFICO,
    },
    'tiktok_ads': {
        'campanhas': TIKTOK_CAMPANHAS,
        'video': TIKTOK_VIDEO,
    },
    'ga4': {
        'fontes': GA4_FONTES,
        'landing_pages': GA4_LANDING_PAGES,
        'consultas': SEARCH_CONSOLE_CONSULTAS,
        'oportunidades': SEARCH_CONSOLE_OPORTUNIDADES,
        'dispositivos': GA4_DISPOSITIVOS,
        'geografico': GA4_GEOGRAFICO,
    },
    'comparativo': {
        'cpa_roas': COMPARATIVO_CPA_ROAS,
        'radar': COMPARATIVO_RADAR,
    },
    'funil': {
        'integrado': FUNIL_INTEGRADO,
    },
    'audiencia': {
        'demografico_cruzado': AUDIENCIA_DEMOGRAFICO_CRUZADO,
    },
    'onde_investir': {
        'distribuicao': ONDE_INVESTIR_DISTRIBUICAO,
        'simulador': ONDE_INVESTIR_SIMULADOR,
    },
    'alertas': {
        'anomalias': ALERTAS_ANOMALIAS,
        'saturacao': ALERTAS_SATURACAO,
    },
    'organico': {
        'volume': ORGANICO_VOLUME,
        'engajamento': ORGANICO_ENGAJAMENTO,
        'evolucao': ORGANICO_EVOLUCAO,
        'tipo_post': ORGANICO_TIPO_POST,
    },
}
