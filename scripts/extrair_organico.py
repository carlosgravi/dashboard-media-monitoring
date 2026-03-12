"""
Extrator Organico Instagram + Facebook — Multi-Shopping
Gera CSVs em Dados/Organico/ (todos com coluna 'shopping')

Metricas:
  - Instagram: posts (feed, reels, stories), engajamento, alcance, impressoes
  - Facebook: posts, engajamento, alcance, impressoes

Requer:
  - requests
  - META_ADS_ACCESS_TOKEN (System User com permissoes pages/instagram)

Uso:
  python scripts/extrair_organico.py [--dias 90]
"""
import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import time

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "Dados" / "Organico"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_VERSION = "v22.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

TOKEN = os.environ.get("META_ADS_ACCESS_TOKEN", "")

# Mapeamento: sigla -> {page_id, ig_id}
CONTAS = {
    "CS": {"page_id": "187091901422712", "ig_id": "17841400808534017", "nome": "Continente Shopping"},
    "BS": {"page_id": "124332530975670", "ig_id": "17841401798534589", "nome": "Balneario Shopping"},
    "NK": {"page_id": "182774648456656", "ig_id": "17841400974884461", "nome": "Neumarkt Shopping"},
    "NS": {"page_id": "330219467162655", "ig_id": "17841401493515662", "nome": "Nacoes Shopping"},
    "AJ": {"page_id": "272901593061536", "ig_id": "17841403498971145", "nome": "Almeida Junior"},
    "CS_Residences": {"page_id": "618481624689888", "ig_id": "17841472944766881", "nome": "CS Residences"},
}


def api_get(endpoint, params=None, max_retries=2):
    """GET request a Graph API com paginacao e retry para erros 500."""
    if params is None:
        params = {}
    params["access_token"] = TOKEN
    all_data = []
    url = f"{BASE_URL}/{endpoint}"

    while url:
        resp = None
        for attempt in range(max_retries + 1):
            resp = requests.get(url, params=params)
            if resp.status_code == 500 and attempt < max_retries:
                time.sleep(2 ** attempt)  # backoff: 1s, 2s
                continue
            break
        if resp.status_code != 200:
            print(f"  [Organico] Erro {resp.status_code}: {resp.text[:200]}")
            break
        data = resp.json()
        all_data.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        params = {}  # next URL ja inclui params

    return all_data


def extrair_instagram_posts(ig_id, sigla, data_inicio_ts, data_fim_ts):
    """Extrai posts do Instagram (feed + reels). Stories sao efemeros (24h)."""
    fields = "id,caption,media_type,media_product_type,timestamp,like_count,comments_count"
    posts = api_get(f"{ig_id}/media", {"fields": fields, "limit": 100})

    data = []
    for post in posts:
        ts = post.get("timestamp", "")
        if not ts:
            continue

        post_date = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        post_ts = post_date.timestamp()

        if post_ts < data_inicio_ts or post_ts > data_fim_ts:
            continue

        media_type = post.get("media_type", "")
        media_product = post.get("media_product_type", "")

        # Classificar tipo
        if media_product == "REELS":
            tipo = "Reel"
        elif media_type == "VIDEO":
            tipo = "Video"
        elif media_type == "CAROUSEL_ALBUM":
            tipo = "Carrossel"
        else:
            tipo = "Imagem"

        # Buscar insights do post
        likes = post.get("like_count", 0)
        comments = post.get("comments_count", 0)
        reach = 0
        impressions = 0
        saved = 0
        shares = 0

        try:
            # v22.0+: impressions depreciado para media, usar reach/saved/shares
            insights_data = api_get(f"{post['id']}/insights", {
                "metric": "reach,saved,shares"
            })
            for metric in insights_data:
                name = metric.get("name", "")
                values = metric.get("values", [{}])
                val = values[0].get("value", 0) if values else 0
                if name == "reach":
                    reach = val
                elif name == "saved":
                    saved = val
                elif name == "shares":
                    shares = val
        except Exception:
            pass  # Alguns posts antigos nao tem insights

        data.append({
            "shopping": sigla,
            "plataforma": "Instagram",
            "tipo_post": tipo,
            "data": post_date.strftime("%Y-%m-%d"),
            "caption": (post.get("caption", "") or "")[:200],
            "likes": likes,
            "comentarios": comments,
            "salvos": saved,
            "compartilhamentos": shares,
            "alcance": reach,
            "impressoes": impressions,
            "engajamento": likes + comments + saved + shares,
        })

    return data


def extrair_instagram_stories_insights(ig_id, sigla):
    """Extrai insights de stories ativos (ultimas 24h apenas)."""
    stories = api_get(f"{ig_id}/stories", {"fields": "id,timestamp,media_type"})

    data = []
    for story in stories:
        ts = story.get("timestamp", "")
        story_date = datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else datetime.now()

        reach = 0
        impressions = 0
        replies = 0

        try:
            insights_data = api_get(f"{story['id']}/insights", {
                "metric": "reach,replies"
            })
            for metric in insights_data:
                name = metric.get("name", "")
                values = metric.get("values", [{}])
                val = values[0].get("value", 0) if values else 0
                if name == "reach":
                    reach = val
                elif name == "replies":
                    replies = val
        except Exception:
            pass

        data.append({
            "shopping": sigla,
            "plataforma": "Instagram",
            "tipo_post": "Story",
            "data": story_date.strftime("%Y-%m-%d"),
            "caption": "",
            "likes": 0,
            "comentarios": replies,
            "salvos": 0,
            "compartilhamentos": 0,
            "alcance": reach,
            "impressoes": impressions,
            "engajamento": replies,
        })

    return data


def obter_page_token(page_id):
    """Obtem Page Access Token via System User token."""
    url = f"{BASE_URL}/{page_id}"
    params = {"access_token": TOKEN, "fields": "access_token"}
    resp = requests.get(url, params=params)
    if resp.status_code == 200:
        return resp.json().get("access_token", TOKEN)
    return TOKEN


def extrair_facebook_posts(page_id, sigla, data_inicio, data_fim):
    """Extrai posts da Facebook Page."""
    page_token = obter_page_token(page_id)

    fields = "id,message,created_time,shares,reactions.summary(true),comments.summary(true),attachments{type}"
    url = f"{BASE_URL}/{page_id}/published_posts"
    params = {
        "access_token": page_token,
        "fields": fields,
        "since": data_inicio,
        "until": data_fim,
        "limit": 100,
    }
    all_data = []
    while url:
        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            print(f"  [Organico] Erro FB posts {sigla}: {resp.status_code} - {resp.text[:200]}")
            break
        data = resp.json()
        all_data.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        params = {}

    posts = all_data

    data = []
    for post in posts:
        ts = post.get("created_time", "")
        if not ts:
            continue

        post_date = datetime.fromisoformat(ts.replace("+0000", "+00:00"))
        # Tipo via attachments (v3.3+)
        attachments = post.get("attachments", {}).get("data", [])
        post_type = attachments[0].get("type", "status") if attachments else "status"

        tipo_map = {
            "photo": "Imagem",
            "video_inline": "Video",
            "video": "Video",
            "share": "Link",
            "status": "Texto",
            "event": "Evento",
            "offer": "Oferta",
            "album": "Album",
            "multi_share": "Carrossel",
            "native_templates": "Template",
        }
        tipo = tipo_map.get(post_type, post_type)

        reactions = post.get("reactions", {}).get("summary", {}).get("total_count", 0)
        comments = post.get("comments", {}).get("summary", {}).get("total_count", 0)
        shares = post.get("shares", {}).get("count", 0)

        # Buscar insights do post (usando page_token)
        reach = 0
        impressions = 0
        try:
            url_ins = f"{BASE_URL}/{post['id']}/insights"
            resp_ins = requests.get(url_ins, params={
                "access_token": page_token,
                "metric": "post_impressions,post_impressions_unique"
            })
            if resp_ins.status_code == 200:
                for metric in resp_ins.json().get("data", []):
                    name = metric.get("name", "")
                    values = metric.get("values", [{}])
                    val = values[0].get("value", 0) if values else 0
                    if name == "post_impressions":
                        impressions = val
                    elif name == "post_impressions_unique":
                        reach = val
        except Exception:
            pass

        data.append({
            "shopping": sigla,
            "plataforma": "Facebook",
            "tipo_post": tipo,
            "data": post_date.strftime("%Y-%m-%d"),
            "caption": (post.get("message", "") or "")[:200],
            "likes": reactions,
            "comentarios": comments,
            "salvos": 0,
            "compartilhamentos": shares,
            "alcance": reach,
            "impressoes": impressions,
            "engajamento": reactions + comments + shares,
        })

    return data


def extrair_instagram_account_insights(ig_id, sigla, data_inicio, data_fim):
    """Extrai metricas de conta Instagram (seguidores, alcance diario).
    Instagram API limita insights de conta a ~30 dias de lookback.
    """
    data = []

    try:
        datas_dict = {}
        fim_dt = datetime.strptime(data_fim, "%Y-%m-%d")
        # Instagram API so retorna ~30 dias de insights de conta
        max_lookback = 30
        inicio_dt_original = datetime.strptime(data_inicio, "%Y-%m-%d")
        inicio_dt = max(inicio_dt_original, fim_dt - timedelta(days=max_lookback))

        # Loop em janelas de 28 dias (respeitando limite da API)
        janela_inicio = inicio_dt
        while janela_inicio < fim_dt:
            janela_fim = min(janela_inicio + timedelta(days=28), fim_dt)
            url = f"{BASE_URL}/{ig_id}/insights"
            params = {
                "access_token": TOKEN,
                "metric": "reach,follower_count",
                "period": "day",
                "metric_type": "time_series",
                "since": janela_inicio.strftime("%Y-%m-%d"),
                "until": janela_fim.strftime("%Y-%m-%d"),
            }
            resp = requests.get(url, params=params)
            if resp.status_code == 200:
                insights = resp.json().get("data", [])
                for metric in insights:
                    name = metric.get("name", "")
                    for val in metric.get("values", []):
                        end_time = val.get("end_time", "")[:10]
                        if end_time not in datas_dict:
                            datas_dict[end_time] = {"data": end_time, "shopping": sigla}
                        datas_dict[end_time][name] = val.get("value", 0)
            else:
                print(f"  [Organico] Erro insights conta IG {sigla} ({janela_inicio.strftime('%Y-%m-%d')}): {resp.status_code}")

            janela_inicio = janela_fim

        data = list(datas_dict.values())
    except Exception as e:
        print(f"  [Organico] Erro insights conta IG {sigla}: {e}")

    return data


def main():
    if not TOKEN:
        print("[ERRO] META_ADS_ACCESS_TOKEN nao configurado")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Extrator Organico Instagram + Facebook")
    parser.add_argument("--dias", type=int, default=90, help="Dias para extrair (default 90)")
    args = parser.parse_args()

    data_fim = datetime.now()
    data_inicio = data_fim - timedelta(days=args.dias)
    data_inicio_str = data_inicio.strftime("%Y-%m-%d")
    data_fim_str = data_fim.strftime("%Y-%m-%d")
    data_inicio_ts = data_inicio.timestamp()
    data_fim_ts = data_fim.timestamp()

    print(f"[Organico] Extraindo de {data_inicio_str} a {data_fim_str} ({len(CONTAS)} contas)...")

    all_posts = []
    all_ig_insights = []

    for sigla, config in CONTAS.items():
        page_id = config["page_id"]
        ig_id = config["ig_id"]
        nome = config["nome"]

        # Instagram posts
        print(f"  [Organico] {sigla} → Instagram posts...")
        try:
            ig_posts = extrair_instagram_posts(ig_id, sigla, data_inicio_ts, data_fim_ts)
            all_posts.extend(ig_posts)
            print(f"    {len(ig_posts)} posts")
        except Exception as e:
            print(f"    Erro: {e}")

        # Instagram stories (apenas ativos - ultimas 24h)
        print(f"  [Organico] {sigla} → Instagram stories...")
        try:
            stories = extrair_instagram_stories_insights(ig_id, sigla)
            all_posts.extend(stories)
            print(f"    {len(stories)} stories ativos")
        except Exception as e:
            print(f"    Erro: {e}")

        # Facebook posts
        print(f"  [Organico] {sigla} → Facebook posts...")
        try:
            fb_posts = extrair_facebook_posts(page_id, sigla, data_inicio_str, data_fim_str)
            all_posts.extend(fb_posts)
            print(f"    {len(fb_posts)} posts")
        except Exception as e:
            print(f"    Erro: {e}")

        # Instagram account insights (diario)
        print(f"  [Organico] {sigla} → Instagram insights conta...")
        try:
            ig_insights = extrair_instagram_account_insights(ig_id, sigla, data_inicio_str, data_fim_str)
            all_ig_insights.extend(ig_insights)
            print(f"    {len(ig_insights)} dias")
        except Exception as e:
            print(f"    Erro: {e}")

    # Salvar CSVs
    if all_posts:
        df_posts = pd.DataFrame(all_posts)
        df_posts.to_csv(OUTPUT_DIR / "posts.csv", index=False, encoding="utf-8-sig")
        print(f"\n[Organico] posts.csv: {len(df_posts)} registros")

        # Resumo diario
        df_posts['data'] = pd.to_datetime(df_posts['data'])
        df_diario = df_posts.groupby(['shopping', 'plataforma', 'data']).agg({
            'likes': 'sum',
            'comentarios': 'sum',
            'salvos': 'sum',
            'compartilhamentos': 'sum',
            'alcance': 'sum',
            'impressoes': 'sum',
            'engajamento': 'sum',
            'caption': 'count',
        }).rename(columns={'caption': 'qtd_posts'}).reset_index()
        df_diario.to_csv(OUTPUT_DIR / "diario.csv", index=False, encoding="utf-8-sig")
        print(f"[Organico] diario.csv: {len(df_diario)} registros")

        # Resumo por tipo de post
        df_tipo = df_posts.groupby(['shopping', 'plataforma', 'tipo_post']).agg({
            'likes': 'sum',
            'comentarios': 'sum',
            'salvos': 'sum',
            'compartilhamentos': 'sum',
            'alcance': 'sum',
            'impressoes': 'sum',
            'engajamento': 'sum',
            'caption': 'count',
        }).rename(columns={'caption': 'qtd_posts'}).reset_index()
        df_tipo.to_csv(OUTPUT_DIR / "por_tipo.csv", index=False, encoding="utf-8-sig")
        print(f"[Organico] por_tipo.csv: {len(df_tipo)} registros")
    else:
        print("\n[Organico] Nenhum post encontrado")

    if all_ig_insights:
        df_ig = pd.DataFrame(all_ig_insights)
        df_ig.to_csv(OUTPUT_DIR / "instagram_conta.csv", index=False, encoding="utf-8-sig")
        print(f"[Organico] instagram_conta.csv: {len(df_ig)} registros")

    print("\n[Organico] Extracao concluida!")


if __name__ == "__main__":
    main()
