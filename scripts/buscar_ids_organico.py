"""
Script auxiliar para descobrir Facebook Page IDs e Instagram Business Account IDs.
Usa o mesmo token do Meta Ads (System User do Business Manager).

Uso:
  python scripts/buscar_ids_organico.py
"""
import os
import sys
import json
import requests

TOKEN = os.environ.get("META_ADS_ACCESS_TOKEN", "")
if not TOKEN:
    print("[ERRO] Configure META_ADS_ACCESS_TOKEN")
    sys.exit(1)

API_VERSION = "v22.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"


def listar_paginas():
    """Lista todas as Pages acessiveis pelo token."""
    print("\n=== FACEBOOK PAGES ===\n")

    # Tentar via /me/accounts (paginas do usuario/system user)
    url = f"{BASE}/me/accounts"
    params = {"access_token": TOKEN, "fields": "id,name,category,instagram_business_account", "limit": 100}
    resp = requests.get(url, params=params)

    if resp.status_code != 200:
        print(f"Erro /me/accounts: {resp.status_code}")
        print(resp.text[:500])

        # Fallback: tentar via Business Manager
        print("\nTentando via Business Manager...")
        listar_via_business_manager()
        return

    data = resp.json().get("data", [])
    if not data:
        print("Nenhuma pagina encontrada via /me/accounts")
        print("Tentando via Business Manager...")
        listar_via_business_manager()
        return

    for page in data:
        ig = page.get("instagram_business_account", {})
        ig_id = ig.get("id", "N/A")
        print(f"  Page ID: {page['id']}")
        print(f"  Nome: {page['name']}")
        print(f"  Categoria: {page.get('category', 'N/A')}")
        print(f"  Instagram Business ID: {ig_id}")
        print(f"  ---")

    print(f"\n{len(data)} pagina(s) encontrada(s)")


def listar_via_business_manager():
    """Lista pages via Business Manager owned_pages."""
    # Primeiro descobrir o Business ID
    url = f"{BASE}/me"
    params = {"access_token": TOKEN, "fields": "id,name"}
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        print(f"Erro /me: {resp.text[:300]}")
        return

    user_info = resp.json()
    print(f"Token pertence a: {user_info.get('name', 'N/A')} (ID: {user_info.get('id', 'N/A')})")

    # Listar businesses
    url = f"{BASE}/{user_info['id']}/businesses"
    params = {"access_token": TOKEN, "fields": "id,name", "limit": 50}
    resp = requests.get(url, params=params)

    if resp.status_code == 200:
        businesses = resp.json().get("data", [])
        for biz in businesses:
            print(f"\n  Business: {biz['name']} (ID: {biz['id']})")

            # Listar pages do business
            url_pages = f"{BASE}/{biz['id']}/owned_pages"
            params_pages = {
                "access_token": TOKEN,
                "fields": "id,name,instagram_business_account{id,name,username}",
                "limit": 100
            }
            resp_pages = requests.get(url_pages, params=params_pages)
            if resp_pages.status_code == 200:
                pages = resp_pages.json().get("data", [])
                for page in pages:
                    ig = page.get("instagram_business_account", {})
                    print(f"    Page: {page['name']} (ID: {page['id']})")
                    if ig:
                        print(f"    Instagram: @{ig.get('username', 'N/A')} (ID: {ig.get('id', 'N/A')})")
                    else:
                        print(f"    Instagram: nao vinculado")
            else:
                print(f"    Erro owned_pages: {resp_pages.status_code} - {resp_pages.text[:200]}")
    else:
        print(f"Erro businesses: {resp.text[:300]}")


def verificar_permissoes():
    """Verifica permissoes do token."""
    print("\n=== PERMISSOES DO TOKEN ===\n")
    url = f"{BASE}/debug_token"
    params = {"input_token": TOKEN, "access_token": TOKEN}
    resp = requests.get(url, params=params)
    if resp.status_code == 200:
        data = resp.json().get("data", {})
        scopes = data.get("scopes", [])
        print(f"  Tipo: {data.get('type', 'N/A')}")
        print(f"  App ID: {data.get('app_id', 'N/A')}")
        print(f"  Expira: {'Nunca' if data.get('expires_at', 0) == 0 else data.get('expires_at')}")
        print(f"  Permissoes ({len(scopes)}): {', '.join(scopes)}")

        # Verificar permissoes necessarias para organico
        needed = ['pages_show_list', 'pages_read_engagement', 'instagram_basic',
                  'instagram_manage_insights', 'read_insights']
        print(f"\n  Permissoes necessarias para organico:")
        for p in needed:
            status = "OK" if p in scopes else "FALTANDO"
            print(f"    {p}: {status}")
    else:
        print(f"Erro debug_token: {resp.text[:300]}")


if __name__ == "__main__":
    verificar_permissoes()
    listar_paginas()
