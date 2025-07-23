# internal_api.py

import requests
import json
import urllib3

# Desativa os avisos de segurança sobre a não verificação do SSL.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://api.giross.com.br/api/painel"

def login(email, password):
    """
    Realiza o login na API interna e retorna o token de acesso.
    """
    url = f"{BASE_URL}/login"
    
    payload = {
        "email": email,
        "password": password,
        "browser": "Chrome",
        "browserVersion": "138.0.0.0",
        "os": "Windows",
        "osVersion": "10",
        "device": None,
        "manufacturer": None,
        "screenSize": "1920x1080"
    }
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    print(f"INFO: A tentar fazer login no ambiente de PRODUÇÃO...")
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=15, verify=False)
        response.raise_for_status()
        
        data = response.json()
        access_token = data.get('access_token')
        
        if access_token:
            print("SUCESSO: Login realizado e token obtido.")
            return access_token
        else:
            # --- MUDANÇA DE DIAGNÓSTICO AQUI ---
            print("FALHA: Não foi possível obter o 'access_token' da resposta.")
            print("Resposta completa da API:", data) # Imprime a resposta completa
            return None
            
    except requests.exceptions.HTTPError as http_err:
        print(f"ERRO: Falha ao fazer login na API interna: {http_err}")
        print(f"Detalhes do erro: {http_err.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha de rede ao fazer login: {e}")
        return None

def assign_order(access_token, provider_id, order_id):
    """
    Atribui uma corrida a um entregador.
    """
    url = f"{BASE_URL}/sai/assign"
    payload = {"provider_id": provider_id, "request_id": order_id}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"INFO: A tentar atribuir a ordem {order_id} ao provedor {provider_id}...")
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=15, verify=False)
        response.raise_for_status()
        
        data = response.json()
        if data.get('success'):
            print(f"SUCESSO: Ordem {order_id} atribuída com sucesso!")
            return True
        else:
            print(f"FALHA: A API não retornou sucesso para a atribuição da ordem {order_id}.")
            return False

    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha ao atribuir a ordem: {e}")
        return False
