# analytics.py

import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta

def get_conversation_analytics(access_token, phone_number_id, start_date, end_date):
    """
    Busca os dados de análise de conversas da API da Meta para um período específico.
    """
    # Formata as datas para o formato de timestamp que a API espera
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    # Constrói a URL do endpoint de análise
    url = f"https://graph.facebook.com/v19.0/{phone_number_id}/conversation_analytics"
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    params = {
        "start": start_timestamp,
        "end": end_timestamp,
        "granularity": "DAILY" # Pode ser DAILY, MONTHLY, etc.
    }
    
    print(f"INFO: A buscar dados de análise de {start_date.date()} a {end_date.date()}...")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        
        data = response.json()
        print("SUCESSO: Dados de análise recebidos.")
        return data.get('conversation_analytics', {}).get('data', [])
        
    except requests.exceptions.HTTPError as http_err:
        print(f"ERRO: Falha ao buscar análise: {http_err}")
        print(f"Detalhes do erro: {http_err.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha de rede: {e}")
        return None


if __name__ == "__main__":
    load_dotenv()
    
    # Carrega as credenciais da Meta do seu ficheiro .env
    ACCESS_TOKEN_GRAPH = os.getenv("ACCESS_TOKEN_GRAPH")
    PHONE_NUMBER_ID = os.getenv("FROM_PHONE_NUMBER_ID")

    if not all([ACCESS_TOKEN_GRAPH, PHONE_NUMBER_ID]):
        print("ERRO: ACCESS_TOKEN ou FROM_PHONE_NUMBER_ID não encontrados no ficheiro .env.")
    else:
        # Define o período que queremos analisar (por exemplo, os últimos 7 dias)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        analytics_data = get_conversation_analytics(ACCESS_TOKEN_GRAPH, PHONE_NUMBER_ID, start_date, end_date)
        
        if analytics_data:
            print("\n" + "="*50)
            print("RELATÓRIO DE ANÁLISE DE CONVERSAS")
            print("="*50)
            
            total_conversations = 0
            # Imprime os dados de cada dia
            for day_data in analytics_data:
                data_point = day_data.get('data_points', [{}])[0]
                conversation_info = data_point.get('conversation', {})
                cost_info = data_point.get('cost', {})

                print(f"\nData: {conversation_info.get('start_date')}")
                print(f"  - ID da Conversa: {conversation_info.get('id')}")
                print(f"  - Tipo: {conversation_info.get('type')}")
                print(f"  - Custo Total: {cost_info.get('total_amount')} {cost_info.get('currency')}")
                total_conversations += 1
            
            print("\n" + "="*50)
            print(f"Total de conversas no período: {total_conversations}")
            print("="*50)

            # --- PRÓXIMO PASSO PARA BI ---
            # Aqui, em vez de imprimir, você iria iterar sobre os dados
            # e guardá-los numa tabela do seu banco de dados para
            # que a sua ferramenta de BI (Power BI, Metabase, etc.) possa lê-los.