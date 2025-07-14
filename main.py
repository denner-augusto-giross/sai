# main.py

import os
from dotenv import load_dotenv
from chatguru_api import ChatguruWABA
from db import read_data_from_db
from query import query_stuck_orders, query_available_providers
from geopy.distance import geodesic
import pandas as pd
from time import sleep


DIALOG_ID_PARA_OFERTA = "68681a2827f824ecd929292a" # ID do seu diálogo do Chatguru

def run_offer_workflow(chat_number, match_data):
    """
    Executa o fluxo completo: registra, atualiza campos e envia a oferta.
    """
    load_dotenv()
    chat_key = os.getenv("CHAT_GURU_KEY")
    chat_account_id = os.getenv("CHAT_GURU_ACCOUNT_ID")
    chat_phone_id = os.getenv("CHAT_GURU_PHONE_ID")
    chat_url = os.getenv("CHAT_GURU_URL")

    if not all([chat_key, chat_account_id, chat_phone_id, chat_url]):
        print("ERRO: Credenciais do Chatguru não encontradas no .env")
        return

    api = ChatguruWABA(chat_key, chat_account_id, chat_phone_id, chat_url)
    
    # Etapa 1: Registrar o Chat
    print(f"Etapa 1: Registrando chat com o número {chat_number}...")
    api.register_chat(chat_number, match_data.get("provider_name", "Novo Provedor"))

    # Etapa 2: Atualizar Campos Personalizados
    order_id = str(match_data.get('order_id'))
    provider_id = str(match_data.get('provider_id'))
    
    print(f"Etapa 2: Atualizando campos para o chat {chat_number} -> order_id: {order_id}, provider_id: {provider_id}")
    custom_fields = {"order_id": order_id, "provider_id": provider_id}
    update_response = api.update_custom_fields(chat_number, custom_fields)
    print("Resposta da Atualização dos Campos:", update_response)

    # --- MUDANÇA IMPORTANTE AQUI ---
    # Adiciona uma pausa de 2 segundos para dar tempo ao servidor do Chatguru.
    print("Aguardando 2 segundos para sincronização...")
    sleep(2)

    # Etapa 3: Executar o Diálogo
    if DIALOG_ID_PARA_OFERTA:
        print(f"Etapa 3: Executando diálogo '{DIALOG_ID_PARA_OFERTA}' para enviar a oferta...")
        dialog_response = api.execute_dialog(chat_number, DIALOG_ID_PARA_OFERTA)
        print("Resposta da Execução do Diálogo:", dialog_response)
    else:
        print("\nERRO: O DIALOG_ID_PARA_OFERTA não está definido.")


if __name__ == "__main__":
    # O código para encontrar a melhor correspondência permanece o mesmo
    # ...
    stuck_orders_df = read_data_from_db(query_stuck_orders())
    providers_df = read_data_from_db(query_available_providers())
    if stuck_orders_df is not None and not stuck_orders_df.empty and providers_df is not None and not providers_df.empty:
        stuck_orders_df.dropna(subset=['store_latitude', 'store_longitude'], inplace=True)
        providers_df.dropna(subset=['latitude', 'longitude'], inplace=True)
        stuck_orders_df['store_latitude'] = pd.to_numeric(stuck_orders_df['store_latitude'])
        stuck_orders_df['store_longitude'] = pd.to_numeric(stuck_orders_df['store_longitude'])
        providers_df['latitude'] = pd.to_numeric(providers_df['latitude'])
        providers_df['longitude'] = pd.to_numeric(providers_df['longitude'])
        stuck_orders_df['key'] = 1
        providers_df['key'] = 1
        all_combinations_df = pd.merge(stuck_orders_df, providers_df, on='key').drop('key', axis=1)
        def calculate_distance(row):
            order_coords = (row['store_latitude'], row['store_longitude'])
            provider_coords = (row['latitude'], row['longitude'])
            return geodesic(order_coords, provider_coords).kilometers
        all_combinations_df['distance_km'] = all_combinations_df.apply(calculate_distance, axis=1)
        nearby_providers_df = all_combinations_df[all_combinations_df['distance_km'] <= 10].copy()
        nearby_providers_df.sort_values(
            by=['order_id', 'distance_km', 'total_releases_last_2_weeks', 'score'],
            ascending=[True, True, True, False],
            inplace=True
        )
        best_matches_df = nearby_providers_df.groupby('order_id').first().reset_index()

        if not best_matches_df.empty:
            top_match = best_matches_df.iloc[0]
            test_phone_number = "5511943597109" # Mude para o seu número de teste
            run_offer_workflow(test_phone_number, top_match.to_dict())
        else:
            print("Nenhum provedor encontrado dentro de um raio de 10km.")
    else:
        print("\nNão foi possível realizar a correspondência.")