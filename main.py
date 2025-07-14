# main.py

import os
from time import sleep
from dotenv import load_dotenv
from chatguru_api import ChatguruWABA
from db import read_data_from_db
from query import query_stuck_orders, query_available_providers
from geopy.distance import geodesic
import pandas as pd

DIALOG_ID_PARA_OFERTA = "68681a2827f824ecd929292a"

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
    api.update_custom_fields(chat_number, custom_fields)

    print("Aguardando 2 segundos para sincronização...")
    sleep(2)

    # Etapa 3: Executar o Diálogo
    if DIALOG_ID_PARA_OFERTA:
        print(f"Etapa 3: Executando diálogo '{DIALOG_ID_PARA_OFERTA}' para enviar a oferta...")
        dialog_response = api.execute_dialog(chat_number, DIALOG_ID_PARA_OFERTA)
        print(f"Resposta da Execução do Diálogo para {chat_number}:", dialog_response)


if __name__ == "__main__":
    # A lógica para encontrar a melhor correspondência permanece a mesma
    stuck_orders_df = read_data_from_db(query_stuck_orders())
    providers_df = read_data_from_db(query_available_providers())

    if stuck_orders_df is not None and not stuck_orders_df.empty and providers_df is not None and not providers_df.empty:
        # ... (código para criar best_matches_df)
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

        # --- MUDANÇA IMPORTANTE PARA PRODUÇÃO ---
        if not best_matches_df.empty:
            print(f"\nEncontrados {len(best_matches_df)} melhores provedores. A iniciar o fluxo de ofertas...")
            # Itera sobre cada melhor correspondência encontrada
            for index, match in best_matches_df.iterrows():
                # Converte a linha do match para um dicionário
                match_data = match.to_dict()
                # Usa o número de celular real do provedor
                provider_phone_number = match_data.get('mobile')

                if provider_phone_number:
                    # Inicia o fluxo de trabalho para este provedor
                    run_offer_workflow(provider_phone_number, match_data)
                    print("-" * 50)
                else:
                    print(f"AVISO: Não foi possível encontrar o número de celular para o provedor ID {match_data.get('provider_id')}. A pular.")
        else:
            print("Nenhum provedor encontrado dentro de um raio de 10km.")
    else:
        print("\nNão foi possível realizar a correspondência.")