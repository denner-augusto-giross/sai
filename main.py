# main.py

import os
from time import sleep
from dotenv import load_dotenv
from chatguru_api import ChatguruWABA
from db import read_data_from_db
from query import query_stuck_orders, query_available_providers
from geopy.distance import geodesic
import pandas as pd

# As constantes permanecem no topo
CITY_ID_PRODUCAO = 50
DIALOG_ID_PARA_OFERTA = "68681a2827f824ecd929292a" 

def run_offer_workflow(chat_number, match_data):
    """Executa o fluxo completo para enviar uma oferta de corrida."""
    # Esta função permanece exatamente a mesma
    load_dotenv()
    chat_key = os.getenv("CHAT_GURU_KEY")
    chat_account_id = os.getenv("CHAT_GURU_ACCOUNT_ID")
    chat_phone_id = os.getenv("CHAT_GURU_PHONE_ID")
    chat_url = os.getenv("CHAT_GURU_URL")

    if not all([chat_key, chat_account_id, chat_phone_id, chat_url]):
        print("ERRO: Credenciais do Chatguru não encontradas no .env")
        return

    api = ChatguruWABA(chat_key, chat_account_id, chat_phone_id, chat_url)
    
    print(f"Etapa 1: Registrando chat com o número {chat_number}...")
    api.register_chat(chat_number, match_data.get("provider_name", "Novo Provedor"))

    order_id = str(match_data.get('order_id'))
    provider_id = str(match_data.get('provider_id'))
    
    print(f"Etapa 2: Atualizando campos para o chat {chat_number} -> order_id: {order_id}, provider_id: {provider_id}")
    custom_fields = {"order_id": order_id, "provider_id": provider_id}
    api.update_custom_fields(chat_number, custom_fields)

    print("Aguardando 2 segundos para sincronização...")
    sleep(2)

    if DIALOG_ID_PARA_OFERTA:
        print(f"Etapa 3: Executando diálogo '{DIALOG_ID_PARA_OFERTA}' para enviar a oferta...")
        dialog_response = api.execute_dialog(chat_number, DIALOG_ID_PARA_OFERTA)
        print(f"Resposta da Execução do Diálogo para {chat_number}:", dialog_response)


# --- NOVA FUNÇÃO AQUI ---
def execute_sai_logic():
    """
    Encapsula toda a lógica de busca e oferta que estava no `if __name__ == "__main__"`.
    """
    print(f"\n--- A INICIAR LÓGICA DO SAI PARA A CIDADE ID: {CITY_ID_PRODUCAO} ---")
    
    stuck_orders_df = read_data_from_db(query_stuck_orders(CITY_ID_PRODUCAO))
    providers_df = read_data_from_db(query_available_providers(CITY_ID_PRODUCAO))

    if stuck_orders_df is not None and not stuck_orders_df.empty and providers_df is not None and not providers_df.empty:
        # ... (toda a lógica para criar best_matches_df permanece a mesma)
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
            print(f"\nEncontrados {len(best_matches_df)} melhores provedores. A iniciar o fluxo de ofertas...")
            for index, match in best_matches_df.iterrows():
                match_data = match.to_dict()
                provider_phone_number = match_data.get('mobile')

                if provider_phone_number:
                    run_offer_workflow(provider_phone_number, match_data)
                    print("-" * 50)
                else:
                    print(f"AVISO: Não foi possível encontrar o número de celular para o provedor ID {match_data.get('provider_id')}. A pular.")
        else:
            print(f"Nenhum provedor encontrado dentro de um raio de 10km para a cidade {CITY_ID_PRODUCAO}.")
    else:
        print(f"\nNão foram encontradas corridas travadas ou provedores disponíveis para a cidade {CITY_ID_PRODUCAO}.")


if __name__ == "__main__":
    # Agora, se executarmos 'python main.py', ele apenas executa a lógica uma vez para teste.
    execute_sai_logic()