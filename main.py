# main.py

import os
import argparse
from time import sleep
from dotenv import load_dotenv
from chatguru_api import ChatguruWABA
from db import read_data_from_db
from query import query_stuck_orders, query_available_providers
from geopy.distance import geodesic
import pandas as pd

CITY_ID_PRODUCAO = 50
DIALOG_ID_PARA_OFERTA = "68681a2827f824ecd929292a" 
AVG_SPEED_KMH = 25

def run_offer_workflow(chat_number, match_data, custom_fields_data):
    """
    Executa o fluxo completo, atualizando todos os campos necessários antes de enviar.
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
    
    print(f"Etapa 1: Registrando chat com o número {chat_number}...")
    api.register_chat(chat_number, match_data.get("provider_name", "Novo Provedor"))

    print(f"Etapa 2: Atualizando campos para o chat {chat_number}...")
    api.update_custom_fields(chat_number, custom_fields_data)

    print("Aguardando 2 segundos para sincronização...")
    sleep(2)

    if DIALOG_ID_PARA_OFERTA:
        print(f"Etapa 3: Executando diálogo '{DIALOG_ID_PARA_OFERTA}'...")
        # A chamada correta, com apenas os argumentos necessários
        dialog_response = api.execute_dialog(chat_number, DIALOG_ID_PARA_OFERTA)
        print(f"Resposta da Execução do Diálogo para {chat_number}:", dialog_response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sistema de Assignação Inteligente (SAI)")
    parser.add_argument("--numero-teste", type=str, help="Envia a oferta para um número de teste.")
    parser.add_argument("--limite", type=int, default=0, help="Limita o número de ofertas a serem enviadas.")
    args = parser.parse_args()
    
    print(f"--- A INICIAR SAI PARA A CIDADE ID: {CITY_ID_PRODUCAO} ---")
    stuck_orders_df = read_data_from_db(query_stuck_orders(CITY_ID_PRODUCAO))
    providers_df = read_data_from_db(query_available_providers())

    if stuck_orders_df is not None and not stuck_orders_df.empty and providers_df is not None and not providers_df.empty:
        # Lógica de combinação e cálculo...
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
            if args.limite > 0:
                best_matches_df = best_matches_df.head(args.limite)
            
            print(f"\nEncontrados {len(best_matches_df)} melhores provedores. A iniciar o fluxo de ofertas...")
            
            for index, match in best_matches_df.iterrows():
                match_data = match.to_dict()
                
                try:
                    # Construção do dicionário de campos personalizados
                    custom_fields_payload = {
                        "order_id": str(match_data.get('order_id')),
                        "provider_id": str(match_data.get('provider_id')),
                        "valor_corrida": f"{match_data.get('value', 'N/D'):.2f}",
                        "endereco_coleta": match_data.get('user_name', 'N/D'),
                        "distancia_ate_loja": f"~{match_data.get('distance_km', 0):.1f} km",
                        "tempo_ate_loja": f"~{int((match_data.get('distance_km', 0) / AVG_SPEED_KMH) * 60)} min",
                        "distancia_corrida_km": "[Distância N/D]",
                        "eta_corrida_total": "[ETA N/D]"
                    }

                    recipient_phone_number = args.numero_teste if args.numero_teste else match_data.get('mobile')
                    
                    if recipient_phone_number:
                        run_offer_workflow(recipient_phone_number, match_data, custom_fields_payload)
                        print("-" * 50)
                
                except Exception as e:
                    print(f"ERRO ao processar o match para a ordem {match_data.get('order_id')}: {e}")
        else:
            print("Nenhum provedor foi encontrado.")