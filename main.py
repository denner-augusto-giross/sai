# main.py

import os
from time import sleep
from dotenv import load_dotenv
from chatguru_api import ChatguruWABA
from db import read_data_from_db
from query import query_stuck_orders, query_available_providers
from geopy.distance import geodesic
import pandas as pd

CITY_ID_PRODUCAO = 50
DIALOG_ID_PARA_OFERTA = "68681a2827f824ecd929292a"
AVG_SPEED_KMH = 25 # Velocidade média de um entregador em km/h para calcular o ETA

def run_offer_workflow(chat_number, match_data, template_params):
    """
    Executa o fluxo completo, agora recebendo a lista de parâmetros do template.
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
    
    # Etapa 1: Registrar e Atualizar Campos
    print(f"Etapa 1: Registrando e atualizando campos para {chat_number}...")
    api.register_chat(chat_number, match_data.get("provider_name", "Novo Provedor"))
    custom_fields = {"order_id": str(match_data.get('order_id')), "provider_id": str(match_data.get('provider_id'))}
    api.update_custom_fields(chat_number, custom_fields)

    print("Aguardando 2 segundos para sincronização...")
    sleep(2)

    # Etapa 2: Executar o Diálogo com os parâmetros do template
    if DIALOG_ID_PARA_OFERTA:
        print(f"Etapa 2: Executando diálogo '{DIALOG_ID_PARA_OFERTA}' com os dados da corrida...")
        api.execute_dialog_with_template(chat_number, DIALOG_ID_PARA_OFERTA, "request_giross", "en_US", template_params)


if __name__ == "__main__":
    
    # --- A lógica para encontrar a melhor correspondência permanece a mesma ---
    stuck_orders_df = read_data_from_db(query_stuck_orders(CITY_ID_PRODUCAO))
    providers_df = read_data_from_db(query_available_providers(CITY_ID_PRODUCAO))

    if stuck_orders_df is not None and not stuck_orders_df.empty and providers_df is not None and not providers_df.empty:
        # ... (código para criar best_matches_df) ...
        # ... (incluindo o cálculo de 'distance_km' que é a distância do provedor até a loja)

        # --- NOVA LÓGICA DE CÁLCULO AQUI ---
        if not best_matches_df.empty:
            print(f"\nEncontrados {len(best_matches_df)} melhores provedores. A preparar e enviar ofertas...")
            
            for index, match in best_matches_df.iterrows():
                match_data = match.to_dict()
                
                # Calcular as novas distâncias
                store_coords = (match_data['store_latitude'], match_data['store_longitude'])
                delivery_coords = (match_data['delivery_latitude'], match_data['delivery_longitude'])
                store_to_delivery_distance = geodesic(store_coords, delivery_coords).kilometers
                total_distance = match_data['distance_km'] + store_to_delivery_distance

                # Calcular os ETAs (Tempo = Distância / Velocidade) -> convertendo para minutos
                eta_to_store_minutes = int((match_data['distance_km'] / AVG_SPEED_KMH) * 60)
                eta_total_minutes = int((total_distance / AVG_SPEED_KMH) * 60)

                # Montar a lista de parâmetros na ordem correta
                template_params = [
                    f"{match_data.get('value', 'N/D'):.2f}",
                    "[Endereço da Loja]", # Placeholder
                    f"~{match_data.get('distance_km', 0):.1f} km",
                    f"~{eta_to_store_minutes} min",
                    f"~{total_distance:.1f} km",
                    f"~{eta_total_minutes} min"
                ]

                provider_phone_number = match_data.get('mobile')
                if provider_phone_number:
                    # Inicia o fluxo de trabalho, agora passando os parâmetros
                    run_offer_workflow(provider_phone_number, match_data, template_params)
                    print("-" * 50)