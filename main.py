# main.py

import os
import argparse
from time import sleep
from dotenv import load_dotenv
from chatguru_api import ChatguruWABA
from db import read_data_from_db
from query import query_stuck_orders, query_available_providers, query_blocked_pairs
from geopy.distance import geodesic
import pandas as pd
from log_db import log_sai_event

pd.set_option('display.max_columns', None)

CITY_ID_PRODUCAO = 50
DIALOG_ID_PARA_OFERTA = "68681a2827f824ecd929292a" 
AVG_SPEED_KMH = 25

def run_offer_workflow(chat_number, match_data, template_params):
    """
    Executa o fluxo completo para enviar uma oferta de corrida e registrar o evento.
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
    
    print(f"Etapa 1: Registrando e atualizando campos para {chat_number}...")
    api.register_chat(chat_number, match_data.get("provider_name", "Novo Provedor"))
    custom_fields = {"order_id": str(match_data.get('order_id')), "provider_id": str(match_data.get('provider_id'))}
    api.update_custom_fields(chat_number, custom_fields)

    print("Aguardando 2 segundos para sincronização...")
    sleep(2)

    if DIALOG_ID_PARA_OFERTA:
        print(f"Etapa 2: Executando diálogo '{DIALOG_ID_PARA_OFERTA}'...")
        dialog_response = api.execute_dialog(chat_number, DIALOG_ID_PARA_OFERTA, template_params)
        print(f"Resposta da Execução do Diálogo para {chat_number}:", dialog_response)
        
        if dialog_response and dialog_response.get('result') == 'success':
            log_metadata = {
                "distance_to_store_km": match_data.get('distance_km'),
                "provider_score": match_data.get('score'),
                "provider_releases": match_data.get('total_releases_last_2_weeks')
            }
            log_sai_event(
                order_id=match_data['order_id'],
                provider_id=match_data['provider_id'],
                event_type='OFFER_SENT',
                metadata=log_metadata
            )

def clean_and_format_phone(phone_number):
    """
    Limpa e formata um número de telefone brasileiro para o formato E.164.
    """
    if not phone_number or not isinstance(phone_number, str):
        return None
    cleaned_number = "".join(filter(str.isdigit, phone_number))
    if cleaned_number.startswith('0'):
        cleaned_number = cleaned_number[1:]
    if cleaned_number.startswith('55'):
        return cleaned_number[:13]
    if len(cleaned_number) in [10, 11]:
        return f"55{cleaned_number}"
    return cleaned_number

# --- FUNÇÃO PRINCIPAL DA LÓGICA DE NEGÓCIO ---
def execute_sai_logic(limit=0, test_number=None, print_dfs=False):
    """
    Encapsula toda a lógica de busca, correspondência e oferta.
    Pode ser chamada pelo worker.py ou para testes.
    """
    print(f"--- A INICIAR LÓGICA DO SAI PARA A CIDADE ID: {CITY_ID_PRODUCAO} ---")
    
    stuck_orders_df = read_data_from_db(query_stuck_orders(CITY_ID_PRODUCAO))
    providers_df = read_data_from_db(query_available_providers())
    blocked_pairs_df = read_data_from_db(query_blocked_pairs())

    if providers_df is not None and not providers_df.empty:
        providers_df['mobile'] = providers_df['mobile'].apply(clean_and_format_phone)
        providers_df.dropna(subset=['mobile'], inplace=True)

    if print_dfs:
        print("\n" + "="*20 + " DEBUG: stuck_orders_df " + "="*20); print(stuck_orders_df.head())
        print("\n" + "="*20 + " DEBUG: providers_df " + "="*20); print(providers_df.head())
        print("\n" + "="*20 + " DEBUG: blocked_pairs_df " + "="*20); print(blocked_pairs_df.head())

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
        merged_df = pd.merge(all_combinations_df, blocked_pairs_df, on=['user_id', 'provider_id'], how='left', indicator=True)
        valid_combinations_df = merged_df[merged_df['_merge'] == 'left_only'].copy()
        def calculate_distance(row):
            order_coords = (row['store_latitude'], row['store_longitude'])
            provider_coords = (row['latitude'], row['longitude'])
            return geodesic(order_coords, provider_coords).kilometers
        valid_combinations_df['distance_km'] = valid_combinations_df.apply(calculate_distance, axis=1)
        nearby_providers_df = valid_combinations_df[valid_combinations_df['distance_km'] <= 10].copy()
        nearby_providers_df.sort_values(by=['order_id', 'distance_km', 'total_releases_last_2_weeks', 'score'], ascending=[True, True, True, False], inplace=True)
        best_matches_df = nearby_providers_df.groupby('order_id').first().reset_index()

        if print_dfs:
            print("\n" + "="*20 + " DEBUG: best_matches_df (Final) " + "="*20); print(best_matches_df.head())

        if not best_matches_df.empty:
            if limit > 0:
                best_matches_df = best_matches_df.head(limit)
            
            print(f"\nEncontrados {len(best_matches_df)} melhores provedores. A iniciar o fluxo de ofertas...")
            for index, match in best_matches_df.iterrows():
                match_data = match.to_dict()
                try:
                    param1 = match_data.get('param1_valor', '💰 Valor da Corrida: N/D')
                    param2 = match_data.get('param2_endereco', '📍 Endereço de Coleta: N/D')
                    dist_to_store = match_data.get('distance_km', 0)
                    eta_to_store = int((dist_to_store / AVG_SPEED_KMH) * 60)
                    param3 = f"Sua Situação:\n- Distância até a coleta: ~{dist_to_store:.1f} km\n- Tempo estimado até a coleta: ~{eta_to_store} min"
                    store_to_delivery_dist = match_data.get('store_to_delivery_distance', 0)
                    total_dist = dist_to_store + store_to_delivery_dist
                    total_eta = int((total_dist / AVG_SPEED_KMH) * 60)
                    param4 = f"Detalhes da Entrega:\n- Percurso total da corrida: ~{total_dist:.1f} km\n- Tempo estimado total (coleta + entrega): ~{total_eta} min"
                    template_params = [param1, param2, param3, param4]
                    
                    recipient_phone_number = test_number if test_number else match_data.get('mobile')
                    if recipient_phone_number:
                        run_offer_workflow(recipient_phone_number, match_data, template_params)
                        print("-" * 50)
                except Exception as e:
                    print(f"ERRO ao processar o match para a ordem {match_data.get('order_id')}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sistema de Assignação Inteligente (SAI)")
    parser.add_argument("--numero-teste", type=str, help="Envia a oferta para um número de teste específico.")
    parser.add_argument("--limite", type=int, default=0, help="Limita o número de ofertas a serem enviadas (0 para sem limite).")
    parser.add_argument("--print-dfs", action="store_true", help="Imprime o cabeçalho dos DataFrames intermediários no console.")
    args = parser.parse_args()
    
    # Chama a função principal com os argumentos da linha de comando
    execute_sai_logic(limit=args.limite, test_number=args.numero_teste, print_dfs=args.print_dfs)
