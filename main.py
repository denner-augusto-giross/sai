# main.py

import os
import argparse
from time import sleep
from dotenv import load_dotenv
from chatguru_api import ChatguruWABA
from db import read_data_from_db
from query import (
    query_stuck_orders, query_available_providers, query_blocked_pairs, 
    query_offers_sent, query_offline_providers_with_history, 
    query_responsive_providers, query_fixed_providers, query_sai_city_configs,
    query_providers_on_unanswered_cooldown, query_offline_providers_by_city, # <-- Novas importações
    query_providers_on_active_orders
)
from geopy.distance import geodesic
import pandas as pd
from log_db import log_sai_event, read_log_data

pd.set_option('display.max_columns', None)

# --- CONFIGURAÇÕES GLOBAIS QUE NÃO VARIAM POR CIDADE ---
DIALOG_ID_PARA_OFERTA = "68681a2827f824ecd929292a" 
AVG_SPEED_KMH = 25
FILTER_ONLY_ACTIVE_PROVIDERS = False
# ---------------------------------------------------------


def run_offer_workflow(chat_number, match_data, template_params):
    """
    Executa o fluxo completo, com verificação de status do chat e logging.
    Retorna a resposta da API do Chatguru.
    """
    load_dotenv()
    chat_key = os.getenv("CHAT_GURU_KEY")
    chat_account_id = os.getenv("CHAT_GURU_ACCOUNT_ID")
    chat_phone_id = os.getenv("CHAT_GURU_PHONE_ID")
    chat_url = os.getenv("CHAT_GURU_URL")

    if not all([chat_key, chat_account_id, chat_phone_id, chat_url]):
        print("ERRO: Credenciais do Chatguru não encontradas no .env")
        return None

    api = ChatguruWABA(chat_key, chat_account_id, chat_phone_id, chat_url)
    
    print(f"Etapa 1: Registrando chat com o número {chat_number}...")
    register_response = api.register_chat(chat_number, match_data.get("provider_name", "Novo Provedor"))
    
    chat_add_id = register_response.get('chat_add_id')
    if not chat_add_id:
        print(f"ERRO: Falha ao iniciar o registro do chat para {chat_number}. Resposta: {register_response}")
        return register_response

    final_chat_number = chat_number
    for i in range(5):
        print(f"Verificando status do registro do chat (tentativa {i+1}/5)...")
        status_response = api.check_chat_status(chat_add_id)
        chat_status = status_response.get('chat_add_status')
        description = status_response.get('chat_add_status_description', '')
        
        if chat_status in ['success', 'fetched', 'done']:
            print(f"SUCESSO: Chat registrado ou encontrado com sucesso (status: {chat_status})!")
            if 'corrigido para' in description:
                try:
                    corrected_number_raw = description.split('corrigido para ')[1].strip().replace('.', '')
                    corrected_number = "".join(filter(str.isdigit, corrected_number_raw))
                    if corrected_number:
                        final_chat_number = corrected_number
                        print(f"INFO: O número do chat foi corrigido para {final_chat_number}")
                except IndexError:
                    print("AVISO: A descrição continha 'corrigido para', mas não foi possível extrair o novo número.")
            break
        
        if chat_status != 'pending':
            print(f"ERRO: O registro do chat falhou com o status '{chat_status}'. Descrição: {description}")
            return status_response
        sleep(3)
    else:
        print(f"ERRO: Timeout. O registro do chat para {chat_number} não foi concluído após 15 segundos.")
        return {"result": "error", "description": "Timeout on chat registration"}

    print(f"Etapa 2: Atualizando campos personalizados para o chat {final_chat_number}...")
    custom_fields = {"order_id": str(match_data.get('order_id')), "provider_id": str(match_data.get('provider_id'))}
    api.update_custom_fields(final_chat_number, custom_fields)

    dialog_response = None
    if DIALOG_ID_PARA_OFERTA:
        print(f"Etapa 3: Executando diálogo '{DIALOG_ID_PARA_OFERTA}'...")
        dialog_response = api.execute_dialog(final_chat_number, DIALOG_ID_PARA_OFERTA, template_params)
        print(f"Resposta da Execução do Diálogo para {final_chat_number}:", dialog_response)
    
    return dialog_response

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

def process_city_offers(city_config, test_number=None, print_dfs=False, limit=0):
    """
    Encapsula toda a lógica de busca e oferta para UMA ÚNICA CIDADE.
    """
    city_id = city_config['city_id']
    city_name = city_config['city_name']
    stuck_threshold = city_config['stuck_order_threshold_minutes']
    max_offers = city_config['max_offers_per_order']
    offer_distance = city_config['offer_distance_km']
    max_unanswered = city_config['max_unanswered_offers']
    cooldown_hours = city_config['unanswered_cooldown_hours']
    offer_all_offline = city_config.get('offer_to_all_city_offline', False) # Usar .get() para retrocompatibilidade

    print(f"\n--- PROCESSANDO CIDADE: {city_name} (ID: {city_id}) ---")
    print(f"Configurações: Limite Travada={stuck_threshold}min, MaxOfertas={max_offers}, Distância={offer_distance}km")
    
    stuck_orders_df = read_data_from_db(query_stuck_orders(city_id, stuck_threshold))
    
    if stuck_orders_df is None or stuck_orders_df.empty:
        print(f"INFO: Nenhuma corrida travada encontrada para {city_name}.")
        return

    online_providers_df = read_data_from_db(query_available_providers())
    
    # --- LÓGICA CONDICIONAL PARA BUSCAR PROVEDORES OFFLINE ---
    if offer_all_offline:
        print(f"INFO: {city_name} é uma cidade pequena. Buscando TODOS os entregadores offline da cidade.")
        offline_providers_df = read_data_from_db(query_offline_providers_by_city(city_id))
    else:
        print(f"INFO: {city_name} é uma cidade grande. Buscando apenas entregadores offline com histórico na loja.")
        store_ids = stuck_orders_df['user_id'].unique().tolist()
        offline_providers_df = read_data_from_db(query_offline_providers_with_history(store_ids))
    # --- FIM DA LÓGICA CONDICIONAL ---

    if online_providers_df is not None:
        online_providers_df['offer_priority'] = 1
    if offline_providers_df is not None:
        offline_providers_df['offer_priority'] = 2

    providers_df = pd.concat([online_providers_df, offline_providers_df], ignore_index=True)
    
    print("\nINFO: Verificando e removendo provedores que já estão em corridas ativas...")
    busy_providers_df = read_data_from_db(query_providers_on_active_orders())
    if busy_providers_df is not None and not busy_providers_df.empty:
        busy_provider_ids = busy_providers_df['provider_id'].tolist()
        initial_count = len(providers_df)
        providers_df = providers_df[~providers_df['provider_id'].isin(busy_provider_ids)]
        print(f"INFO: {len(busy_provider_ids)} provedores em corrida removidos. {initial_count} -> {len(providers_df)} provedores restantes.")
    else:
        print("INFO: Nenhum provedor em corrida ativa encontrado.")

    print("\nINFO: Buscando e removendo provedores fixos da lista de ofertas...")
    fixed_providers_df = read_data_from_db(query_fixed_providers())
    if fixed_providers_df is not None and not fixed_providers_df.empty:
        fixed_provider_ids = fixed_providers_df['provider_id'].tolist()
        initial_count = len(providers_df)
        providers_df = providers_df[~providers_df['provider_id'].isin(fixed_provider_ids)]
        print(f"INFO: {len(fixed_provider_ids)} provedores fixos removidos. {initial_count} -> {len(providers_df)} provedores restantes.")
    else:
        print("INFO: Nenhum provedor fixo ativo encontrado.")

    print(f"\nINFO: Verificando provedores em cooldown (mais de {max_unanswered} ofertas ignoradas em {cooldown_hours}h)...")
    cooldown_providers_df = read_log_data(query_providers_on_unanswered_cooldown(max_unanswered, cooldown_hours))
    if cooldown_providers_df is not None and not cooldown_providers_df.empty:
        cooldown_provider_ids = cooldown_providers_df['provider_id'].tolist()
        initial_count = len(providers_df)
        providers_df = providers_df[~providers_df['provider_id'].isin(cooldown_provider_ids)]
        print(f"INFO: {len(cooldown_provider_ids)} provedores em cooldown removidos. {initial_count} -> {len(providers_df)} provedores restantes.")
    else:
        print("INFO: Nenhum provedor em cooldown por ignorar ofertas encontrado.")

    if FILTER_ONLY_ACTIVE_PROVIDERS:
        print("\nINFO: Filtro de provedores ativos está LIGADO.")
        responsive_providers_df = read_log_data(query_responsive_providers())
        
        if responsive_providers_df is not None and not responsive_providers_df.empty:
            active_provider_ids = responsive_providers_df['provider_id'].tolist()
            initial_provider_count = len(providers_df)
            providers_df = providers_df[providers_df['provider_id'].isin(active_provider_ids)]
            print(f"INFO: {initial_provider_count} provedores totais -> {len(providers_df)} provedores ativos encontrados e filtrados.")
        else:
            print("AVISO: Nenhum provedor ativo encontrado no log. Nenhuma oferta será enviada.")
            providers_df = pd.DataFrame()
    else:
        print("\nINFO: Filtro de provedores ativos está DESLIGADO. Considerando todos os provedores elegíveis.")

    blocked_pairs_df = read_data_from_db(query_blocked_pairs())
    offers_sent_df = read_log_data(query_offers_sent())
    
    if offers_sent_df is None:
        print("AVISO: A tabela 'sai_event_log' não foi encontrada. A assumir que nenhuma oferta foi enviada.")
        offers_sent_df = pd.DataFrame(columns=['order_id', 'provider_id'])
    
    if providers_df.empty:
        print(f"INFO: Nenhum entregador elegível para {city_name} após os filtros.")
        return

    providers_df['mobile'] = providers_df['mobile'].apply(clean_and_format_phone)
    providers_df.dropna(subset=['mobile'], inplace=True)

    if print_dfs:
        print("\n" + "="*20 + " DEBUG: stuck_orders_df " + "="*20); print(stuck_orders_df.head())
        print("\n" + "="*20 + " DEBUG: providers_df (com prioridade) " + "="*20); print(providers_df.head())
        print("\n" + "="*20 + " DEBUG: blocked_pairs_df " + "="*20); print(blocked_pairs_df.head())

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
    valid_combinations_df = merged_df[merged_df['_merge'] == 'left_only'].drop('_merge', axis=1).copy()

    if not offers_sent_df.empty:
        valid_combinations_df = pd.merge(
            valid_combinations_df,
            offers_sent_df,
            on=['order_id', 'provider_id'],
            how='left',
            indicator=True
        ).query('_merge == "left_only"').drop('_merge', axis=1)
    
    print(f"INFO: {len(valid_combinations_df)} combinações restantes após todos os filtros.")
    
    if not valid_combinations_df.empty:
        def calculate_distance(row):
            order_coords = (row['store_latitude'], row['store_longitude'])
            provider_coords = (row['latitude'], row['longitude'])
            return geodesic(order_coords, provider_coords).kilometers
        
        valid_combinations_df['distance_km'] = valid_combinations_df.apply(calculate_distance, axis=1)
        nearby_providers_df = valid_combinations_df[valid_combinations_df['distance_km'] <= offer_distance].copy()
        
        nearby_providers_df.sort_values(
            by=['order_id', 'offer_priority', 'distance_km', 'total_releases_last_2_weeks', 'score'],
            ascending=[True, True, True, True, False],
            inplace=True
        )
        
        best_matches_df = nearby_providers_df.groupby('order_id').head(max_offers).reset_index()

        if print_dfs:
            print("\n" + "="*20 + " DEBUG: best_matches_df (Final com Prioridade) " + "="*20); print(best_matches_df.head())

        if not best_matches_df.empty:
            if limit > 0:
                best_matches_df = best_matches_df.head(limit)
            
            sent_providers_this_run = set()
            
            print(f"\nEncontrados {len(best_matches_df)} melhores provedores para {city_name}. A iniciar o fluxo de ofertas...")
            for index, match in best_matches_df.iterrows():
                match_data = match.to_dict()
                provider_id = match_data.get('provider_id')

                if provider_id in sent_providers_this_run:
                    print(f"INFO: Provedor {provider_id} já recebeu uma oferta nesta execução. Pulando para evitar spam.")
                    continue

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
                        dialog_response = run_offer_workflow(recipient_phone_number, match_data, template_params)
                        print("-" * 50)
                        
                        sent_providers_this_run.add(provider_id)

                        log_metadata = {
                            "distance_to_store_km": match_data.get('distance_km'),
                            "provider_score": match_data.get('score'),
                            "provider_releases": match_data.get('total_releases_last_2_weeks'),
                            "offer_priority": match_data.get('offer_priority')
                        }

                        if dialog_response and dialog_response.get('result') == 'success':
                            log_sai_event(
                                order_id=match_data['order_id'],
                                provider_id=match_data['provider_id'],
                                event_type='OFFER_SENT',
                                metadata=log_metadata
                            )
                        else:
                            log_metadata['api_error_response'] = dialog_response
                            log_sai_event(
                                order_id=match_data['order_id'],
                                provider_id=match_data['provider_id'],
                                event_type='OFFER_DELIVERY_FAILURE',
                                metadata=log_metadata
                            )
                except Exception as e:
                    print(f"ERRO ao processar o match para a ordem {match_data.get('order_id')}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sistema de Assignação Inteligente (SAI) - Teste Local")
    parser.add_argument("--city-id", type=int, required=True, help="ID da cidade para executar o teste.")
    parser.add_argument("--numero-teste", type=str, help="Envia a oferta para um número de teste específico.")
    parser.add_argument("--limite", type=int, default=0, help="Limita o número TOTAL de ofertas a serem enviadas neste teste.")
    parser.add_argument("--print-dfs", action="store_true", help="Imprime os DataFrames de depuração no console.")
    args = parser.parse_args()
    
    print("--- MODO DE TESTE LOCAL ATIVADO ---")
    
    all_configs_df = read_log_data(query_sai_city_configs())
    
    if all_configs_df is None or all_configs_df.empty:
        print(f"ERRO: Nenhuma configuração de cidade encontrada no banco de dados.")
    else:
        city_config_df = all_configs_df[all_configs_df['city_id'] == args.city_id]
        
        if city_config_df.empty:
            print(f"ERRO: Nenhuma configuração encontrada para a cidade com ID {args.city_id}.")
        else:
            city_config = city_config_df.iloc[0].to_dict()
            process_city_offers(
                city_config=city_config,
                test_number=args.numero_teste,
                print_dfs=args.print_dfs,
                limit=args.limite
            )
