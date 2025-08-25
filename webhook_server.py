# webhook_server.py

import os
import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from internal_api import login, assign_order
from log_db import log_sai_event
from datetime import datetime
from db import read_data_from_db
from query import query_order_status, query_provider_by_phone, query_best_stuck_order_for_provider
from chatguru_api import ChatguruWABA

load_dotenv()
app = Flask(__name__)

GIROSS_EMAIL = os.getenv("GIROSS_EMAIL")
GIROSS_PASSWORD = os.getenv("GIROSS_PASSWORD")

# --- Instancia a API do Chatguru para ser usada no modo passivo ---
CHAT_GURU_KEY = os.getenv("CHAT_GURU_KEY")
CHAT_GURU_ACCOUNT_ID = os.getenv("CHAT_GURU_ACCOUNT_ID")
CHAT_GURU_PHONE_ID = os.getenv("CHAT_GURU_PHONE_ID")
CHAT_GURU_URL = os.getenv("CHAT_GURU_URL")

chat_api = None
if all([CHAT_GURU_KEY, CHAT_GURU_ACCOUNT_ID, CHAT_GURU_PHONE_ID, CHAT_GURU_URL]):
    chat_api = ChatguruWABA(CHAT_GURU_KEY, CHAT_GURU_ACCOUNT_ID, CHAT_GURU_PHONE_ID, CHAT_GURU_URL)
else:
    print("AVISO: Credenciais do Chatguru não configuradas. O modo passivo não poderá enviar respostas.")

def find_next_provider_and_send_offer(order_id, rejected_provider_id):
    """Placeholder para a lógica de encontrar o próximo melhor provedor."""
    print(f"\nAVISO: O provedor {rejected_provider_id} rejeitou a ordem {order_id}.")
    print("A lógica para encontrar o próximo provedor precisa ser implementada aqui.")
    # Em desenvolvimento
    pass

@app.route('/webhook', methods=['POST'])
def receive_message():
    """
    Recebe a resposta do provedor, registra o evento e toma a ação apropriada.
    """
    print("\n" + "="*50)
    print(f">>> ROTA /webhook ACIONADA ÀS {datetime.now()} <<<")
    print(f"INFO: Método da Requisição: {request.method}")
    print(f"INFO: IP de Origem: {request.remote_addr}")
    print("="*50)

    data = request.json
    print("\n" + "="*50)
    print(">>> DADOS JSON RECEBIDOS DO CHATGURU <<<")
    print(data)
    
    bot_context = data.get('bot_context', {})
    response_status = bot_context.get('Status')
    custom_fields = data.get('campos_personalizados', {})
    
    order_id = custom_fields.get('order_id')
    provider_id = custom_fields.get('provider_id')

    if not order_id or not provider_id:
        print("ERRO: 'order_id' ou 'provider_id' não encontrados nos campos personalizados.")
        return jsonify({"status": "error", "message": "Missing required custom fields"}), 400

    order_id_int = int(order_id)
    provider_id_int = int(provider_id)

    if response_status == 'Resposta_sim':
        print(f"INFO: Provedor {provider_id_int} ACEITOU a ordem {order_id_int}.")
        log_sai_event(order_id_int, provider_id_int, 'PROVIDER_ACCEPTED')
        
        # --- INÍCIO DA NOVA LÓGICA DE VERIFICAÇÃO ---
        print(f"INFO: Verificando o status atual da ordem {order_id_int} no banco de dados...")
        order_status_df = read_data_from_db(query_order_status(order_id_int))

        if order_status_df is None or order_status_df.empty:
            print(f"ERRO: Não foi possível encontrar a ordem {order_id_int} no banco de dados para verificação.")
            log_sai_event(order_id_int, provider_id_int, 'VERIFICATION_FAILED_NOT_FOUND')
            return jsonify({"status": "error", "message": "Order not found for verification"}), 404

        current_provider_id = order_status_df.iloc[0]['provider_id']

        # Verifica se a corrida já foi atribuída (provider_id diferente de 0 ou 1266)
        if current_provider_id not in [0, 1266]:
            print(f"INFO: A ordem {order_id_int} já foi atribuída ao provedor {current_provider_id}. Esta oferta não está mais disponível para o provedor {provider_id_int}.")
            log_sai_event(order_id_int, provider_id_int, 'ORDER_ALREADY_TAKEN')
            return jsonify({"status": "success", "message": "Order already assigned to another provider"}), 200
        
        print(f"INFO: A ordem {order_id_int} está disponível. Tentando atribuir ao provedor {provider_id_int}...")
        # --- FIM DA NOVA LÓGICA DE VERIFICAÇÃO ---

        access_token = login(GIROSS_EMAIL, GIROSS_PASSWORD)
        
        if access_token:
            success = assign_order(access_token, provider_id_int, order_id_int)
            if success:
                log_sai_event(order_id_int, provider_id_int, 'ASSIGNMENT_SUCCESS')
            else:
                log_sai_event(order_id_int, provider_id_int, 'ASSIGNMENT_FAILURE')
        else:
            print("ERRO: Não foi possível atribuir a ordem devido a falha no login.")
            log_sai_event(order_id_int, provider_id_int, 'ASSIGNMENT_FAILURE_LOGIN')
            
    else:
        log_sai_event(order_id_int, provider_id_int, 'PROVIDER_REJECTED')
        find_next_provider_and_send_offer(order_id, provider_id)
        
    return jsonify({"status": "success"}), 200

@app.route('/request_order', methods=['POST'])
def request_order():
    """
    Nova rota para o modo passivo. Recebe a solicitação de um entregador,
    encontra a melhor corrida e a aloca.
    """
    print("\n" + "="*50)
    print(f">>> ROTA /request_order (SAI PASSIVO) ACIONADA ÀS {datetime.now()} <<<")
    
    data = request.json
    chat_number = data.get('chat_number')

    if not chat_number:
        print("ERRO: 'chat_number' não encontrado no webhook.")
        return jsonify({"status": "error", "message": "Missing chat_number"}), 400

    # 1. Identificar o entregador pelo telefone
    provider_df = read_data_from_db(query_provider_by_phone(chat_number))

    if provider_df is None or provider_df.empty:
        print(f"ERRO: Nenhum provedor encontrado com o número {chat_number}.")
        if chat_api:
            chat_api.send_text_message(chat_number, "Desculpe, não conseguimos encontrar seu cadastro em nosso sistema.")
        return jsonify({"status": "error", "message": "Provider not found"}), 404

    provider_info = provider_df.iloc[0]
    provider_id = int(provider_info['provider_id'])
    
    # 2. Validar se o entregador está apto a receber uma corrida
    if provider_info['provider_status'] != 'active':
        print(f"INFO: Provedor {provider_id} solicitou corrida, mas seu status é '{provider_info['provider_status']}'.")
        log_sai_event(0, provider_id, 'PASSIVE_ASSIGNMENT_PROVIDER_NOT_ACTIVE')
        if chat_api:
            chat_api.send_text_message(chat_number, "Olá! Para buscar corridas, seu status precisa estar 'Online' no aplicativo. Por favor, fique online e tente novamente.")
        return jsonify({"status": "success"}), 200

    if pd.isna(provider_info['provider_latitude']) or pd.isna(provider_info['provider_longitude']):
        print(f"ERRO: Provedor {provider_id} está sem dados de localização.")
        log_sai_event(0, provider_id, 'PASSIVE_ASSIGNMENT_NO_LOCATION')
        if chat_api:
            chat_api.send_text_message(chat_number, "Não conseguimos encontrar sua localização atual. Por favor, verifique se o GPS está ativado e tente novamente.")
        return jsonify({"status": "success"}), 200

    # 3. Encontrar a melhor corrida para este entregador
    print(f"INFO: Buscando a melhor corrida para o provedor {provider_id}...")
    best_order_df = read_data_from_db(query_best_stuck_order_for_provider(
        provider_id=provider_id,
        provider_lat=provider_info['provider_latitude'],
        provider_lon=provider_info['provider_longitude']
    ))

    # 4. Lógica de Alocação e Resposta
    if best_order_df is None or best_order_df.empty:
        print(f"INFO: Nenhuma corrida encontrada para o provedor {provider_id}.")
        log_sai_event(0, provider_id, 'PASSIVE_ASSIGNMENT_NO_ORDER_FOUND')
        if chat_api:
            chat_api.send_text_message(chat_number, "Obrigado pela disponibilidade! No momento, não encontramos corridas travadas perto de você. Continue online!")
    else:
        best_order_info = best_order_df.iloc[0]
        order_id = int(best_order_info['order_id'])
        print(f"INFO: Melhor corrida encontrada: {order_id} (Distância: {best_order_info['distance_km']:.2f} km). Alocando...")

        access_token = login(GIROSS_EMAIL, GIROSS_PASSWORD)
        if access_token:
            success = assign_order(access_token, provider_id, order_id)
            if success:
                print(f"SUCESSO: Corrida {order_id} alocada para o provedor {provider_id}.")
                log_sai_event(order_id, provider_id, 'PASSIVE_ASSIGNMENT_SUCCESS')
                if chat_api:
                    chat_api.send_text_message(chat_number, f"✅ Ótima notícia! Alocamos a corrida #{order_id} para você. Por favor, verifique os detalhes no seu aplicativo.")
            else:
                print(f"FALHA: A API interna falhou ao tentar alocar a corrida {order_id} para o provedor {provider_id}.")
                log_sai_event(order_id, provider_id, 'PASSIVE_ASSIGNMENT_FAILURE')
                if chat_api:
                    chat_api.send_text_message(chat_number, f"⚠️ Encontramos a corrida #{order_id}, mas ocorreu um erro ao tentar alocá-la. Por favor, tente novamente em alguns instantes.")
        else:
            print("ERRO: Falha no login da API interna. Não foi possível alocar a corrida.")
            log_sai_event(order_id, provider_id, 'PASSIVE_ASSIGNMENT_LOGIN_FAILURE')
            if chat_api:
                chat_api.send_text_message(chat_number, "Ocorreu um erro interno em nosso sistema. Nossa equipe já foi notificada. Por favor, tente novamente mais tarde.")

    return jsonify({"status": "success"}), 200
if __name__ == '__main__':
    # Em produção, este script será executado pelo Docker/CapRover.
    # Para testes locais, pode usar o waitress:
    # waitress-serve --host 127.0.0.1 --port=5000 webhook_server:app
    app.run(port=5000, debug=True)
