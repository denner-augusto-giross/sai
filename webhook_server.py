# webhook_server.py

import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from internal_api import login, assign_order
from log_db import log_sai_event
from datetime import datetime
from db import read_data_from_db
from query import query_order_status

load_dotenv()
app = Flask(__name__)

GIROSS_EMAIL = os.getenv("GIROSS_EMAIL")
GIROSS_PASSWORD = os.getenv("GIROSS_PASSWORD")

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

if __name__ == '__main__':
    # Em produção, este script será executado pelo Docker/CapRover.
    # Para testes locais, pode usar o waitress:
    # waitress-serve --host 127.0.0.1 --port=5000 webhook_server:app
    app.run(port=5000, debug=True)
