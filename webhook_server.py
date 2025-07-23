# webhook_server.py

import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from internal_api import login, assign_order
from log_db import log_sai_event

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

    # --- NOVO LOG DE DIAGNÓSTICO AQUI ---
    print("\n" + "="*50)
    print(f">>> ROTA /webhook ACIONADA ÀS {datetime.now()} <<<")
    print(f"INFO: Método da Requisição: {request.method}")
    print(f"INFO: IP de Origem: {request.remote_addr}")
    print("="*50)
    # --- FIM DO LOG DE DIAGNÓSTICO ---
    
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

    # Converte para inteiros para o logging
    order_id_int = int(order_id)
    provider_id_int = int(provider_id)

    # --- LÓGICA DE LOGGING E DECISÃO ---
    if response_status == 'Resposta_sim':
        print(f"INFO: Provedor {provider_id} ACEITOU a ordem {order_id}.")
        log_sai_event(order_id_int, provider_id_int, 'PROVIDER_ACCEPTED')
        
        access_token = login(GIROSS_EMAIL, GIROSS_PASSWORD)
        
        if access_token:
            success = assign_order(access_token, provider_id_int, order_id_int)
            # Log do resultado da atribuição
            if success:
                log_sai_event(order_id_int, provider_id_int, 'ASSIGNMENT_SUCCESS')
            else:
                log_sai_event(order_id_int, provider_id_int, 'ASSIGNMENT_FAILURE')
        else:
            print("ERRO: Não foi possível atribuir a ordem devido a falha no login.")
            
    else:
        # Se a resposta for qualquer outra coisa, registra como rejeição
        log_sai_event(order_id_int, provider_id_int, 'PROVIDER_REJECTED')
        find_next_provider_and_send_offer(order_id, provider_id)
        
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    # Em produção, este script será executado pelo Docker/CapRover.
    # Para testes locais, pode usar o waitress:
    # waitress-serve --host 127.0.0.1 --port=5000 webhook_server:app
    app.run(port=5000, debug=True)