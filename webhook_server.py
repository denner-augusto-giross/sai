# webhook_server.py

import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from internal_api import login, assign_order

load_dotenv()
app = Flask(__name__)

GIROSS_EMAIL = os.getenv("GIROSS_EMAIL")
GIROSS_PASSWORD = os.getenv("GIROSS_PASSWORD")

def find_next_provider_and_send_offer(order_id, rejected_provider_id):
    """Placeholder para a lógica de encontrar o próximo melhor provedor."""
    print(f"\nAVISO: O provedor {rejected_provider_id} rejeitou a ordem {order_id}.")
    print("A lógica para encontrar o próximo provedor precisa ser implementada aqui.")
    # Este será o seu próximo grande desafio de desenvolvimento!
    pass

@app.route('/webhook', methods=['POST'])
def receive_message():
    """
    Recebe a resposta do provedor e usa os dados reais do webhook para atribuir a ordem.
    """
    data = request.json
    print("\n" + "="*50)
    print(">>> DADOS JSON RECEBIDOS DO CHATGURU <<<")
    print(data)
    
    # --- LÓGICA DE PRODUÇÃO AQUI ---
    bot_context = data.get('bot_context', {})
    response_status = bot_context.get('Status')
    custom_fields = data.get('campos_personalizados', {})
    
    order_id = custom_fields.get('order_id')
    provider_id = custom_fields.get('provider_id')

    if not order_id or not provider_id:
        print("ERRO: 'order_id' ou 'provider_id' não encontrados nos campos personalizados do webhook.")
        return jsonify({"status": "error", "message": "Missing required custom fields"}), 400

    # Verifica se a resposta foi positiva
    if response_status == 'Resposta_sim':
        print(f"INFO: Provedor {provider_id} ACEITOU a ordem {order_id}.")
        
        access_token = login(GIROSS_EMAIL, GIROSS_PASSWORD)
        
        if access_token:
            assign_order(access_token, int(provider_id), int(order_id))
        else:
            print("ERRO: Não foi possível atribuir a ordem devido a falha no login.")
            
    else:
        # Se a resposta for "Resposta_nao" ou qualquer outra coisa, aciona a lógica de rejeição.
        find_next_provider_and_send_offer(order_id, provider_id)
        
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    # Em produção, este script será executado pelo Docker/CapRover.
    # Para testes locais, pode usar o waitress:
    # waitress-serve --host 127.0.0.1 --port=5000 webhook_server:app
    app.run(port=5000, debug=True)