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
    pass

@app.route('/webhook', methods=['POST'])
def receive_message():
    """
    Recebe a resposta do provedor e usa IDs de teste para atribuir a ordem.
    """
    data = request.json
    print("\n" + "="*50)
    print(">>> DADOS JSON RECEBIDOS DO CHATGURU <<<")
    print(data)
    
    response_text = data.get('texto_mensagem', '').strip()

    # --- MUDANÇA IMPORTANTE PARA O TESTE ---
    # Ignoramos os dados do Chatguru e usamos os seus IDs de teste fixos.
    order_id = 162333
    provider_id = 5210
    print("\n--- USANDO DADOS DE TESTE FIXOS ---")
    print(f"Order ID Fixo: {order_id}")
    print(f"Provider ID Fixo: {provider_id}")
    print("---------------------------------")
    
    if response_text.lower() == 'aceitar corrida':
        print(f"INFO: Provedor {provider_id} ACEITOU a ordem {order_id}.")
        
        access_token = login(GIROSS_EMAIL, GIROSS_PASSWORD)
        
        if access_token:
            # Convertemos para int, pois a API espera números.
            assign_order(access_token, int(provider_id), int(order_id))
        else:
            print("ERRO: Não foi possível atribuir a ordem devido a falha no login.")
            
    else:
        find_next_provider_and_send_offer(order_id, provider_id)
        
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(port=5000, debug=True)