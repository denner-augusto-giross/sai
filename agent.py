# agent.py

import os
import requests # We will use the requests library directly
import json
from dotenv import load_dotenv

def generate_whatsapp_message(match_data: dict):
    """
    Uses the requests library to call the OpenAI API directly and generate
    a formatted WhatsApp message.
    """
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return "Error: OPENAI_API_KEY not found in .env file."

    # --- Direct API Call using requests ---

    # 1. Define the endpoint and headers
    api_url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    # The rest of the logic to prepare the prompt remains the same
    valor_corrida = match_data.get('value', 'N/A')
    distancia_ate_loja = f"{match_data.get('distance_km', 0):.2f} km"
    endereco_coleta = "[Endereço da Loja - A ser implementado com geocoding]"
    tempo_ate_loja = "[Tempo estimado até a loja - A ser calculado]"
    distancia_corrida_km = "[Distância total da corrida - A ser implementado]"
    eta_corrida_total = "[ETA total - A ser implementado]"

    prompt = f"""
        Você é um assistente de logística para uma empresa de entregas.
        Sua tarefa é preencher um modelo de mensagem do WhatsApp para oferecer uma nova corrida a um entregador.
        Use os dados fornecidos para preencher o modelo. Seja exato.

        **Modelo de Mensagem:**
        ---
        Oportunidade de Ouro para você!
        Olá! Encontramos uma corrida perfeita para você agora! 🛵💨

        💰 Valor da Corrida: R${{valor_corrida}}

        📍 Endereço de Coleta:
        {{endereco_coleta}}

        Sua Situação:
        - Distância até a coleta: {{distancia_ate_loja}}
        - Tempo estimado até a coleta: {{tempo_ate_loja}}

        Detalhes da Entrega:
        - Percurso total da corrida: {{distancia_corrida_km}}
        - Tempo estimado total (coleta + entrega): {{eta_corrida_total}}

        Aceita essa? Responda abaixo! 👇
        ---

        **Dados para esta Corrida:**
        - valor_corrida: {valor_corrida}
        - endereco_coleta: {endereco_coleta}
        - distancia_ate_loja: {distancia_ate_loja}
        - tempo_ate_loja: {tempo_ate_loja}
        - distancia_corrida_km: {distancia_corrida_km}
        - eta_corrida_total: {eta_corrida_total}

        Agora, gere a mensagem final preenchida.
    """

    # 2. Structure the data payload for the API
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "system", "content": "Você é um assistente de logística."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2
    }

    try:
        # 3. Make the POST request
        response = requests.post(api_url, headers=headers, data=json.dumps(data), timeout=20)
        
        # Check for a successful response
        if response.status_code == 200:
            response_json = response.json()
            return response_json['choices'][0]['message']['content'].strip()
        else:
            return f"Error from OpenAI API: Status {response.status_code} - {response.text}"

    except requests.exceptions.RequestException as e:
        # This will catch any network errors from the requests library
        return f"A network error occurred: {e}"