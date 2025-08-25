# chatguru_api.py

import requests
import json

class ChatguruWABA:
    """
    Classe para interagir com a API do Chatguru, seguindo o fluxo WABA.
    """
    def __init__(self, key, account_id, phone_id, url):
        self.base_url = url
        self.base_params = {
            "key": key,
            "account_id": account_id,
            "phone_id": phone_id
        }

    def _send_request(self, params):
        """Envia uma requisição POST para a API do Chatguru."""
        try:
            response = requests.post(self.base_url, data=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            return {"error": f"Erro HTTP: {http_err}", "details": response.text}
        except requests.exceptions.RequestException as e:
            return {"error": f"Erro de Requisição: {e}"}

    def register_chat(self, chat_number, user_name="Novo Lead"):
        """Etapa 1: Cadastrar um novo chat."""
        params = self.base_params.copy()
        params.update({
            "action": "chat_add", "chat_number": chat_number,
            "name": user_name, "text": "Alerta de corrida Giross próxima de você!"
        })
        return self._send_request(params)

    def update_custom_fields(self, chat_number, fields_to_update: dict):
        params = self.base_params.copy()
        params.update({
            "action": "chat_update_custom_fields",
            "chat_number": chat_number,
        })
        for key, value in fields_to_update.items():
            params[f"field__{key}"] = value
        return self._send_request(params)

    def check_chat_status(self, chat_add_id):
        """
        Verifica o status de um registro de chat pendente.
        """
        params = self.base_params.copy()
        params.update({
            "action": "chat_add_status",
            "chat_add_id": chat_add_id
        })
        return self._send_request(params)

    def execute_dialog(self, chat_number, dialog_id, template_params: dict):
        """
        Executa um diálogo e envia os parâmetros nomeados para o template.
        """
        params = self.base_params.copy()
        params.update({
            "action": "dialog_execute",
            "dialog_id": dialog_id,
            "chat_number": chat_number,
        })
        
        if isinstance(template_params, dict):
            for key, value in template_params.items():
                params[key] = value
            
        return self._send_request(params)

    def send_text_message(self, chat_number, message_text):
        """
        Envia uma mensagem de texto simples para um chat existente.
        """
        params = self.base_params.copy()
        params.update({
            "action": "chat_send_message",
            "chat_number": chat_number,
            "text": message_text
        })
        return self._send_request(params)
