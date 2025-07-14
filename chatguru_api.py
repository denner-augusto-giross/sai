import requests

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
        """Função auxiliar para enviar requisições POST."""
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
            "action": "chat_add",
            "chat_number": chat_number,
            "name": user_name,
            #"text": "Iniciando contato"
        })
        return self._send_request(params)

    def update_custom_fields(self, chat_number, fields_to_update: dict):
        """
        Etapa 2: Atualizar campos personalizados para um chat.
        """
        params = self.base_params.copy()
        params.update({
            "action": "chat_update_custom_fields",
            "chat_number": chat_number,
        })
        
        # Adiciona o prefixo 'field__' a cada nome de campo, como a documentação sugere.
        for key, value in fields_to_update.items():
            params[f"field__{key}"] = value
            
        return self._send_request(params)

    def execute_dialog(self, chat_number, dialog_id):
        """Etapa 3: Executar um diálogo (enviar o template)."""
        params = self.base_params.copy()
        params.update({
            "action": "dialog_execute",
            "dialog_id": dialog_id,
            "chat_number": chat_number
        })
        return self._send_request(params)