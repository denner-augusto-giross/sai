# email_checker.py

import os
import imaplib
import email
from email.header import decode_header
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

# --- Configurações Lidas do .env ---
IMAP_SERVER = os.getenv("EMAIL_IMAP_SERVER")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TEMPLATE_NAME = os.getenv("SAI_TEMPLATE_NAME")
# ------------------------------------

def get_body(msg):
    """Extrai o corpo do e-mail, decodificando se necessário."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    body = part.get_payload(decode=True)
                    return body.decode()
                except Exception:
                    continue
    else:
        try:
            body = msg.get_payload(decode=True)
            return body.decode()
        except Exception:
            return ""

def check_for_category_change_email():
    """
    Conecta-se à caixa de e-mail, procura por e-mails de alerta da Meta
    e verifica se algum corresponde ao template do SAI.
    Retorna True se um alerta for encontrado, False caso contrário.
    """
    if not all([IMAP_SERVER, EMAIL_ADDRESS, EMAIL_PASSWORD, TEMPLATE_NAME]):
        print("ERRO DE VERIFICAÇÃO DE E-MAIL: Verifique se as variáveis de e-mail e o nome do template estão no .env.")
        return False

    try:
        print("INFO: Conectando ao servidor de e-mail...")
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        
        mail.select("inbox")
        
        # Busca por e-mails não lidos do remetente específico
        status, messages = mail.search(None, '(UNSEEN FROM "reminders@facebookmail.com")')
        
        if status != "OK" or not messages[0]:
            print("INFO: Nenhum e-mail de alerta não lido encontrado.")
            mail.logout()
            return False

        email_ids = messages[0].split()
        print(f"INFO: Encontrados {len(email_ids)} e-mails não lidos do remetente.")

        for email_id in email_ids:
            status, msg_data = mail.fetch(email_id, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    
                    # Decodifica o assunto do e-mail
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding if encoding else "utf-8")

                    # Pega o corpo do e-mail
                    body = get_body(msg)
                    
                    # --- Lógica de Verificação ---
                    expected_subject_part = "A categoria de"
                    
                    print(f"DEBUG: Verificando E-mail ID {email_id.decode()} | Assunto: {subject}")

                    if expected_subject_part in subject and TEMPLATE_NAME in body:
                        print("\n" + "="*50)
                        print("ALERTA CRÍTICO: E-mail de mudança de categoria encontrado para o template do SAI!")
                        print(f"Assunto: {subject}")
                        print("="*50 + "\n")
                        mail.logout()
                        return True # Alerta encontrado!

        mail.logout()
        print("INFO: Verificação de e-mails concluída. Nenhum alerta relevante encontrado.")
        return False

    except Exception as e:
        print(f"ERRO CRÍTICO ao verificar e-mails: {e}")
        # Em caso de erro na verificação, retornamos False para não parar o sistema por engano.
        return False

if __name__ == "__main__":
    # Permite testar o script diretamente
    check_for_category_change_email()
