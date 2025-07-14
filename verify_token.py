# verify_token.py

import os
from dotenv import load_dotenv

# Carrega as variáveis do seu ficheiro .env
load_dotenv()

print("="*50)
print("A VERIFICAR O ACCESS TOKEN DO FICHEIRO .ENV")
print("="*50)

# Obtém o token do ambiente
access_token = os.getenv("ACCESS_TOKEN_GRAPH")

if access_token:
    print("\n[INÍCIO DO TOKEN]")
    print(access_token)
    print("[FIM DO TOKEN]")
    
    print(f"\nComprimento do Token: {len(access_token)} caracteres")
    
    if " " in access_token:
        print("\nALERTA: Foram detectados espaços no seu token! Isto é um problema comum.")
    else:
        print("\nINFO: Não foram detectados espaços no token.")
else:
    print("\nERRO: Não foi possível encontrar a variável ACCESS_TOKEN no seu ficheiro .env.")

print("\nInstrução: Copie o token impresso entre [INÍCIO DO TOKEN] e [FIM DO TOKEN] e compare-o cuidadosamente com o token no site da Meta.")
print("="*50)