# verify_log_table.py

import os
import pymysql
from dotenv import load_dotenv
import pandas as pd

def check_table_existence():
    """
    Conecta-se ao banco de dados de log com as credenciais corretas e
    tenta executar uma consulta simples na tabela de log.
    """
    load_dotenv()
    
    # Carrega as credenciais específicas para o banco de dados de log
    host = os.getenv('LOG_DB_HOST')
    user = os.getenv('LOG_DB_USER')
    password = os.getenv('LOG_DB_PASSWORD')
    port = int(os.getenv('LOG_DB_PORT'))
    db_name = os.getenv('LOG_DB_NAME')

    if not all([host, user, password, port, db_name]):
        print("ERRO: Verifique se as variáveis LOG_DB_* estão definidas no seu .env.")
        return

    print(f"--- A iniciar verificação da tabela '{db_name}.sai_event_log' ---")
    
    db_connection = None
    try:
        # Conecta-se diretamente ao banco de dados de desenvolvimento usando pymysql
        db_connection = pymysql.connect(
            host=host, user=user, password=password, database=db_name,
            port=int(port), connect_timeout=15
        )
        
        query = f"SELECT COUNT(*) FROM {db_name}.sai_event_log"
        
        # Usa o pandas para ler o resultado da query
        result_df = pd.read_sql_query(query, db_connection)
        
        if result_df is not None:
            print("\n==========================================================")
            print(f"SUCESSO: A tabela '{db_name}.sai_event_log' foi encontrada e lida com sucesso!")
            print(f"Número de registros na tabela: {result_df.iloc[0,0]}")
            print("==========================================================")
        else:
            print("\n==========================================================")
            print("FALHA: A consulta não retornou resultados.")
            print("==========================================================")

    except pymysql.Error as err:
        print(f"\nERRO: Ocorreu um erro com o PyMySQL: {err}")
        print("CAUSA PROVÁVEL: O nome da tabela está incorreto ou o usuário não tem permissão de SELECT.")
    
    finally:
        if db_connection:
            db_connection.close()

if __name__ == "__main__":
    check_table_existence()
