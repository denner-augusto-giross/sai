# log_db.py

import os
import pymysql
import json
import pandas as pd
from dotenv import load_dotenv

def log_sai_event(order_id: int, provider_id: int, event_type: str, metadata: dict = None):
    """
    Conecta-se ao banco de dados de desenvolvimento e insere um novo
    registro de evento na tabela 'sai_event_log'.
    """
    load_dotenv()
    
    # Carrega as credenciais específicas para o banco de dados de log
    host = os.getenv('LOG_DB_HOST')
    user = os.getenv('LOG_DB_USER')
    password = os.getenv('LOG_DB_PASSWORD')
    port = int(os.getenv('LOG_DB_PORT'))
    db_name = os.getenv('LOG_DB_NAME')

    if not all([host, user, password, port, db_name]):
        print("ERRO DE LOG: Verifique se as variáveis LOG_DB_* estão definidas no seu .env.")
        return

    db_connection = None
    cursor = None
    try:
        db_connection = pymysql.connect(
            host=host, user=user, password=password, database=db_name,
            port=int(port), connect_timeout=15
        )
        cursor = db_connection.cursor()
        
        # Converte o dicionário de metadados para uma string JSON
        metadata_json = json.dumps(metadata) if metadata else None
        
        query = """
            INSERT INTO sai_event_log (order_id, provider_id, event_type, metadata)
            VALUES (%s, %s, %s, %s)
        """
        
        cursor.execute(query, (order_id, provider_id, event_type, metadata_json))
        db_connection.commit()
        
        print(f"LOG: Evento '{event_type}' para a ordem {order_id} registrado com sucesso.")

    except pymysql.Error as err:
        print(f"ERRO DE LOG: Falha ao registrar o evento '{event_type}': {err}")

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

def read_log_data(query: str):
    """
    Conecta-se ao banco de dados de log com as credenciais corretas e
    lê os dados usando pandas.
    """
    load_dotenv()
    
    host = os.getenv('LOG_DB_HOST')
    user = os.getenv('LOG_DB_USER')
    password = os.getenv('LOG_DB_PASSWORD')
    port = int(os.getenv('LOG_DB_PORT'))
    db_name = os.getenv('LOG_DB_NAME')

    if not all([host, user, password, port, db_name]):
        print("ERRO DE LEITURA DE LOG: Verifique se as variáveis LOG_DB_* estão definidas no seu .env.")
        return None

    db_connection = None
    try:
        db_connection = pymysql.connect(
            host=host, user=user, password=password, database=db_name,
            port=int(port), connect_timeout=15
        )
        
        result_df = pd.read_sql_query(query, db_connection)
        return result_df

    except pymysql.Error as err:
        print(f"\nERRO DE LEITURA DE LOG: Ocorreu um erro com o PyMySQL: {err}")
        return None
    
    finally:
        if db_connection:
            db_connection.close()