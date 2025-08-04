# create_sent_offers_analytics.py

import pandas as pd
from db import read_data_from_db
from log_db import read_log_data, write_dataframe_to_db
from query import query_sent_offers_log, query_order_details_by_ids
from sqlalchemy import create_engine, text # <-- Adicionar importação de 'text'
from dotenv import load_dotenv
import os

# Define o nome da NOVA tabela que será criada no banco de dados de BI
ANALYTICS_TABLE_NAME = 'sai_sent_offers_analytics'

def run_sent_offers_etl():
    """
    Executa o processo de ETL para consolidar os dados de ofertas ENVIADAS
    com os detalhes das corridas para análise.
    """
    print("\n--- INICIANDO PROCESSO DE ETL PARA ANÁLISE DE OFERTAS ENVIADAS ---")

    # 1. EXTRAÇÃO: Buscar os logs de ofertas ENVIADAS
    print("ETAPA 1: Extraindo logs de ofertas ENVIADAS do banco de BI...")
    sent_offers_df = read_log_data(query_sent_offers_log())

    if sent_offers_df is None or sent_offers_df.empty:
        print("INFO: Nenhuma oferta enviada encontrada nos logs para processar. Finalizando ETL.")
        return

    print(f"INFO: {len(sent_offers_df)} ofertas enviadas encontradas.")
    
    order_ids_to_check = sent_offers_df['order_id'].unique().tolist()

    # 2. EXTRAÇÃO: Buscar os detalhes das ordens no banco de produção
    print(f"ETAPA 2: Buscando detalhes de {len(order_ids_to_check)} corridas no banco de produção...")
    order_details_df = read_data_from_db(query_order_details_by_ids(order_ids_to_check))

    if order_details_df is None or order_details_df.empty:
        print("ERRO: Não foi possível buscar os detalhes das corridas. Abortando ETL.")
        return
    
    print(f"INFO: {len(order_details_df)} detalhes de corridas encontrados.")

    # 3. TRANSFORMAÇÃO: Juntar os DataFrames
    print("ETAPA 3: Transformando e juntando os dados...")
    sent_offers_df['order_id'] = sent_offers_df['order_id'].astype(int)
    order_details_df['order_id'] = order_details_df['order_id'].astype(int)

    analytics_df = pd.merge(sent_offers_df, order_details_df, on='order_id', how='left')

    # 4. CARGA: Escrever o resultado na nova tabela de análise
    print(f"ETAPA 4: Carregando os dados na tabela '{ANALYTICS_TABLE_NAME}'...")
    
    load_dotenv()
    host = os.getenv('LOG_DB_HOST')
    user = os.getenv('LOG_DB_USER')
    password = os.getenv('LOG_DB_PASSWORD')
    port = int(os.getenv('LOG_DB_PORT'))
    db_name = os.getenv('LOG_DB_NAME')
    
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")
        with engine.connect() as connection:
            print(f"INFO: Limpando a tabela '{ANALYTICS_TABLE_NAME}' antes da inserção...")
            # --- CORREÇÃO AQUI ---
            connection.execute(text(f"TRUNCATE TABLE {ANALYTICS_TABLE_NAME}"))
            connection.commit() # Adicionado commit para garantir a execução do TRUNCATE
            # ---------------------
            print("INFO: Tabela limpa com sucesso.")
        
        write_dataframe_to_db(analytics_df, ANALYTICS_TABLE_NAME)
    except Exception as e:
        print(f"ERRO: Falha ao limpar ou carregar dados na tabela de análise: {e}")
        return

    print("\n--- PROCESSO DE ETL DE OFERTAS ENVIADAS CONCLUÍDO COM SUCESSO! ---")

if __name__ == "__main__":
    try:
        import sqlalchemy
    except ImportError:
        print("Instalando a dependência 'SQLAlchemy' necessária...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "SQLAlchemy"])
    
    run_sent_offers_etl()
