# analytics_etl.py

import pandas as pd
from db import read_data_from_db
from log_db import read_log_data, write_dataframe_to_db
from query import query_accepted_offers_log, query_order_details_by_ids
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Define o nome da tabela de análise no banco de dados de BI
ANALYTICS_TABLE_NAME = 'sai_performance_analytics'

def run_analytics_etl():
    """
    Executa o processo de ETL para consolidar os dados de ofertas aceitas
    com o status final das corridas, atualizando a tabela de análise.
    """
    print("\n--- INICIANDO PROCESSO DE ETL PARA ANÁLISE DE PERFORMANCE DO SAI ---")

    # 1. EXTRAÇÃO: Buscar os logs de ofertas aceitas
    print("ETAPA 1: Extraindo logs de ofertas aceitas do banco de BI...")
    accepted_offers_df = read_log_data(query_accepted_offers_log())

    if accepted_offers_df is None or accepted_offers_df.empty:
        print("INFO: Nenhuma nova oferta aceita encontrada nos logs para processar. Finalizando ETL.")
        return

    print(f"INFO: {len(accepted_offers_df)} ofertas aceitas encontradas.")
    
    order_ids_to_check = accepted_offers_df['order_id'].unique().tolist()

    # 2. EXTRAÇÃO: Buscar os detalhes e status finais das ordens
    print(f"ETAPA 2: Buscando detalhes de {len(order_ids_to_check)} corridas no banco de produção...")
    order_details_df = read_data_from_db(query_order_details_by_ids(order_ids_to_check))

    if order_details_df is None or order_details_df.empty:
        print("ERRO: Não foi possível buscar os detalhes das corridas. Abortando ETL.")
        return
    
    print(f"INFO: {len(order_details_df)} detalhes de corridas encontrados.")

    # 3. TRANSFORMAÇÃO: Juntar os DataFrames
    print("ETAPA 3: Transformando e juntando os dados...")
    accepted_offers_df['order_id'] = accepted_offers_df['order_id'].astype(int)
    order_details_df['order_id'] = order_details_df['order_id'].astype(int)

    performance_df = pd.merge(accepted_offers_df, order_details_df, on='order_id', how='left')

    # 4. CARGA: Escrever o resultado na tabela de análise
    print(f"ETAPA 4: Carregando os dados na tabela '{ANALYTICS_TABLE_NAME}'...")
    
    # TRUNCATE: Limpa a tabela antes de inserir os novos dados para evitar duplicatas
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
            connection.execute(f"TRUNCATE TABLE {ANALYTICS_TABLE_NAME}")
            print("INFO: Tabela limpa com sucesso.")
        
        write_dataframe_to_db(performance_df, ANALYTICS_TABLE_NAME)
    except Exception as e:
        print(f"ERRO: Falha ao limpar ou carregar dados na tabela de análise: {e}")
        return

    print("\n--- PROCESSO DE ETL CONCLUÍDO COM SUCESSO! ---")

if __name__ == "__main__":
    # Permite que o script seja executado manualmente para testes ou backfill inicial
    try:
        import sqlalchemy
    except ImportError:
        print("Instalando a dependência 'SQLAlchemy' necessária...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "SQLAlchemy"])
    
    run_analytics_etl()
