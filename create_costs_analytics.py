# create_costs_analytics.py

import pandas as pd
from db import read_data_from_db
from log_db import read_log_data, write_dataframe_to_db
from query import query_sai_costs_daily, query_tracking_link_costs_daily, query_nps_costs_daily
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Define o nome da tabela de análise e o custo por mensagem
ANALYTICS_TABLE_NAME = 'whatsapp_costs_daily'
COST_PER_MESSAGE = 0.046

def run_costs_etl():
    """
    Executa o processo de ETL para consolidar os dados de custos de todas as
    aplicações que enviam mensagens via WhatsApp.
    """
    print("\n--- INICIANDO ETL DE CUSTOS DO WHATSAPP ---")

    # 1. EXTRAÇÃO: Busca os dados de cada fonte
    print("ETAPA 1: Extraindo dados de envio...")
    sai_df = read_log_data(query_sai_costs_daily())
    tracking_df = read_data_from_db(query_tracking_link_costs_daily())
    nps_df = read_data_from_db(query_nps_costs_daily())
    
    print(f"INFO: Encontrados {len(sai_df) if sai_df is not None else 0} registros do SAI.")
    print(f"INFO: Encontrados {len(tracking_df) if tracking_df is not None else 0} registros de Links de Rastreio.")
    print(f"INFO: Encontrados {len(nps_df) if nps_df is not None else 0} registros de NPS.")

    # 2. TRANSFORMAÇÃO: Junta todos os dados em um único DataFrame
    print("ETAPA 2: Consolidando e transformando os dados...")
    all_costs_df = pd.concat([sai_df, tracking_df, nps_df], ignore_index=True)

    if all_costs_df.empty:
        print("INFO: Nenhum dado de envio encontrado para processar. A tabela será apenas limpa.")
    else:
        # Se houver dados, calcula o custo e formata a data
        all_costs_df['estimated_cost'] = all_costs_df['message_count'] * COST_PER_MESSAGE
        all_costs_df['event_date'] = pd.to_datetime(all_costs_df['event_date'])

    # 3. CARGA: Escreve o resultado na tabela de análise
    print(f"ETAPA 3: Carregando os dados na tabela '{ANALYTICS_TABLE_NAME}'...")
    
    load_dotenv()
    host = os.getenv('LOG_DB_HOST')
    user = os.getenv('LOG_DB_USER')
    password = os.getenv('LOG_DB_PASSWORD')
    port = int(os.getenv('LOG_DB_PORT'))
    db_name = os.getenv('LOG_DB_NAME')
    
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")
        
        # Lógica robusta que assume que a tabela já existe
        with engine.connect() as connection:
            print(f"INFO: Limpando a tabela '{ANALYTICS_TABLE_NAME}' antes da inserção...")
            connection.execute(text(f"TRUNCATE TABLE {ANALYTICS_TABLE_NAME}"))
            connection.commit()
            print("INFO: Tabela pronta para receber novos dados.")

        # Se houver dados para inserir, chama a função de escrita que anexa os dados
        if not all_costs_df.empty:
            write_dataframe_to_db(all_costs_df, ANALYTICS_TABLE_NAME)

    except Exception as e:
        print(f"ERRO: Falha ao carregar dados na tabela de custos: {e}")
        return

    print("\n--- PROCESSO DE ETL DE CUSTOS CONCLUÍDO COM SUCESSO! ---")

if __name__ == "__main__":
    try:
        import sqlalchemy
    except ImportError:
        print("Instalando a dependência 'SQLAlchemy' necessária...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "SQLAlchemy"])
    
    run_costs_etl()
