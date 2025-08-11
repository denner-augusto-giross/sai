# worker.py

import time
from datetime import datetime, timedelta
import pandas as pd
from main import process_city_offers
from log_db import read_log_data, update_city_last_run
from query import query_sai_city_configs
from analytics_etl import run_analytics_etl
from create_sent_offers_analytics import run_sent_offers_etl
from log_unanswered_etl import run_log_unanswered_etl

def main():
    """
    Loop principal do worker. A cada minuto, verifica quais cidades
    precisam ser processadas e executa tarefas de ETL diárias.
    """
    print(f"--- WORKER DO SAI INICIADO ÀS {datetime.now()} ---")
    
    last_etl_run_time = None
    
    while True:
        print(f"\n--- {datetime.now()}: Verificando ciclo de tarefas... ---")
        
        try:
            # --- LÓGICA DE OFERTAS POR CIDADE (a cada minuto) ---
            city_configs_df = read_log_data(query_sai_city_configs())

            if city_configs_df is None or city_configs_df.empty:
                print("AVISO: Nenhuma configuração de cidade ativa encontrada.")
            else:
                now = datetime.now()
                for index, city_config in city_configs_df.iterrows():
                    city_id = city_config['city_id']
                    city_name = city_config['city_name']
                    interval = timedelta(minutes=city_config['time_interval_minutes'])
                    last_run = city_config['last_run_timestamp']

                    should_run = pd.isna(last_run) or (now - last_run) >= interval
                    
                    if not should_run:
                        print(f"INFO: Aguardando para {city_name}. Próxima execução após {(last_run + interval).strftime('%H:%M:%S')}.")
                    else:
                        print(f"\n>>> EXECUTANDO SAI PARA: {city_name} (ID: {city_id}) <<<")
                        process_city_offers(city_config=city_config.to_dict())
                        update_city_last_run(city_id)
                        print(f">>> FINALIZADO SAI PARA: {city_name} <<<")

            # --- LÓGICA DO GATILHO DE ETL (uma vez por dia) ---
            now = datetime.now()
            if last_etl_run_time is None or (now - last_etl_run_time) > timedelta(hours=24):
                print(f"\n--- {now.strftime('%Y-%m-%d %H:%M:%S')} - INICIANDO TAREFAS DE ETL DIÁRIAS ---")
                
                try:
                    print("\n-> Executando ETL de Performance...")
                    run_analytics_etl()
                    print("-> ETL de Performance concluído.")
                except Exception as e:
                    print(f"ERRO no ETL de Performance: {e}")

                try:
                    print("\n-> Executando ETL de Ofertas Enviadas...")
                    run_sent_offers_etl()
                    print("-> ETL de Ofertas Enviadas concluído.")
                except Exception as e:
                    print(f"ERRO no ETL de Ofertas Enviadas: {e}")

                try:
                    print("\n-> Executando ETL de Ofertas Não Respondidas...")
                    run_log_unanswered_etl()
                    print("-> ETL de Ofertas Não Respondidas concluído.")
                except Exception as e:
                    print(f"ERRO no ETL de Ofertas Não Respondidas: {e}")

                last_etl_run_time = now
                print("--- TAREFAS DE ETL DIÁRIAS CONCLUÍDAS. ---")

        except Exception as e:
            print(f"ERRO CRÍTICO NO LOOP DO WORKER: {e}")

        print("\n--- Ciclo do worker concluído. Aguardando 60 segundos... ---")
        time.sleep(60)

if __name__ == "__main__":
    main()
