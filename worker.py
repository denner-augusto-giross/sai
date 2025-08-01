# worker.py

import time
from datetime import datetime, timedelta
from croniter import croniter
from main import execute_sai_logic
from analytics_etl import run_analytics_etl
from create_sent_offers_analytics import run_sent_offers_etl # <-- Nova importação

# Expressão de cron para agendar a tarefa principal a cada 5 minutos.
CRON_EXPRESSION = "*/5 * * * *" 

# Variável para controlar a última vez que os ETLs de análise foram executados
last_etl_run_time = None

def main():
    """
    Loop para executar o cronjob indefinidamente, com gatilhos diários para os ETLs.
    """
    global last_etl_run_time
    
    print(f"INFO: Worker iniciado. A aguardar o próximo agendamento baseado em '{CRON_EXPRESSION}'...")
    base_time = datetime.now()
    
    while True:
        scheduler = croniter(CRON_EXPRESSION, base_time)
        next_schedule = scheduler.get_next(datetime)
        
        wait_time = (next_schedule - datetime.now()).total_seconds()
        if wait_time > 0:
            print(f"Próxima execução da oferta em: {next_schedule.strftime('%Y-%m-%d %H:%M:%S')}. A aguardar por {int(wait_time)} segundos...")
            time.sleep(wait_time)

        # --- LÓGICA PRINCIPAL DE OFERTAS (executa a cada 5 minutos) ---
        print(f"\n--- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - EXECUTANDO TAREFA DE OFERTAS ---")
        try:
            execute_sai_logic()
        except Exception as e:
            print(f"ERRO: Ocorreu um erro ao executar a tarefa de ofertas: {e}")
        print("--- TAREFA DE OFERTAS CONCLUÍDA. ---")
        
        # --- LÓGICA DO GATILHO DE ETL (executa uma vez por dia) ---
        now = datetime.now()
        if last_etl_run_time is None or (now - last_etl_run_time) > timedelta(hours=24):
            print(f"\n--- {now.strftime('%Y-%m-%d %H:%M:%S')} - INICIANDO TAREFAS DE ETL DIÁRIAS ---")
            
            # Executa o ETL de Performance (Ofertas Aceitas vs. Completadas)
            try:
                print("\n-> Executando ETL de Performance...")
                run_analytics_etl()
                print("-> ETL de Performance concluído.")
            except Exception as e:
                print(f"ERRO no ETL de Performance: {e}")

            # Executa o novo ETL de Ofertas Enviadas
            try:
                print("\n-> Executando ETL de Ofertas Enviadas...")
                run_sent_offers_etl()
                print("-> ETL de Ofertas Enviadas concluído.")
            except Exception as e:
                print(f"ERRO no ETL de Ofertas Enviadas: {e}")

            last_etl_run_time = now # Atualiza o timestamp após a tentativa de ambos
            print("--- TAREFAS DE ETL DIÁRIAS CONCLUÍDAS. ---")
        
        base_time = datetime.now()

if __name__ == "__main__":
    main()
