# worker.py

import time
from datetime import datetime, timedelta
from croniter import croniter
from main import execute_sai_logic
from analytics_etl import run_analytics_etl # <-- Nova importação

# Expressão de cron para agendar a tarefa principal a cada 5 minutos.
CRON_EXPRESSION = "*/5 * * * *" 

# Variável para controlar a última vez que o ETL de análise foi executado
last_etl_run_time = None

def main():
    """
    Loop para executar o cronjob indefinidamente, com um gatilho diário para o ETL.
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
        # Verifica se o ETL nunca foi executado ou se já se passaram mais de 24 horas
        if last_etl_run_time is None or (now - last_etl_run_time) > timedelta(hours=24):
            print(f"\n--- {now.strftime('%Y-%m-%d %H:%M:%S')} - INICIANDO TAREFA DE ETL DE ANÁLISE ---")
            try:
                run_analytics_etl()
                last_etl_run_time = now # Atualiza o timestamp da última execução bem-sucedida
                print("--- TAREFA DE ETL DE ANÁLISE CONCLUÍDA. ---")
            except Exception as e:
                print(f"ERRO: Ocorreu um erro ao executar a tarefa de ETL: {e}")
        
        base_time = datetime.now()

if __name__ == "__main__":
    main()

