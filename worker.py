# worker.py

import time
from datetime import datetime
from croniter import croniter
from main import execute_sai_logic # Importamos a função com a lógica principal

# Expressão de cron para agendar a tarefa a cada 5 minutos.
# Use o site crontab.guru para criar a sua própria expressão.
CRON_EXPRESSION = "*/5 * * * *" 

def main():
    """
    Loop para executar o cronjob indefinidamente.
    """
    print(f"INFO: Worker iniciado. A aguardar o próximo agendamento baseado em '{CRON_EXPRESSION}'...")
    base_time = datetime.now()
    
    while True:
        # Calcula o próximo horário de execução
        scheduler = croniter(CRON_EXPRESSION, base_time)
        next_schedule = scheduler.get_next(datetime)
        
        # Calcula o tempo de espera e dorme
        wait_time = (next_schedule - datetime.now()).total_seconds()
        if wait_time > 0:
            print(f"Próxima execução em: {next_schedule.strftime('%Y-%m-%d %H:%M:%S')}. A aguardar por {int(wait_time)} segundos...")
            time.sleep(wait_time)

        # Executa a tarefa
        print(f"\n--- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - A EXECUTAR TAREFA AGENDADA ---")
        try:
            execute_sai_logic()
        except Exception as e:
            print(f"ERRO: Ocorreu um erro ao executar a tarefa: {e}")
        print("--- TAREFA CONCLUÍDA. A preparar o próximo agendamento. ---")
        
        # Atualiza o tempo base para o próximo cálculo
        base_time = datetime.now()

if __name__ == "__main__":
    main()