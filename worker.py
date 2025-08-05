# worker.py

import time
from datetime import datetime, timedelta
from main import process_city_offers
from log_db import read_log_data, update_city_last_run
from query import query_sai_city_configs

def main():
    """
    Loop principal do worker. A cada minuto, verifica quais cidades
    precisam ser processadas com base em suas configurações.
    """
    print(f"--- WORKER DO SAI INICIADO ÀS {datetime.now()} ---")
    
    while True:
        print(f"\n--- {datetime.now()}: Verificando cidades para processar... ---")
        
        try:
            # 1. Busca as configurações de todas as cidades ativas
            city_configs_df = read_log_data(query_sai_city_configs())

            if city_configs_df is None or city_configs_df.empty:
                print("AVISO: Nenhuma configuração de cidade ativa encontrada. Tentando novamente em 1 minuto.")
                time.sleep(60)
                continue

            now = datetime.now()

            # 2. Itera sobre cada cidade e verifica se é hora de rodar
            for index, city_config in city_configs_df.iterrows():
                city_id = city_config['city_id']
                city_name = city_config['city_name']
                interval = timedelta(minutes=city_config['time_interval_minutes'])
                last_run = city_config['last_run_timestamp']

                # Verifica se já passou tempo suficiente desde a última execução
                if last_run is None or (now - last_run) >= interval:
                    print(f"\n>>> EXECUTANDO SAI PARA: {city_name} (ID: {city_id}) <<<")
                    
                    # 3. Chama a lógica principal para a cidade específica
                    process_city_offers(city_config=city_config.to_dict())
                    
                    # 4. Atualiza o timestamp da última execução no sucesso
                    update_city_last_run(city_id)
                    
                    print(f">>> FINALIZADO SAI PARA: {city_name} <<<")
                else:
                    print(f"INFO: Aguardando para {city_name}. Próxima execução após {(last_run + interval).strftime('%H:%M:%S')}.")

        except Exception as e:
            print(f"ERRO CRÍTICO NO LOOP DO WORKER: {e}")

        # Aguarda 60 segundos antes da próxima verificação
        print("\n--- Ciclo do worker concluído. Aguardando 60 segundos... ---")
        time.sleep(60)

if __name__ == "__main__":
    main()
