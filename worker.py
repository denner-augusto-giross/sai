# worker.py

import time
from datetime import datetime, timedelta
import pandas as pd # Importar pandas para a verificação
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
            city_configs_df = read_log_data(query_sai_city_configs())

            if city_configs_df is None or city_configs_df.empty:
                print("AVISO: Nenhuma configuração de cidade ativa encontrada. Tentando novamente em 1 minuto.")
                time.sleep(60)
                continue

            now = datetime.now()

            for index, city_config in city_configs_df.iterrows():
                city_id = city_config['city_id']
                city_name = city_config['city_name']
                interval = timedelta(minutes=city_config['time_interval_minutes'])
                last_run = city_config['last_run_timestamp']

                # --- LÓGICA DE VERIFICAÇÃO CORRIGIDA E LOGS DE DIAGNÓSTICO ---
                # pd.isna() lida corretamente com None e NaT (Not a Time) do pandas
                should_run = pd.isna(last_run) or (now - last_run) >= interval
                
                # Log de diagnóstico para entender a decisão
                if not should_run:
                    print(f"INFO: Aguardando para {city_name}. Última execução: {last_run}. Próxima: {(last_run + interval).strftime('%H:%M:%S')}.")
                
                if should_run:
                    print(f"\n>>> EXECUTANDO SAI PARA: {city_name} (ID: {city_id}) <<<")
                    
                    process_city_offers(city_config=city_config.to_dict())
                    
                    update_city_last_run(city_id)
                    
                    print(f">>> FINALIZADO SAI PARA: {city_name} <<<")
                # --- FIM DA CORREÇÃO ---

        except Exception as e:
            print(f"ERRO CRÍTICO NO LOOP DO WORKER: {e}")

        print("\n--- Ciclo do worker concluído. Aguardando 60 segundos... ---")
        time.sleep(60)

if __name__ == "__main__":
    main()
