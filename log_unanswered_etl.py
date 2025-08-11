# log_unanswered_etl.py

from log_db import read_log_data, log_sai_event
from query import query_unanswered_offers_to_log

def run_log_unanswered_etl():
    """
    Executa o processo de ETL para encontrar e registrar ofertas que foram
    enviadas mas nunca obtiveram resposta.
    """
    print("\n--- INICIANDO ETL PARA REGISTRAR OFERTAS NÃO RESPONDIDAS ---")
    
    # Busca no banco de dados todas as ofertas que se encaixam nos critérios
    unanswered_df = read_log_data(query_unanswered_offers_to_log())

    if unanswered_df is None or unanswered_df.empty:
        print("INFO: Nenhuma nova oferta não respondida para registrar. Processo concluído.")
        return

    print(f"INFO: Encontradas {len(unanswered_df)} novas ofertas não respondidas para registrar.")

    # Itera sobre cada oferta encontrada e registra um novo evento
    for index, row in unanswered_df.iterrows():
        log_sai_event(
            order_id=int(row['order_id']),
            provider_id=int(row['provider_id']),
            event_type='UNANSWERED_OFFER',
            metadata=None # Não precisamos mais de metadados específicos aqui
        )
    
    print(f"--- ETL DE OFERTAS NÃO RESPONDIDAS CONCLUÍDO: {len(unanswered_df)} eventos registrados. ---")

if __name__ == "__main__":
    # Permite que o script seja executado manualmente para o backfill inicial
    run_log_unanswered_etl()
