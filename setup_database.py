# setup_database.py

import os
import pymysql
from dotenv import load_dotenv

def setup_analytics_tables():
    """
    Conecta-se ao banco de dados de desenvolvimento e cria as tabelas
    necessárias para o rastreamento e análise do SAI.
    """
    load_dotenv()
    
    # Carrega as credenciais específicas para o banco de dados de log
    host = os.getenv('LOG_DB_HOST')
    user = os.getenv('LOG_DB_USER')
    password = os.getenv('LOG_DB_PASSWORD')
    port = int(os.getenv('LOG_DB_PORT'))
    db_name = os.getenv('LOG_DB_NAME')

    if not all([host, user, password, port, db_name]):
        print("ERRO: Verifique se as variáveis LOG_DB_* estão definidas no seu ficheiro .env.")
        return

    db_connection = None
    cursor = None
    try:
        # Conecta-se diretamente ao banco de dados de desenvolvimento
        print(f"INFO: A conectar-se ao banco de dados '{db_name}'...")
        db_connection = pymysql.connect(
            host=host, user=user, password=password, database=db_name,
            port=int(port), connect_timeout=15
        )
        cursor = db_connection.cursor()
        
        # --- Definição da Tabela de Eventos ---
        create_log_table_query = """
        CREATE TABLE IF NOT EXISTS sai_event_log (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT NOT NULL,
            provider_id INT NOT NULL,
            event_type VARCHAR(50) NOT NULL COMMENT 'Ex: OFFER_SENT, PROVIDER_ACCEPTED, ASSIGNMENT_SUCCESS',
            event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metadata JSON COMMENT 'Para guardar dados extras, como distância, score, etc.',
            INDEX (order_id),
            INDEX (event_type)
        );
        """
        
        print("INFO: A executar o comando para criar a tabela 'sai_event_log'...")
        cursor.execute(create_log_table_query)
        db_connection.commit()
        
        print("\n==========================================================")
        print("SUCESSO: Tabela 'sai_event_log' pronta para uso!")
        print("==========================================================")

    except pymysql.Error as err:
        print(f"\nERRO: Ocorreu um erro com o PyMySQL: {err}")

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
            print("INFO: Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    setup_analytics_tables()
