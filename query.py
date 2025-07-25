import pymysql
def query_stuck_orders(city_ids: list):
    """
    Retorna uma query SQL que encontra as corridas travadas para uma
    lista específica de cidades.
    """
    # Converte a lista de IDs de Python para uma string formatada para o SQL (ex: "(50, 193, 162)")
    city_ids_str = f"({', '.join(map(str, city_ids))})"

    return f"""
        WITH
        base_latitude as (
            SELECT
                ua.user_id,
                ua.value as latitude
            FROM giross_producao.user_attributes ua
            INNER JOIN (SELECT user_id, MAX(id) as id FROM giross_producao.user_attributes WHERE attribute_id = 2 GROUP BY 1) ua1
                ON ua.id = ua1.id
            WHERE ua.attribute_id = 2
        ),
        base_longitude as (
            SELECT
                ua.user_id,
                ua.value as longitude
            FROM giross_producao.user_attributes ua
            INNER JOIN (SELECT user_id, MAX(id) as id FROM giross_producao.user_attributes WHERE attribute_id = 3 GROUP BY 1) ua1
                ON ua.id = ua1.id
            WHERE ua.attribute_id = 3
        ),
        user_requests_ AS (
            SELECT
                ur.id,
                ur.user_id,
                ur.provider_id,
                c.id AS city_id,
                blat.latitude AS store_latitude,
                blon.longitude AS store_longitude,
                ur.distance as store_to_delivery_distance,
                
                CONCAT('💰 Valor da Corrida: R$ ', FORMAT(
                    ROUND(ur.estimated_total * (1 - (COALESCE(prl.percent, 20) / 100)), 2),
                    2,
                    'de_DE'
                )) AS param1_valor,

                CONCAT('📍 Endereço de Coleta: ', 
                    REGEXP_REPLACE(
                        TRIM(REPLACE(REPLACE(CONCAT(u.first_name, ' ', u.last_name), 'Integração ', ''), 'Integracao', '')),
                        ' - [A-Z ]+ - [A-Z]{{2}}$', ''
                    ),
                    ' - ',
                    ur.s_address
                ) AS param2_endereco

            FROM
                giross_producao.user_requests ur
                LEFT JOIN giross_producao.user_request_delay_rules urdr ON urdr.type = 'USER'
                    AND ur.user_id = urdr.value
                    AND ur.original_created_at >= urdr.created_at
                LEFT JOIN giross_producao.percentage_receipt_location prl ON prl.location_type = 'USER'
                    AND ur.user_id = prl.value
                    AND ur.original_created_at BETWEEN prl.created_at AND COALESCE(prl.deleted_at, NOW())
                LEFT JOIN giross_producao.cities c ON ur.city_id = c.id
                LEFT JOIN giross_producao.users u ON ur.user_id = u.id
                LEFT JOIN base_latitude blat ON ur.user_id = blat.user_id
                LEFT JOIN base_longitude blon ON ur.user_id = blon.user_id
            WHERE
                CASE
                    WHEN ur.scheduled_cod IS NULL THEN TIMESTAMPDIFF(MINUTE, ur.original_created_at, NOW())
                    ELSE TIMESTAMPDIFF(MINUTE, ur.started_at, NOW())
                END >= 3
                AND ur.status = 'SEARCHING'
                AND ur.provider_id IN (0, 1266)
                -- ===================================
                -- ALTERAÇÃO PARA MÚLTIPLAS CIDADES AQUI
                AND ur.city_id IN {city_ids_str}
                -- ===================================
                AND (ur.integration_service NOT LIKE '%d+1%' OR ur.integration_service IS NULL)
                AND (ur.integration_service NOT LIKE '%mercado livre%' OR ur.integration_service IS NULL)
                AND DATE(
                    COALESCE(ur.original_created_at, ur.started_at)
                ) >= CURDATE() - INTERVAL 7 DAY
        )
        SELECT
            id AS order_id,
            user_id,
            city_id,
            store_latitude,
            store_longitude,
            store_to_delivery_distance,
            param1_valor,
            param2_endereco
        FROM
            user_requests_;
    """

def query_available_providers():
    return f"""
        WITH
        provider_releases AS (
            SELECT
                provider_id,
                COUNT(id) AS total_releases
            FROM
                giross_producao.provider_cancelled_user_requests
            GROUP BY
                1
        )
        SELECT
            p.id AS provider_id,
            CONCAT(p.first_name, ' ', p.last_name) AS provider_name,
            p.mobile,
            ps.status AS online_status,
            p.latitude,
            p.longitude,
            score.score,
            COALESCE(pr.total_releases, 0) AS total_releases_last_2_weeks
        FROM
            giross_producao.providers p
            INNER JOIN giross_producao.provider_services ps ON p.id = ps.provider_id AND ps.status IN ('active')
            LEFT JOIN giross_producao.provider_scores score ON p.id = score.provider_id
            LEFT JOIN provider_releases pr ON p.id = pr.provider_id
        ORDER BY
            total_releases_last_2_weeks ASC,
            score DESC;
    """

def query_blocked_pairs():
    """
    Retorna uma query SQL que busca todos os pares de user_id e provider_id
    que estão na tabela de bloqueios.
    """
    return "SELECT user_id, provider_id FROM giross_producao.user_provider_blocks"

#def verificar_execucao_ausente():
#    try:
#        conn = pymysql.connect(**db_config)
#        cursor = conn.cursor()
#        query = """
#        CREATE TABLE IF NOT EXISTS sai_tracking_log_TEST (
#            id INT AUTO_INCREMENT PRIMARY KEY,
#            order_id INT NOT NULL,
#            provider_id INT NOT NULL,
#            event_type VARCHAR(50) NOT NULL,
#            event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#            metadata JSON,
#            INDEX (order_id),
#            INDEX (event_type)
#        )
#        """
#        cursor.execute(query)
#        resultado = cursor.fetchone()
#        return resultado[0] == 0
#    except pymysql.Error as err:
#        print(f"[verificar_execucao_ausente] Erro: {err}")
#        return False
#    finally:
#        cursor.close()
#        conn.close()

def query_offers_sent():
    """
    Retorna uma query SQL que busca todos os pares de order_id e provider_id
    para os quais uma oferta já foi enviada, a partir da tabela de logs.
    """
    return """
        SELECT DISTINCT
            order_id,
            provider_id
        FROM
            desenvolvimento_bi.sai_event_log
        WHERE
            event_type = 'OFFER_SENT'
    """
def query_offline_providers_with_history(user_ids: list):
    """
    Retorna uma query SQL que encontra entregadores OFFLINE que completaram
    entregas para uma lista de lojas (user_ids) nos últimos 7 dias.
    Query corrigida com base na análise do DBeaver.
    """
    if not user_ids:
        # Se a lista de lojas estiver vazia, retorna uma query que não trará resultados.
        return "SELECT provider_id FROM giross_producao.providers WHERE 1=0;"

    user_ids_str = f"({', '.join(map(str, user_ids))})"

    return f"""
        -- Etapa 1: Encontrar entregadores que já completaram entregas para as lojas de interesse.
        WITH providers_with_history AS (
            SELECT DISTINCT
                ur.provider_id
            FROM
                giross_producao.user_requests ur
            WHERE
                -- Filtra apenas pelas lojas que têm corridas travadas AGORA.
                ur.user_id IN {user_ids_str}
                -- Condição principal: Apenas corridas concluídas com sucesso ('COMPLETED').
                AND ur.status = 'COMPLETED'
                -- Filtra pelo histórico dos últimos 7 dias.
                AND ur.original_created_at >= NOW() - INTERVAL 31 DAY
                AND ur.provider_id IS NOT NULL AND ur.provider_id > 0
        ),
        -- Etapa 2: Obter os releases dos últimos 14 dias para cálculo de score.
        provider_releases AS (
            SELECT
                provider_id,
                COUNT(id) AS total_releases
            FROM
                giross_producao.provider_cancelled_user_requests
            WHERE
                created_at >= NOW() - INTERVAL 14 DAY
            GROUP BY
                1
        )
        -- Etapa Final: Buscar as informações completas dos entregadores encontrados.
        SELECT
            p.id AS provider_id,
            CONCAT(p.first_name, ' ', p.last_name) AS provider_name,
            p.mobile,
            ps.status AS online_status,
            p.latitude,
            p.longitude,
            score.score,
            COALESCE(pr.total_releases, 0) AS total_releases_last_2_weeks
        FROM
            giross_producao.providers p
            -- Junta com a lista de entregadores relevantes que encontramos na Etapa 1.
            INNER JOIN providers_with_history ph ON p.id = ph.provider_id
            -- Junta para buscar o status de serviço atual.
            INNER JOIN giross_producao.provider_services ps ON p.id = ps.provider_id
            LEFT JOIN giross_producao.provider_scores score ON p.id = score.provider_id
            LEFT JOIN provider_releases pr ON p.id = pr.provider_id
        WHERE
            -- Filtro final: Garante que estamos trazendo apenas os que estão offline ou inativos.
            ps.status IN ('inactive', 'offline')
        ORDER BY
            score DESC;
    """
