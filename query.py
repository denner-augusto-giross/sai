def query_stuck_orders(city_id: int, stuck_threshold: int):
    """
    Retorna uma query SQL que encontra as corridas travadas para uma
    cidade específica, usando um limite de tempo dinâmico.
    """
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
                END >= {stuck_threshold}
                AND ur.status = 'SEARCHING'
                AND ur.provider_id IN (0, 1266)
                AND ur.city_id = {city_id}
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

def query_responsive_providers():
    """
    Retorna uma query SQL que busca todos os provider_id's únicos que já
    responderam (aceitaram ou rejeitaram) a uma oferta.
    """
    return """
        SELECT DISTINCT provider_id
        FROM desenvolvimento_bi.sai_event_log
        WHERE event_type IN ('PROVIDER_ACCEPTED', 'PROVIDER_REJECTED');
    """

def query_fixed_providers():
    """
    Retorna uma query SQL que busca os IDs de todos os provedores
    que são 'fixos' e não foram desvinculados (deleted_at IS NULL).
    """
    return """
        SELECT DISTINCT provider_id
        FROM giross_producao.provider_fixeds
        WHERE deleted_at IS NULL;
    """

def query_offline_providers_with_history(user_ids: list):
    """
    Retorna uma query SQL que encontra entregadores OFFLINE que completaram
    entregas para uma lista de lojas (user_ids) nos últimos 7 dias.
    """
    if not user_ids:
        return "SELECT provider_id FROM giross_producao.providers WHERE 1=0;"

    user_ids_str = f"({', '.join(map(str, user_ids))})"

    return f"""
        WITH providers_with_history AS (
            SELECT DISTINCT
                ur.provider_id
            FROM
                giross_producao.user_requests ur
            WHERE
                ur.user_id IN {user_ids_str}
                AND ur.status = 'COMPLETED'
                AND ur.original_created_at >= NOW() - INTERVAL 31 DAY
                AND ur.provider_id IS NOT NULL AND ur.provider_id > 0
        ),
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
            INNER JOIN providers_with_history ph ON p.id = ph.provider_id
            INNER JOIN giross_producao.provider_services ps ON p.id = ps.provider_id
            LEFT JOIN giross_producao.provider_scores score ON p.id = score.provider_id
            LEFT JOIN provider_releases pr ON p.id = pr.provider_id
        WHERE
            ps.status IN ('inactive', 'offline')
        ORDER BY
            score DESC;
    """

def query_order_status(order_id: int):
    """
    Retorna uma query SQL para verificar o provider_id atual de uma corrida.
    """
    return f"""
        SELECT provider_id 
        FROM giross_producao.user_requests 
        WHERE id = {order_id};
    """

def query_accepted_offers_log():
    """
    Retorna uma query que busca todos os eventos de 'PROVIDER_ACCEPTED' do log,
    incluindo o order_id, provider_id e o timestamp do aceite.
    """
    return """
        SELECT
            order_id,
            provider_id,
            event_timestamp AS accepted_at
        FROM
            desenvolvimento_bi.sai_event_log
        WHERE
            event_type = 'PROVIDER_ACCEPTED';
    """

def query_order_details_by_ids(order_ids: list):
    """
    Retorna uma query que busca detalhes e o status final de uma lista
    específica de order_ids do banco de produção.
    """
    if not order_ids:
        return "SELECT id FROM giross_producao.user_requests WHERE 1=0;"

    order_ids_str = f"({', '.join(map(str, order_ids))})"

    return f"""
        SELECT
            ur.id AS order_id,
            ur.status AS final_status,
            c.name AS city_name,
            ur.original_created_at AS created_at,
            ur.finished_at AS completed_at
        FROM
            giross_producao.user_requests ur
        LEFT JOIN
            giross_producao.cities c ON ur.city_id = c.id
        WHERE
            ur.id IN {order_ids_str};
    """
def query_sent_offers_log():
    """
    Retorna uma query que busca todos os eventos de 'OFFER_SENT' do log,
    incluindo o order_id, provider_id e o timestamp do envio.
    """
    return """
        SELECT
            order_id,
            provider_id,
            event_timestamp AS sent_at
        FROM
            desenvolvimento_bi.sai_event_log
        WHERE
            event_type = 'OFFER_SENT';
    """

def query_sai_city_configs():
    """
    Retorna uma query que busca todas as configurações ativas para as cidades
    na tabela de configuração do SAI.
    """
    return """
        SELECT
            city_id,
            city_name,
            time_interval_minutes,
            stuck_order_threshold_minutes,
            max_offers_per_order,
            offer_distance_km,
            max_unanswered_offers,
            unanswered_cooldown_hours,
            is_active,
            last_run_timestamp
        FROM
            desenvolvimento_bi.sai_city_configs
        WHERE
            is_active = TRUE;
    """

def query_providers_on_unanswered_cooldown(max_unanswered: int, cooldown_hours: int):
    """
    Retorna uma query que busca provedores que ignoraram mais ofertas
    consecutivas do que o permitido (`max_unanswered`).
    """
    return f"""
        WITH last_response AS (
            SELECT
                provider_id,
                MAX(event_timestamp) AS last_response_time
            FROM
                desenvolvimento_bi.sai_event_log
            WHERE
                event_type IN ('PROVIDER_ACCEPTED', 'PROVIDER_REJECTED')
            GROUP BY
                provider_id
        ),
        unanswered_offers AS (
            SELECT
                log.provider_id,
                COUNT(log.log_id) AS unanswered_count,
                MAX(log.event_timestamp) as last_offer_sent_time
            FROM
                desenvolvimento_bi.sai_event_log log
            LEFT JOIN
                last_response lr ON log.provider_id = lr.provider_id
            WHERE
                log.event_type = 'OFFER_SENT'
                AND (log.event_timestamp > lr.last_response_time OR lr.last_response_time IS NULL)
            GROUP BY
                log.provider_id
        )
        SELECT
            provider_id
        FROM
            unanswered_offers
        WHERE
            unanswered_count >= {max_unanswered}
            AND last_offer_sent_time >= NOW() - INTERVAL {cooldown_hours} HOUR;
    """

def query_unanswered_offers_to_log():
    """
    Retorna uma query que identifica pares (order_id, provider_id) de ofertas
    enviadas que não foram respondidas e para as quais ainda não foi gerado
    um log de 'UNANSWERED_OFFER'.
    """
    return """
        WITH sent_pairs AS (
            -- Seleciona todos os pares únicos (ordem, provedor) que receberam uma oferta
            SELECT DISTINCT order_id, provider_id
            FROM desenvolvimento_bi.sai_event_log
            WHERE event_type = 'OFFER_SENT'
        ),
        responded_pairs AS (
            -- Seleciona todos os pares únicos que tiveram uma resposta
            SELECT DISTINCT order_id, provider_id
            FROM desenvolvimento_bi.sai_event_log
            WHERE event_type IN ('PROVIDER_ACCEPTED', 'PROVIDER_REJECTED')
        ),
        already_logged_unanswered AS (
            -- Seleciona todos os pares que já foram logados como não respondidos
            SELECT DISTINCT order_id, provider_id
            FROM desenvolvimento_bi.sai_event_log
            WHERE event_type = 'UNANSWERED_OFFER'
        )
        -- Seleciona os pares enviados que não estão na lista de respondidos
        -- e que ainda não foram logados como não respondidos.
        SELECT
            s.order_id,
            s.provider_id
        FROM
            sent_pairs s
        LEFT JOIN
            responded_pairs r ON s.order_id = r.order_id AND s.provider_id = r.provider_id
        LEFT JOIN
            already_logged_unanswered alu ON s.order_id = alu.order_id AND s.provider_id = alu.provider_id
        WHERE
            r.provider_id IS NULL -- Filtra apenas as que não tiveram resposta
            AND alu.provider_id IS NULL; -- Filtra as que ainda não foram logadas
    """

