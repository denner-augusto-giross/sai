# query.py

def query_stuck_orders(city_id: int):
    """
    Retorna uma query SQL que encontra todas as corridas "travadas"
    para uma cidade específica, incluindo os dados de entrega.
    """
    return f"""
        -- A sua query de ordens travadas, agora com os dados de entrega
        WITH
        base_latitude as (
            -- ... (código CTE inalterado)
        ),
        base_longitude as (
            -- ... (código CTE inalterado)
        ),
        user_requests_ AS (
            SELECT
                ur.id,
                ur.user_id,
                -- ... (outros campos SELECT)

                -- ===================================
                -- ALTERAÇÃO PARA OBTER DADOS DE ENTREGA
                ur.latitude AS delivery_latitude,
                ur.longitude AS delivery_longitude,
                -- ===================================
                
                blat.latitude AS store_latitude,
                blon.longitude AS store_longitude
            FROM
                giross_producao.user_requests ur
                -- ... (código JOINs inalterado)
            WHERE
                -- ... (condições WHERE inalteradas)
                ur.city_id = {city_id} 
        )
        SELECT
            id AS order_id,
            user_id,
            user_name,
            amount AS value,
            city_id,
            store_latitude,
            store_longitude,
            delivery_latitude,
            delivery_longitude
        FROM
            user_requests_;
    """

def query_available_providers(city_id: int):
    """
    Retorna uma query SQL que busca todos os provedores disponíveis
    para uma cidade específica.
    """
    # Nota: Assumimos que a tabela de provedores tem uma coluna city_id.
    # Se não tiver, podemos remover o filtro desta query específica.
    return f"""
        -- A sua query de provedores, agora com o filtro de cidade
        WITH
        provider_releases AS (
            -- ... (código CTE inalterado)
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
            INNER JOIN giross_producao.provider_services ps ON p.id = ps.provider_id AND ps.status IN ('active', 'riding')
            LEFT JOIN giross_producao.provider_scores score ON p.id = score.provider_id
            LEFT JOIN provider_releases pr ON p.id = pr.provider_id
        WHERE
            -- ===================================
            -- ALTERAÇÃO PARA PRODUÇÃO AQUI
            p.city_id = {city_id}
            -- ===================================
        ORDER BY
            total_releases_last_2_weeks ASC,
            score DESC;
    """