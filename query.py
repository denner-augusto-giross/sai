def query_region(cities_ids: list):

    cities_ids_str = f"({', '.join(map(str, cities_ids))})"

    return f"""
        WITH user_requests_sp AS (
            SELECT 
                ur.estimated_total, 
                ur.id, 
                ur.provider_id, 
                ur.status, 
                ROUND(ur.estimated_total*(1-(COALESCE(prl.percent, 20)/100)), 2) as amount,
                CASE 
                    WHEN ur.scheduled_cod IS NULL THEN TIMESTAMPDIFF(MINUTE, ur.original_created_at, NOW()) 
                    ELSE TIMESTAMPDIFF(MINUTE, ur.started_at, NOW()) 
                END AS time_,
                CASE 
                    WHEN ur.scheduled_cod IS NULL THEN DATE(ur.original_created_at)
                    ELSE DATE(ur.started_at)
                END AS creation_date_,
                CASE 
                    WHEN ur.user_id IN (51688, 51687, 52567, 52570, 52593, 52566, 52591, 52592, 52489, 48833, 48831, 50269, 48832, 50272, 53056, 48830) THEN 'Zona Leste'
                    WHEN ur.user_id IN (51656, 48816) THEN 'Vila Prudente'
                    WHEN ur.user_id IN (42716, 51679, 51685, 44517, 44521, 44519, 43243, 44548, 42720, 52407, 52428, 52598, 52511) THEN 'Vila Mariana'
                    WHEN ur.user_id IN (44559, 52409, 52626, 52412, 52420, 52539, 52513, 52506, 52475, 52608, 52419, 50273, 44525, 53207, 53186, 53185, 53158, 53088) THEN 'Tatuapé'
                    WHEN ur.user_id IN (51668, 51677, 42715, 40821, 52611, 52610, 52549, 52609, 53096, 53021, 43294, 42422) THEN 'Santana'
                    WHEN ur.user_id IN (51657, 44514, 51689, 44523, 50549) THEN 'Pinheiros'
                    WHEN ur.user_id IN (51666, 52488, 52583, 52497, 48817) THEN 'Mooca'
                    WHEN ur.user_id IN (50084, 50490, 53320, 53109) THEN 'Limão'
                    WHEN ur.user_id IN (49806, 50081, 53147, 50080, 49804) THEN 'Jardim Planalto'
                    WHEN ur.user_id IN (51676, 51678, 51684) THEN 'Ipiranga'
                    WHEN ur.user_id IN (44512) THEN 'Indianópolis'
                    WHEN ur.user_id IN (52604, 48822, 48829, 53156) THEN 'Cidade Líder'
                    WHEN ur.user_id IN (51660, 51664, 51673, 51680, 51675, 51674, 42719, 50549, 52501, 48815, 48814) THEN 'Centro'
                    WHEN ur.user_id IN (51681, 51659, 44518, 53087) THEN 'Carrão'
                    WHEN ur.user_id IN (51686, 50235, 53222, 50215, 40818, 53137, 53125, 53072) THEN 'Campo Limpo'
                    WHEN ur.user_id IN (51442) THEN 'Butantã'
                    WHEN ur.user_id IN (51671, 50537, 51669, 44549) THEN 'Brooklin'
                    -- Salvador
                    WHEN ur.user_id IN (41051,40892,37943,40454,38371,40457,37693,40451,40453,40455,40456,40466,37695) THEN 'Barra'
                    WHEN ur.user_id IN (51856,40460,40461,40462,40459,38902,39540,37944,40458,41049,47926,52330,37721,38901,38923) THEN 'Brotas'
                    WHEN ur.user_id IN (37715,38603,53212,49085,37947,40464,40465) THEN 'Cabula'
                    WHEN ur.user_id IN (37948,39538,37949,38022,42859) THEN 'Imbuí'
                    WHEN ur.user_id IN (48412,37698,42339) THEN 'Nazaré'
                    WHEN ur.user_id IN (38728) THEN 'Peri Peri'
                    WHEN ur.user_id IN (38930) THEN 'Pirajá'
                    WHEN ur.user_id IN (37714,42592,53014,42594,39693,42595,37707,37942,21750,40473,37681,37725,40476,37703,37720,40474,37708,38604,40472,40497,47929,40471,40478,37940,40822,40469,40475) THEN 'Pituba'
                    WHEN ur.user_id IN (40283,40463,41323,38910,38926,38023,41322) THEN 'São Caetano'
                    WHEN ur.user_id IN (39150,37718,42854,44277,40467,47504,38893,39541,49033,52014,38770,44258) THEN 'Stella Mares'
                END AS region,
                c.id as city_id
            FROM 
                giross_producao.user_requests ur
            LEFT JOIN giross_producao.user_request_delay_rules urdr
            ON urdr.type = 'USER'
            AND ur.user_id = urdr.value
            AND ur.original_created_at >= urdr.created_at
            LEFT JOIN giross_producao.percentage_receipt_location prl
            ON prl.location_type = 'USER'
            #AND prl.deleted_at IS NULL
            AND ur.user_id = prl.value
            AND (
            CASE 
                WHEN ur.integration_service like '%Turbo%' THEN 'turbo' 
                WHEN ur.integration_service like '%d+1%' THEN 'd+1' 
                WHEN (CASE
                        WHEN urdr.value IS NOT NULL AND ur.integration_service IS NOT NULL AND ur.schedule_at IS NOT NULL AND ur.scheduled_cod IS NULL AND ur.integration_service NOT LIKE '%d+1%' THEN 'Integrado - Delay'
                        WHEN ur.integration_service IS NOT NULL AND ur.schedule_at IS NOT NULL AND ur.scheduled_cod IS NULL THEN 'Integrado - Agendado'
                        WHEN ur.integration_service IS NOT NULL AND ur.schedule_at IS NULL AND ur.scheduled_cod IS NULL THEN 'Integrado - Nuvem'
                        WHEN ur.integration_service IS NOT NULL AND ur.schedule_at IS NULL AND ur.scheduled_cod IS NOT NULL THEN 'Integrado - Embarque Rápido'
                        WHEN ur.integration_service IS NULL AND ur.schedule_at IS NOT NULL AND ur.scheduled_cod IS NULL THEN 'Manual - Agendado'
                        WHEN ur.integration_service IS NULL AND ur.schedule_at IS NULL AND ur.scheduled_cod IS NULL THEN 'Manual - Nuvem'
                        WHEN ur.integration_service IS NULL AND ur.schedule_at IS NULL AND ur.scheduled_cod IS NOT NULL THEN 'Manual - Embarque Rápido'
                    END) LIKE '%Embarque Rápido%' THEN 'Embarque Rapido' 
                ELSE 'Sem Modalidade'
            END) = COALESCE(prl.integration_service, 'Sem Modalidade')
            AND ur.original_created_at BETWEEN prl.created_at AND COALESCE(prl.deleted_at, NOW())
            LEFT JOIN giross_producao.cities c 
			ON ur.city_id = c.id
            WHERE 
                CASE 
                    WHEN ur.scheduled_cod IS NULL THEN TIMESTAMPDIFF(MINUTE, ur.original_created_at, NOW()) 
                    ELSE TIMESTAMPDIFF(MINUTE, ur.started_at, NOW()) END >= 3 
                AND ur.status = 'SEARCHING'
                AND ur.provider_id IN (0, 1266)
                AND DATE(COALESCE(ur.original_created_at, ur.started_at)) = CURDATE() -- Filter by today's DAY
                AND city_id in {cities_ids_str}
        )
        SELECT 
            id AS order_id, 
            region AS region,
            amount AS value,
            city_id AS city_id
        FROM user_requests_sp
        WHERE (region, city_id) IN (
            SELECT region, city_id
            FROM user_requests_sp
            GROUP BY region, city_id
            HAVING COUNT(DISTINCT id) >= 
                CASE 
                    WHEN city_id = 112 THEN 3 
                    ELSE 1 
                END
        );
    """

def query_all_cities():
    """
    Generates a SQL query to retrieve all records from the 'cities' table.
    """
    return "SELECT * FROM cities;"

def query_stuck_orders():
    """
    Returns a SQL query that finds all orders that have been searching for a
    provider for more than a specified time (e.g., >= 1 minute).
    The query also retrieves the store's latitude and longitude.
    """
    return """
        -- Use multiple Common Table Expressions (CTEs) to get store coordinates and then process user requests.
        WITH
base_latitude as (
    select
        ua.user_id,
        ua.value as latitude
    from giross_producao.user_attributes ua
    inner join (select user_id, max(id) as id from giross_producao.user_attributes where attribute_id = 2 group by 1) ua1
        on ua.id = ua1.id
    where ua.attribute_id = 2
),
base_longitude as (
    select
        ua.user_id,
        ua.value as longitude
    from giross_producao.user_attributes ua
    inner join (select user_id, max(id) as id from giross_producao.user_attributes where attribute_id = 3 group by 1) ua1
        on ua.id = ua1.id
    where ua.attribute_id = 3
),
user_requests_ AS (
    SELECT
        ur.estimated_total,
        ur.id,
        ur.provider_id,
        ur.status,
        -- Add the address column
        ur.s_address,
        ROUND(
            ur.estimated_total * (
                1 - (COALESCE(prl.percent, 20) / 100)
            ),
            2
        ) AS amount,
        CASE
            WHEN ur.scheduled_cod IS NULL THEN TIMESTAMPDIFF(MINUTE, ur.original_created_at, NOW())
            ELSE TIMESTAMPDIFF(MINUTE, ur.started_at, NOW())
        END AS time_,
        CASE
            WHEN ur.scheduled_cod IS NULL THEN DATE(ur.original_created_at)
            ELSE DATE(ur.started_at)
        END AS creation_date_,
        ur.user_id,
        REGEXP_REPLACE(
            TRIM(
                REPLACE(
                    REPLACE(
                        CONCAT(u.first_name, ' ', u.last_name),
                        'Integração ',
                        ''
                    ),
                    'Integracao',
                    ''
                )
            ),
            ' - [A-Z ]+ - [A-Z]{2}$',
            ''
        ) AS user_name,
        c.id AS city_id,
        -- Add store latitude and longitude from the CTEs
        blat.latitude AS store_latitude,
        blon.longitude AS store_longitude
    FROM
        giross_producao.user_requests ur
        LEFT JOIN giross_producao.user_request_delay_rules urdr ON urdr.type = 'USER'
            AND ur.user_id = urdr.value
            AND ur.original_created_at >= urdr.created_at
        LEFT JOIN giross_producao.percentage_receipt_location prl ON prl.location_type = 'USER'
            AND ur.user_id = prl.value
            AND (
                CASE
                    WHEN ur.integration_service LIKE '%Turbo%' THEN 'turbo'
                    WHEN ur.integration_service LIKE '%d+1%' THEN 'd+1'
                    WHEN (
                        CASE
                            WHEN urdr.value IS NOT NULL
                            AND ur.integration_service IS NOT NULL
                            AND ur.schedule_at IS NOT NULL
                            AND ur.scheduled_cod IS NULL
                            AND ur.integration_service NOT LIKE '%d+1%' THEN 'Integrado - Delay'
                            WHEN ur.integration_service IS NOT NULL
                            AND ur.schedule_at IS NOT NULL
                            AND ur.scheduled_cod IS NULL THEN 'Integrado - Agendado'
                            WHEN ur.integration_service IS NOT NULL
                            AND ur.schedule_at IS NULL
                            AND ur.scheduled_cod IS NULL THEN 'Integrado - Nuvem'
                            WHEN ur.integration_service IS NOT NULL
                            AND ur.schedule_at IS NULL
                            AND ur.scheduled_cod IS NOT NULL THEN 'Integrado - Embarque Rápido'
                            WHEN ur.integration_service IS NULL
                            AND ur.schedule_at IS NOT NULL
                            AND ur.scheduled_cod IS NULL THEN 'Manual - Agendado'
                            WHEN ur.integration_service IS NULL
                            AND ur.schedule_at IS NULL
                            AND ur.scheduled_cod IS NULL THEN 'Manual - Nuvem'
                            WHEN ur.integration_service IS NULL
                            AND ur.schedule_at IS NULL
                            AND ur.scheduled_cod IS NOT NULL THEN 'Manual - Embarque Rápido'
                        END
                    ) LIKE '%Embarque Rápido%' THEN 'Embarque Rapido'
                    ELSE 'Sem Modalidade'
                END
            ) = COALESCE(prl.integration_service, 'Sem Modalidade')
            AND ur.original_created_at BETWEEN prl.created_at AND COALESCE(prl.deleted_at, NOW())
        LEFT JOIN giross_producao.cities c ON ur.city_id = c.id
        LEFT JOIN giross_producao.users u ON ur.user_id = u.id
        -- Join the location CTEs
        LEFT JOIN base_latitude blat ON ur.user_id = blat.user_id
        LEFT JOIN base_longitude blon ON ur.user_id = blon.user_id
    WHERE
        CASE
            WHEN ur.scheduled_cod IS NULL THEN TIMESTAMPDIFF(MINUTE, ur.original_created_at, NOW())
            ELSE TIMESTAMPDIFF(MINUTE, ur.started_at, NOW())
        END >= 1
        AND ur.status = 'SEARCHING'
        AND ur.provider_id IN (0, 1266)
        AND DATE(
            COALESCE(ur.original_created_at, ur.started_at)
        ) = CURDATE()
)
SELECT
    id AS order_id,
    user_id,
    user_name,
    s_address AS address,
    amount AS value,
    city_id,
    store_latitude,
    store_longitude
FROM
    user_requests_;
    """

def query_available_providers():
    """
    Returns a SQL query that retrieves all online providers, ranked by their
    reliability (low releases) and score.
    """
    return """
        -- Use a CTE to gather the total releases for each provider.
        WITH
        provider_releases AS (
            SELECT
                provider_id,
                COUNT(id) AS total_releases
            FROM
                giross_producao.provider_cancelled_user_requests
            WHERE
                -- Filter for releases that occurred in the last 14 days.
                created_at >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
            GROUP BY
                1
        )
        -- Final SELECT to join all provider data and display their key information.
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
            -- Join to get only online providers
            INNER JOIN giross_producao.provider_services ps ON p.id = ps.provider_id AND ps.status IN ('active', 'riding')
            -- Join to get provider scores
            LEFT JOIN giross_producao.provider_scores score ON p.id = score.provider_id
            -- Join to get provider releases from the last 2 weeks
            LEFT JOIN provider_releases pr ON p.id = pr.provider_id
        ORDER BY
            -- Rank providers by the most reliable first
            total_releases_last_2_weeks ASC,
            score DESC;
    """