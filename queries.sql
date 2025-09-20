-- query 1
SELECT fm.function_id,
       f.day,
       u.minute_of_day,
       u.calls
FROM fact_invocations_minutely_sparse f
CROSS JOIN LATERAL (
    SELECT cnt::int AS calls, ord AS minute_of_day
    FROM unnest(f.usage) WITH ORDINALITY AS t(cnt, ord)
    ORDER BY cnt DESC
    LIMIT 1
) AS u
JOIN function_mapping fm ON fm.function_id = f.function_id
ORDER BY u.calls DESC
LIMIT 10;




-- query 2
WITH candidates AS (
    SELECT
        fim.function_id,
        fim.day,
        s.start_minute,
        s.end_minute,
        s.duration_minutes
    FROM fact_invocations_minutely_sparse AS fim
    CROSS JOIN LATERAL (
        SELECT
            MIN(i) AS start_minute,
            MAX(i) AS end_minute,
            COUNT(*) AS duration_minutes
        FROM (
            SELECT
                i,
                i - ROW_NUMBER() OVER (ORDER BY i) AS grp
            FROM generate_subscripts(fim."usage", 1) AS i
            WHERE fim."usage"[i] = 0
        ) z
        GROUP BY grp
        HAVING COUNT(*) >= 720  -- at least 12 hours of consecutive zeros
        ORDER BY MIN(i)
        LIMIT 1
    ) AS s
    WHERE 0 = ANY (fim."usage")
)
SELECT
    fm.hash_function,
    c.function_id,
    fa.hash_app,
    a.hash_owner,
    c.day,
    c.start_minute,
    c.end_minute,
    c.duration_minutes
FROM candidates c
LEFT JOIN function_mapping fm USING (function_id)
LEFT JOIN functions fa USING (function_id)
LEFT JOIN apps a USING (hash_app)
ORDER BY c.day, c.function_id;





-- query 3
WITH daily_invocations AS (
    SELECT 
        function_id,
        day,
        CAST(SUM(unnested_value) AS BIGINT) as daily_total
    FROM (
        SELECT 
            function_id,
            day,
            unnest(usage) as unnested_value
        FROM fact_invocations_minutely_sparse
    ) t
    GROUP BY function_id, day
),
stats AS (
    SELECT 
        function_id,
        AVG(daily_total) as avg_invocations,
        STDDEV(daily_total) as stddev_invocations,
        COUNT(*) as days_count
    FROM daily_invocations
    GROUP BY function_id
    HAVING COUNT(*) > 1  
)
SELECT 
    s.function_id,
    fm.hash_function,
    f.hash_app,
    a.hash_owner,
    s.avg_invocations,
    s.stddev_invocations,
    CASE 
        WHEN s.avg_invocations > 0 THEN s.stddev_invocations / s.avg_invocations
        ELSE 0 
    END as coefficient_of_variation,
    s.days_count
FROM stats s
JOIN functions f ON f.function_id = s.function_id
JOIN apps a ON a.hash_app = f.hash_app
JOIN function_mapping fm ON fm.function_id = s.function_id
ORDER BY coefficient_of_variation DESC;


-- query 4
WITH invocation_totals AS (
    SELECT 
        function_id,
        day,
        CAST(SUM(unnested_value) AS BIGINT) as daily_invocations
    FROM (
        SELECT 
            function_id,
            day,
            unnest(usage) as unnested_value
        FROM fact_invocations_minutely_sparse
    ) t
    GROUP BY function_id, day
)
SELECT 
    fdd.function_id,
    fm.hash_function,
    f.hash_app,
    fdd.day,
    fdd.avg_ms,
    fdd.count as duration_samples,
    COALESCE(it.daily_invocations, 0) as invocations,
    CORR(fdd.avg_ms, it.daily_invocations) OVER (PARTITION BY fdd.function_id) as correlation
FROM fact_function_duration_daily fdd
LEFT JOIN invocation_totals it 
    ON it.function_id = fdd.function_id 
    AND it.day = fdd.day
JOIN functions f ON f.function_id = fdd.function_id
JOIN function_mapping fm ON fm.function_id = fdd.function_id
WHERE fdd.count > 0
ORDER BY fdd.function_id, fdd.day;





-- query 5
WITH usage_agg AS (
    SELECT 
        f.function_id,
        a.hash_app,
        o.hash_owner,
        SUM( COALESCE(unnest_val, 0) ) AS total_usage
    FROM owners o
    JOIN apps a ON a.hash_owner = o.hash_owner
    JOIN functions f ON f.hash_app = a.hash_app
    JOIN fact_invocations_minutely_sparse fis ON fis.function_id = f.function_id
    CROSS JOIN LATERAL unnest(fis.usage) AS u(unnest_val)
    GROUP BY f.function_id, a.hash_app, o.hash_owner
),
joined AS (
    SELECT 
        ua.function_id,
        ua.hash_app,
        ua.hash_owner,
        ua.total_usage,
        fd.avg_ms,
        fam.avg_mb
    FROM usage_agg ua
    LEFT JOIN fact_function_duration_daily fd ON fd.function_id = ua.function_id
    LEFT JOIN fact_app_memory_daily fam ON fam.hash_app = ua.hash_app
)
SELECT 
    j.hash_owner,
    SUM(j.total_usage)     AS sum_total_usage,
    SUM(j.avg_mb)          AS sum_avg_mb,
    SUM(j.avg_ms)          AS sum_avg_ms
FROM joined j
GROUP BY j.hash_owner
ORDER BY sum_total_usage DESC;





-- query 6
WITH daily_invocations AS (
    SELECT 
        function_id,
        day,
        CAST(SUM(unnested_value) AS BIGINT) as daily_total
    FROM (
        SELECT 
            function_id,
            day,
            unnest(usage) as unnested_value
        FROM fact_invocations_minutely_sparse
    ) t
    GROUP BY function_id, day
),
avg_invocations AS (
    SELECT 
        function_id,
        AVG(daily_total) as avg_daily,
        MAX(daily_total) as max_daily
    FROM daily_invocations
    GROUP BY function_id
)
SELECT 
    di.function_id,
    fm.hash_function,
    f.hash_app,
    a.hash_owner,
    di.day,
    di.daily_total as invocations_on_day,
    ai.avg_daily as average_daily_invocations,
    di.daily_total / NULLIF(ai.avg_daily, 0) as ratio
FROM daily_invocations di
JOIN avg_invocations ai ON ai.function_id = di.function_id
JOIN functions f ON f.function_id = di.function_id
JOIN apps a ON a.hash_app = f.hash_app
JOIN function_mapping fm ON fm.function_id = di.function_id
WHERE di.daily_total > 2 * ai.avg_daily
ORDER BY ratio DESC;





-- query 7
CREATE OR REPLACE FUNCTION array_sum(arr INTEGER[])
RETURNS BIGINT AS $$
DECLARE
    total BIGINT := 0;
    elem INTEGER;
BEGIN
    IF arr IS NULL THEN
        RETURN 0;
    END IF;

    FOREACH elem IN ARRAY arr
    LOOP
        total := total + elem;
    END LOOP;

    RETURN total;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION has_consecutive_zeros(arr SMALLINT[], consecutive_count INT)
RETURNS BOOLEAN AS $$
DECLARE
    current_streak INT := 0;
    i INT;
BEGIN
    FOR i IN 1..array_length(arr, 1) LOOP
        IF arr[i] = 0 THEN
            current_streak := current_streak + 1;
            IF current_streak >= consecutive_count THEN
                RETURN TRUE;
            END IF;
        ELSE
            current_streak := 0;
        END IF;
    END LOOP;
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE INDEX IF NOT EXISTS idx_invocations_function_day ON fact_invocations_minutely_sparse(function_id, day);
CREATE INDEX IF NOT EXISTS idx_duration_function_day ON fact_function_duration_daily(function_id, day);
CREATE INDEX IF NOT EXISTS idx_memory_app_day ON fact_app_memory_daily(hash_app, day);
CREATE INDEX IF NOT EXISTS idx_functions_app ON functions(hash_app);


SELECT 
    f.function_id,
    f.hash_app,
    f.trigger,
    inv.day,
    array_sum(inv.usage) as total_daily_invocations
FROM functions f
JOIN fact_invocations_minutely_sparse inv ON f.function_id = inv.function_id
WHERE has_consecutive_zeros(inv.usage, 720)
ORDER BY inv.day, f.function_id;   
WITH invocation_stats AS (
    SELECT 
        function_id,
        SUM(array_sum(usage)) AS total_invocations
    FROM fact_invocations_minutely_sparse
    GROUP BY function_id
),
duration_stats AS (
    SELECT 
        function_id,
        CASE 
            WHEN SUM(count) > 0 THEN SUM(avg_ms * count) / SUM(count)
            ELSE 0 
        END AS weighted_avg_duration,
        SUM(count) AS total_executions
    FROM fact_function_duration_daily
    GROUP BY function_id
),
memory_stats AS (
    SELECT 
        f.function_id,
        CASE 
            WHEN SUM(m.sample_count) > 0 THEN SUM(m.avg_mb * m.sample_count) / SUM(m.sample_count)
            ELSE 0 
        END AS weighted_avg_memory,
        SUM(m.sample_count) AS total_samples
    FROM functions f
    JOIN fact_app_memory_daily m ON f.hash_app = m.hash_app
    GROUP BY f.function_id
),
criticality_calculation AS (
    SELECT 
        f.function_id,
        f.hash_app,
        f.trigger,
        COALESCE(i.total_invocations, 0) AS total_invocations,
        COALESCE(d.weighted_avg_duration, 0) AS avg_duration_ms,
        COALESCE(d.total_executions, 0) AS total_executions,
        COALESCE(m.weighted_avg_memory, 0) AS avg_memory_mb,
        COALESCE(m.total_samples, 0) AS total_memory_samples,
       
        (COALESCE(i.total_invocations, 0) * 0.4 + 
         COALESCE(d.weighted_avg_duration, 0) * 0.0003 +  
         COALESCE(m.weighted_avg_memory, 0) * 0.003) AS criticality_score
    FROM functions f
    LEFT JOIN invocation_stats i ON f.function_id = i.function_id
    LEFT JOIN duration_stats d ON f.function_id = d.function_id
    LEFT JOIN memory_stats m ON f.function_id = m.function_id
)
SELECT 
    function_id,
    hash_app,
    trigger,
    total_invocations,
    avg_duration_ms,
    total_executions,
    avg_memory_mb,
    total_memory_samples,
    criticality_score,

    CASE 
        WHEN criticality_score > 1000 THEN 'خیلی بالا'
        WHEN criticality_score > 100 THEN 'بالا'
        WHEN criticality_score > 10 THEN 'متوسط'
        ELSE 'پایین'
    END AS criticality_level
FROM criticality_calculation
ORDER BY criticality_score DESC;

WITH daily_invocations AS (
    SELECT 
        function_id,
        day,
        array_sum(usage) AS daily_count
    FROM fact_invocations_minutely_sparse
),
function_avg_invocations AS (
    SELECT 
        function_id,
        AVG(daily_count) AS avg_daily_count,
        STDDEV(daily_count) AS std_daily_count
    FROM daily_invocations
    GROUP BY function_id
),
anomaly_detection AS (
    SELECT 
        d.function_id,
        d.day,
        d.daily_count,
        f.avg_daily_count,
        f.std_daily_count,
       
        CASE 
            WHEN d.daily_count > 2 * f.avg_daily_count THEN TRUE
            ELSE FALSE
        END AS is_anomaly,
    
        (d.daily_count - f.avg_daily_count) / NULLIF(f.std_daily_count, 0) AS z_score
    FROM daily_invocations d
    JOIN function_avg_invocations f ON d.function_id = f.function_id
    WHERE f.avg_daily_count > 0 
)
SELECT 
    a.function_id,
    f.hash_app,
    f.trigger,
    a.day,
    a.daily_count,
    ROUND(a.avg_daily_count::numeric, 2) AS avg_daily_count,
    ROUND(a.std_daily_count::numeric, 2) AS std_daily_count,
    ROUND(a.z_score::numeric, 2) AS z_score,
    a.is_anomaly,
  
    CASE 
        WHEN a.z_score > 3 THEN 'شدید'
        WHEN a.z_score > 2 THEN 'متوسط'
        ELSE 'خفیف'
    END AS anomaly_severity
FROM anomaly_detection a
JOIN functions f ON a.function_id = f.function_id
WHERE a.is_anomaly = TRUE
ORDER BY a.z_score DESC, a.day DESC;













-- query 8
WITH invocation_stats AS (
    SELECT 
        function_id,
        SUM(inv_count) AS total_invocations
    FROM (
        SELECT 
            function_id,
            day,
            SUM(unnested_usage) AS inv_count
        FROM fact_invocations_minutely_sparse,
        LATERAL unnest(usage) AS unnested_usage
        GROUP BY function_id, day
    ) daily_invocations
    GROUP BY function_id
),
duration_stats AS (
    SELECT 
        function_id,
        CASE 
            WHEN SUM(count) > 0 THEN SUM(avg_ms * count) / SUM(count)
            ELSE 0 
        END AS weighted_avg_duration,
        SUM(count) AS total_executions
    FROM fact_function_duration_daily
    GROUP BY function_id
),
memory_stats AS (
    SELECT 
        f.function_id,
        CASE 
            WHEN SUM(m.sample_count) > 0 THEN SUM(m.avg_mb * m.sample_count) / SUM(m.sample_count)
            ELSE 0 
        END AS weighted_avg_memory
    FROM functions f
    JOIN fact_app_memory_daily m ON f.hash_app = m.hash_app
    GROUP BY f.function_id
)
SELECT 
    f.function_id,
    f.hash_app,
    COALESCE(i.total_invocations, 0) AS total_invocations,
    COALESCE(d.weighted_avg_duration, 0) AS avg_duration_ms,
    COALESCE(d.total_executions, 0) AS total_executions,
    COALESCE(m.weighted_avg_memory, 0) AS avg_memory_mb,
    (COALESCE(i.total_invocations, 0) * 0.4 + 
     COALESCE(d.weighted_avg_duration, 0) * 0.0003 + 
     COALESCE(m.weighted_avg_memory, 0) * 0.003) AS criticality_score  
FROM functions f
LEFT JOIN invocation_stats i ON f.function_id = i.function_id
LEFT JOIN duration_stats d ON f.function_id = d.function_id
LEFT JOIN memory_stats m ON f.function_id = m.function_id
ORDER BY criticality_score DESC;




-- query 9
SELECT 
    fdd.function_id,
    fm.hash_function,
    f.hash_app,
    a.hash_owner,
    AVG(fdd.p75) as avg_p75,
    AVG(fdd.p99) as avg_p99,
    AVG(fdd.p99 - fdd.p75) as avg_difference,
    MAX(fdd.p99 - fdd.p75) as max_difference,
    -- Volatility score
    CASE 
        WHEN AVG(fdd.p75) > 0 
        THEN AVG(fdd.p99 - fdd.p75) / AVG(fdd.p75)
        ELSE 0 
    END as volatility_ratio
FROM fact_function_duration_daily fdd
JOIN functions f ON f.function_id = fdd.function_id
JOIN apps a ON a.hash_app = f.hash_app
JOIN function_mapping fm ON fm.function_id = fdd.function_id
WHERE fdd.p75 IS NOT NULL AND fdd.p99 IS NOT NULL
GROUP BY fdd.function_id, fm.hash_function, f.hash_app, a.hash_owner
HAVING AVG(fdd.p99 - fdd.p75) > 100  -- At least 100ms difference
ORDER BY volatility_ratio DESC;


-- query 10
WITH memory_trend AS (
    SELECT 
        hash_app,
        day,
        avg_mb,
        sample_count,
        LAG(avg_mb, 1) OVER (PARTITION BY hash_app ORDER BY day) as prev_day_mb,
        AVG(avg_mb) OVER (PARTITION BY hash_app ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3day
    FROM fact_app_memory_daily
    WHERE hash_app = 'YOUR_HASH_APP'  -- Replace with actual hash
        AND day <= 12
)
SELECT 
    hash_app,
    day,
    avg_mb,
    sample_count,
    prev_day_mb,
    avg_mb - prev_day_mb as daily_change_mb,
    CASE 
        WHEN prev_day_mb > 0 
        THEN ((avg_mb - prev_day_mb) / prev_day_mb) * 100
        ELSE 0 
    END as daily_change_percent,
    moving_avg_3day,
    MIN(avg_mb) OVER () as min_mb_period,
    MAX(avg_mb) OVER () as max_mb_period,
    AVG(avg_mb) OVER () as avg_mb_period
FROM memory_trend
ORDER BY day;
