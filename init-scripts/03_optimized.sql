
CREATE TABLE owners (
    hash_owner TEXT PRIMARY KEY
);

CREATE TABLE apps (
    hash_app TEXT PRIMARY KEY,
    hash_owner TEXT NOT NULL REFERENCES owners(hash_owner)
);

CREATE TABLE functions (
    function_id BIGINT,
    hash_app TEXT NOT NULL REFERENCES apps(hash_app),
    trigger TEXT,
    PRIMARY KEY(function_id, trigger)
);

CREATE TABLE function_mapping (
    function_id BIGINT PRIMARY KEY,
    hash_function TEXT NOT NULL
);

CREATE TABLE fact_invocations_minutely_sparse (
    function_id BIGINT NOT NULL,
    day INT NOT NULL,
    usage INTEGER[1440],
    PRIMARY KEY (function_id, day)
);

CREATE TABLE fact_function_duration_daily (
    function_id BIGINT NOT NULL,
    day INT NOT NULL,
    avg_ms DOUBLE PRECISION,
    min_ms DOUBLE PRECISION,
    max_ms DOUBLE PRECISION,
    p0 DOUBLE PRECISION,
    p1 DOUBLE PRECISION,
    p25 DOUBLE PRECISION,
    p50 DOUBLE PRECISION,
    p75 DOUBLE PRECISION,
    p99 DOUBLE PRECISION,
    p100 DOUBLE PRECISION,
    count BIGINT,
    PRIMARY KEY (function_id, day, count)
);

CREATE TABLE fact_app_memory_daily (
    hash_app TEXT NOT NULL REFERENCES apps(hash_app),
    day INT NOT NULL,
    sample_count BIGINT,
    avg_mb DOUBLE PRECISION,
    p1 DOUBLE PRECISION,
    p5 DOUBLE PRECISION,
    p25 DOUBLE PRECISION,
    p50 DOUBLE PRECISION,
    p75 DOUBLE PRECISION,
    p95 DOUBLE PRECISION,
    p99 DOUBLE PRECISION,
    p100 DOUBLE PRECISION,
    PRIMARY KEY (hash_app, day, sample_count)
);

CREATE TABLE raw_invocations (
    function_id BIGINT,
    day INT,
    minute INT,
    count INT DEFAULT 1
);



CREATE OR REPLACE FUNCTION increment_minute_fast(
    p_function_id BIGINT,
    p_day INT,
    p_minute INT,
    p_count INT DEFAULT 1
) RETURNS VOID AS $$
BEGIN
    IF p_minute < 1 OR p_minute > 1440 THEN
        RAISE EXCEPTION 'Minute must be between 1 and 1440';
    END IF;

    -- Try updating the minute
    UPDATE fact_invocations_minutely_sparse
    SET usage[p_minute] = LEAST(usage[p_minute] + p_count, 2147483647)
    WHERE function_id = p_function_id AND day = p_day;


    IF NOT FOUND THEN
        INSERT INTO fact_invocations_minutely_sparse(function_id, day, usage)
        VALUES (
            p_function_id,
            p_day,
            array_fill(0::integer, ARRAY[1440])
        );

        UPDATE fact_invocations_minutely_sparse
        SET usage[p_minute] = LEAST(usage[p_minute] + p_count, 2147483647)
        WHERE function_id = p_function_id AND day = p_day;
    END IF;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION trigger_increment_minute()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM increment_minute_fast(
        NEW.function_id,
        NEW.day,
        NEW.minute,
        COALESCE(NEW.count, 1)
    );
    RETURN NULL;  
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER fact_usage_increment_trigger
AFTER INSERT ON raw_invocations
FOR EACH ROW
EXECUTE FUNCTION trigger_increment_minute();


CREATE SEQUENCE function_mapping_function_id_seq START 1;

ALTER TABLE function_mapping
ALTER COLUMN function_id SET DEFAULT nextval('function_mapping_function_id_seq');



CREATE INDEX idx_fim_function_day
ON fact_invocations_minutely_sparse(function_id, day);

CREATE INDEX idx_raw_invocations_function_day_minute
ON raw_invocations(function_id, day, minute);

CREATE INDEX idx_function_mapping_hash
ON function_mapping(hash_function);

CREATE INDEX idx_apps_owner
ON apps(hash_owner);

CREATE INDEX idx_fim_day
ON fact_invocations_minutely_sparse(day);


CREATE TABLESPACE fast_tmp LOCATION '/mnt/ramdisk';
ALTER DATABASE azuredata SET temp_tablespaces = 'fast_tmp';

ALTER SYSTEM SET shared_buffers = '16GB';
ALTER SYSTEM SET work_mem = '128MB';
ALTER SYSTEM SET maintenance_work_mem = '4GB';
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_workers = 8;

SELECT pg_reload_conf();