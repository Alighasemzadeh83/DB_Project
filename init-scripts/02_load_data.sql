COPY owners(hash_owner)
  FROM '/data/owners.csv' CSV HEADER;

COPY apps(hash_app,hash_owner)
  FROM '/data/apps.csv' CSV HEADER;

COPY functions(function_id,hash_app,trigger)
  FROM '/data/functions_clean.csv' CSV HEADER;

COPY function_mapping(function_id,hash_function)
  FROM '/data/function_mapping.csv' CSV HEADER;

--COPY fact_invocations_minutely_sparse(function_id,day,usage)
--FROM '/data/fact_invocations_minutely_sparse_pg.csv' CSV HEADER;
--WITH (FORMAT text, DELIMITER ',', NULL '', HEADER true);

COPY fact_function_duration_daily(function_id,day,avg_ms,min_ms,max_ms,p0,p1,p25,p50,p75,p99,p100,count)
  FROM '/data/fact_function_duration_daily.csv' CSV HEADER;

COPY fact_app_memory_daily(hash_app,day,sample_count,avg_mb,p1,p5,p25,p50,p75,p95,p99,p100)
  FROM '/data/fact_app_memory_daily.csv' CSV HEADER;
