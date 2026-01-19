CREATE TABLE IF NOT EXISTS raw.jobs_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    job_id TEXT,
    payload JSONB,
    page INT,
    run_date DATE,
    run_id TEXT,
    dss_load_time TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_job_id
ON raw.jobs_raw (job_id);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_run_date
ON raw.jobs_raw (run_date);

SELECT * FROM raw.raw_jobs;

WITH CTE AS (
    SELECT 
    job_id,payload,run_date,run_id,
    ROW_NUMBER() OVER (PARTITION BY job_id,payload,run_date,run_id
    ORDER BY job_id,payload,run_date,run_id) as rn
    FROM raw.raw_jobs
)
SELECT * FROM CTE WHERE rn>1;