-- Active: 1785217612835@@localhost@5432@hiring_platform_prod
create schema if not exists raw;

create table if not exists raw.raw_jobs (
    raw_id bigserial primary key,
    job_id text,
    payload jsonb,
    page int,
    run_date date,
    dss_load_time timestamptz default now(),
    unique(job_id, run_date)
);

create index if not exists idx_raw_jobs_job_id on raw.raw_jobs(job_id);
create index if not exists idx_raw_jobs_run_date on raw.raw_jobs(run_date);

create table if not exists raw.etl_file_load (
    file_path text primary key,
    run_date date not null,
    layer text not null,
    status text not null,
    row_count int,
    loaded_at timestamptz default now()
);

create index if not exists idx_etl_file_load_run_date on raw.etl_file_load(run_date);
create index if not exists idx_etl_file_load_status on raw.etl_file_load(status);

select * from raw.etl_file_load;

select * from raw.raw_jobs;