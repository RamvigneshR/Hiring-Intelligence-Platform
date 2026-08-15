-- Active: 1780412979706@@localhost@5432@hiring_platform_prod
create schema if not exists stage;

create table if not exists stage.stage_jobs (
    job_id text not null,
    company_name text,
    job_title text,
    seniority_level text,
    location text,
    city text,
    state text,
    country text,
    is_remote boolean,
    posted_time date,
    posted_date_key integer,
    job_description text,
    experience_level text,
    source_url text,
    run_date date not null,
    dss_load_time timestamptz default now()
);

create index if not exists idx_stage_jobs_job_id on stage.stage_jobs(job_id);
create index if not exists idx_stage_jobs_company on stage.stage_jobs(company_name);
create index if not exists idx_stage_jobs_posted_date_key on stage.stage_jobs(posted_date_key);
create index if not exists idx_stage_jobs_run_date on stage.stage_jobs(run_date);

--select * from stage.stage_jobs;


create table if not exists stage.stage_fact_jobs (
    job_id text not null,
    dim_company_key integer not null,
    dim_location_key integer,
    dim_job_title_key integer not null,
    dim_posted_date_key integer not null,
    is_remote boolean,
    experience_level text,
    source_url text,
    run_date date,
    dss_load_time timestamptz default now()
);

create table if not exists stage.filter_stage_fact_jobs (
    job_id text,
    company_name text,
    location text,
    job_title text,
    posted_date_key integer,
    run_date date,
    rejection_reason text,
    dss_load_time timestamptz default now()
);

--select * from stage.stage_fact_jobs;

--select * from stage.filter_stage_fact_jobs;