create schema if not exists load;

create table if not exists load.load_stage_jobs (
    id serial primary key,
    job_id text,
    role text,
    description text,
    company_name text,
    location text,
    is_remote boolean,
    source_url text,
    posted_time text,
    run_date date,
    dss_load_time timestamptz default now()
);

select * from load.load_stage_jobs