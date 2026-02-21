create schema if not exists stage;

create table if not exists stage.stage_jobs(
    job_id text not null,
    company_name text not null,
    job_title text not null,
    seniority_level text,
    location text not null,
    city text,
    state text,
    country text,
    is_remote boolean,
    posted_time date not null,
    posted_date_key integer not null,
    job_description text ,
    experience_level text,
    source_url text,
    run_date date not null,
    run_id text,
    dss_update_time timestamp default now()
);
select * from stage.stage_jobs;
create index if not exists ids_stage_jobs on stage.stage_jobs(job_id,company_name,posted_date_key);
create table if not exists stage.stage_fact_jobs(
    
    job_id text not null,
    run_id text not null,
    company_key integer,
    company_name text,
    location_key integer,
    location text,
    job_title_key integer,
    job_title text,
    posted_date_key integer,
    
    is_remote boolean,
    experience_level text,
    source_url text,
    run_date date,
    
    is_valid boolean default true,
    validation_error text,
    dss_load_time timestamp default current_timestamp
);

create index if not exists idx_stage_fact_jobs on stage.stage_fact_jobs(job_id,run_id);

select * from stage.stage_fact_jobs
order by dss_load_time desc;

