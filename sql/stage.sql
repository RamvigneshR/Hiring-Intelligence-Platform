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
    job_description text ,
    experience_level text,
    source_url text,
    run_date date not null,
    run_id text,
    dss_update_time timestamp default now()
);

select * from stage.stage_jobs;
