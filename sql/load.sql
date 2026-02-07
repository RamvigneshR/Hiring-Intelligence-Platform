create schema if not exists load;
create table if not exists load.load_jobs(
    id serial,
    run_id text,
    job_id text,
    Role text,
    description text,
    company_name text,
    location text,
    is_remote boolean,
    source_url text,
    posted_time text,
    run_date timestamp,
    dss_load_time timestamp default now()
);  

select * from load.load_jobs;


