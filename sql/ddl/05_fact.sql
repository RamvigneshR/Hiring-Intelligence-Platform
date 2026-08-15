-- Active: 1780412979706@@localhost@5432@hiring_platform_prod
create schema if not exists fact;

create table if not exists fact.fact_jobs (
    fact_job_key serial primary key,
    job_id text not null,
    dim_company_key integer,
    dim_location_key integer,
    dim_job_title_key integer,
    dim_posted_date_key integer,
    is_remote boolean,
    experience_level text,
    source_url text,
    run_date date not null,
    dss_load_time timestamptz default now(),
    unique(job_id, run_date),
    foreign key (dim_company_key) references dim.dim_company(dim_company_key),
    foreign key (dim_location_key) references dim.dim_location(dim_location_key),
    foreign key (dim_job_title_key) references dim.dim_job_title(dim_job_title_key),
    foreign key (dim_posted_date_key) references dim.dim_date(dim_posted_date_key)
);
create index if not exists idx_fact_company on fact.fact_jobs(dim_company_key);
create index if not exists idx_fact_location on fact.fact_jobs(dim_location_key);
create index if not exists idx_fact_title on fact.fact_jobs(dim_job_title_key);
create index if not exists idx_fact_date on fact.fact_jobs(dim_posted_date_key);
create index if not exists idx_fact_job_id on fact.fact_jobs(job_id);

select * from fact.fact_jobs;