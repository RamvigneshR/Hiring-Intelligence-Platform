create schema if not exists fact;

create table if not exists fact.fact_jobs(
    fact_job_key serial primary key,
    job_id text not null,
    company_key integer not null,
    location_key integer not null,
    job_title_key integer not null,
    posted_date_key integer not null,
    is_remote boolean,
    experience_level text,
    source_url text,
    run_id text not null,
    run_date date not null,
    dss_load_time timestamp default current_timestamp,

    unique(job_id,run_id),
    foreign key (company_key) references dim.dim_company(company_key),
    foreign key (location_key) references dim.dim_location(location_key),
    foreign key (job_title_key) references dim.dim_job_title(job_title_key),
    foreign key (posted_date_key) references dim.dim_date(date_key)
);

create index idx_fact_company on fact.fact_jobs(company_key);
create index idx_fact_location on fact.fact_jobs(location_key);
create index idx_fact_title on fact.fact_jobs(job_title_key);
create index idx_fact_date on fact.fact_jobs(posted_date_key);
create index idx_fact_run_id on fact.fact_jobs(run_id);
create index idx_fact_date_company on fact.fact_jobs(posted_date_key, company_key);
create index idx_fact_job_id on fact.fact_jobs(job_id);

select * from fact.fact_jobs;

