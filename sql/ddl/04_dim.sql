create schema if not exists dim;

create table if not exists dim.dim_company (
    dim_company_key serial primary key,
    company_name text unique not null,
    dss_create_time timestamptz default now(),
    dss_update_time timestamptz default now()
);
create index if not exists idx_dim_company_name on dim.dim_company(company_name);

create table if not exists dim.dim_location (
    dim_location_key serial primary key,
    city text,
    state text,
    country text,
    is_remote boolean,
    location text unique,
    dss_create_time timestamptz default now(),
    dss_update_time timestamptz default now()
);
create index if not exists idx_dim_location on dim.dim_location(location);

create table if not exists dim.dim_job_title (
    dim_job_title_key serial primary key,
    job_title text unique not null,
    seniority_level text,
    dss_create_time timestamptz default now(),
    dss_update_time timestamptz default now()
);
create index if not exists idx_dim_job_title on dim.dim_job_title(job_title);

create table if not exists dim.dim_date (
    dim_posted_date_key integer primary key,
    full_date date unique not null,
    year integer,
    quarter integer,
    month integer,
    month_name text,
    week_of_year integer,
    day_of_month integer,
    day_of_week integer,
    day_name text,
    is_weekend boolean,
    dss_create_time timestamptz default now()
);
create index if not exists idx_dim_date on dim.dim_date(full_date);

--select * from dim.dim_company;

--select * from dim.dim_location;

--select * from dim.dim_job_title;

--select * from dim.dim_date;