create schema if not exists dim;

create table if not exists dim.dim_company(
    company_key_id serial primary key,
    company_name text unique not null,
    dss_create_time timestamp default current_timestamp,
    dss_update_time timestamp default current_timestamp
);

create index if not exists idx_company_name on dim.dim_company(company_name);

select * from dim.dim_company;

create table if not exists dim.dim_location(
    location_key_id serial primary key,
    city text,
    state text,
    country text,
    is_remote boolean,
    location text unique not null,
    dss_create_time timestamp default current_timestamp,
    dss_update_time timestamp default current_timestamp
);

create index if not exists idx_dim_location on dim.dim_location(location);

select * from dim.dim_location;


create table if not exists dim.dim_job_title (
    job_title_id serial primary key,
    job_title text unique not null,
    seniority_level text,
    dss_create_time timestamp default current_timestamp,
    dss_update_time timestamp default current_timestamp
);

create index if not exists idx_dim_job_title on dim.dim_job_title(job_title);

select * from dim.dim_job_title;

create table if not exists dim.dim_date(
    date_key integer primary key,
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
    dss_create_time timestamp default current_timestamp
);

create index if not exists idx_dim_date on dim.dim_date(full_date);