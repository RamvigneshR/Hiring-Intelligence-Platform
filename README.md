# Hiring Intelligence Platform

An ETL pipeline that pulls job postings from the Arbeitnow API, cleans and models them into a star schema in PostgreSQL and builds toward a full data engineering stack (Airflow, dbt and cloud).

This started as a set of standalone scripts and was rebuilt into a proper layered pipeline as I worked through real production concerns - idempotency, environment separation, data quality handling, and schema design. The v1-backup branch has the original version if you want to see where this started.

## Architecture

Data flows through five layers, and each layer has a different persistence rule depending on what it's for:

Arbeitnow API
    |
    v
raw -> append-only, never touched again. This is the source of truth for what the API actually sent.
    |
    v
load -> truncate + reload, scoped to whatever date range was requested. Just unpacks JSONB into flat columns, no cleaning.
    |
    v
stage -> truncate + reload. HTML/entity cleanup, location parsing, seniority/experience derivation.
    |
    v
dim -> upsert only, never truncated. Company, location, job title, and date lookups.
    |
    v
fact -> append-only, permanent. One row per job per day. This is the actual historical record.

Two tables sit between stage and fact:
- stage.stage_fact_jobs - rows where every dimension lookup succeeded, ready to load
- stage.filter_stage_fact_jobs - rows that failed a dimension lookup, with a reason, kept separate so bad data doesn't block good data from loading

****Why the layers behave differently****

**Raw and fact never truncate**:
This was a deliberate decision, not an oversight - raw needs to stay immutable so you can always answer "what did the source actually give us," and fact needs to accumulate so you can answer "what did the job market look like on any given day." load and stage don't need to answer either question on their own, so treating them as disposable, rebuilt-every-run tables keeps the pipeline simpler without losing anything.

**Idempotency is handled with a table not filename parsing** 
Early version tried to figure out "has this file already been loaded" by parsing timestamps out of filenames. It got fragile fast - multiple runs on the same day, partial failures, backfills all made it worse. raw.etl_control tracks file_path / status / row_count / loaded_at per file, so a rerun just checks the table instead of guessing from a filename.

**Dimensions are Type 1** 
None of the current dimension attributes (company name, job title, location string) actually change over time in a way that needs historical tracking - a company's name doesn't get versioned it just gets overwritten. If a field like company_size or industry gets added later from a richer data source, that's when Type 2 would actually make sense. Building it now would just be unused complexity.

**Clean and rejected records are split into separate tables**
 not filtered with a WHERE clause on one table. If a job's company or title can't be matched to a dimension, it goes into filter_stage_fact_jobs with a specific reason instead of silently disappearing or blocking the whole load. The rest of the batch still makes it into fact.fact_jobs.

## Known limitations (being upfront about these)

- **Location parsing** Source data has anywhere from 1 to 3 comma separated location parts with no consistent meaning per position ("Hamburg" vs "Berlin, Germany" vs "Bielefeld, Nordrhein-Westfalen, Deutschland"). City is usually reliable; state/country get filled where the structure allows it, NULL otherwise. No attempt is made to guess beyond what's actually there.
- **Seniority/experience detection is keyword based**
 It'll misclassify anything that doesn't use common title/description patterns. Fine for a first pass, not something I'd trust for anything analytical yet.
- **No salary extraction.** 
A few postings mention salary in free text (e.g. "mindestens 90.000€") but there's no structured field for it from the API. Planned as a v3 addition, probably regex-based first
- **A job is never marked as closed/inactive.** Once it's in fact.fact_jobs, it stays, even if the listing disappears from the source. Also on the v3 list.

## Tech Stack

- **Language:** Python
- **Database:** PostgreSQL
- **DB layer:** SQLAlchemy
- **Config:** python-dotenv, environment-driven (ENV=dev / ENV=prod)
- **Logging:** Python's logging module, file + console output per layer
- **Orchestration:** Apache Airflow (in progress)
- **Transformation:** dbt (planned)
- **Cloud:** planned- Azure/AWS

## Project Structure


pipeline/
├── extract/
│   ├── fetch_arbeitnow.py       # hits the API writes JSON to storage
│   └── load_raw_jobs.py         # loads JSON into raw.raw_jobs tracks via etl_control for idempotency
├── transform/
│   ├── load_stage_jobs.py       # raw JSONB -> flat columns in load.load_jobs
│   ├── clean_stage_jobs.py      # cleaning/parsing -> stage.stage_jobs
│   ├── build_stage_fact.py      # joins stage against dims, keeps only clean rows
│   └── build_filter_stage_fact.py  # same join captures the rejected records which causes failing of pipeline
├── dimensions/
│   ├── load_dim_company.py
│   ├── load_dim_location.py
│   ├── load_dim_job_title.py
│   └── load_dim_date.py
└── load/
    └── load_fact_jobs.py        # appends valid rows into fact.fact_jobs

config/
├── settings.py    # single place reading env vars,.env selection
└── db.py          # SQLAlchemy engine

sql/ddl/
├── 01_raw.sql
├── 02_load.sql
├── 03_stage.sql
├── 04_dim.sql
└── 05_fact.sql

utils/
└── logger.py      # shared logger-one file + console logger per script run


## Setup

**1. Clone and install dependencies**
bash
git clone https://github.com/RamvigneshR/Hiring-Intelligence-Platform.git
cd Hiring-Intelligence-Platform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


**2. Set up Postgres and create both databases**
bash
sudo apt install postgresql postgresql-contrib
sudo service postgresql start
psql -U postgres -c "CREATE DATABASE hiring_platform_dev;"
psql -U postgres -c "CREATE DATABASE hiring_platform_prod;"


**3. Configure environment**

Copy .env.prod.example and fill in real values for each environment:
bash
cp .env.prod.example .env.dev
cp .env.prod.example .env.prod

Edit both with the right DB_NAME (hiring_platform_dev / hiring_platform_prod) and your actual Postgres credentials. Neither file is committed - they're gitignored on purpose.

**4. Apply the schema**
bash
export $(grep -v '^#' .env.dev | xargs)
psql -U "$DB_USER" -h "$DB_HOST" -d "$DB_NAME" -f sql/ddl/01_raw.sql
psql -U "$DB_USER" -h "$DB_HOST" -d "$DB_NAME" -f sql/ddl/02_load.sql
psql -U "$DB_USER" -h "$DB_HOST" -d "$DB_NAME" -f sql/ddl/03_stage.sql
psql -U "$DB_USER" -h "$DB_HOST" -d "$DB_NAME" -f sql/ddl/04_dim.sql
psql -U "$DB_USER" -h "$DB_HOST" -d "$DB_NAME" -f sql/ddl/05_fact.sql

Repeat against .env.prod for the prod database.

## Running the pipeline

Each step can be run standalone, and most accept --start-date/--end-date (format YYYYMMDD), defaulting to today if omitted.

bash
python -m pipeline.extract.fetch_arbeitnow
python -m pipeline.extract.load_raw_jobs
python -m pipeline.transform.load_stage_jobs
python -m pipeline.transform.clean_stage_jobs
python -m pipeline.dimensions.load_dim_company
python -m pipeline.dimensions.load_dim_location
python -m pipeline.dimensions.load_dim_job_title
python -m pipeline.dimensions.load_dim_date
python -m pipeline.transform.build_stage_fact
python -m pipeline.transform.build_filter_stage_fact
python -m pipeline.load.load_fact_jobs


To switch environments, set ENV before running:
bash
ENV=prod python -m pipeline.extract.fetch_arbeitnow

Defaults to dev if unset.

## Status

**Done:**
- Full raw → fact pipeline, tested end-to-end against both dev and prod
- Idempotent ingestion via table for tracking(raw.etl_file_load)
- Environment isolated config
- Data quality validation with clean/reject separation

**In progress:**
- Airflow DAG to orchestrate the full sequence on a schedule

**Planned:**
- dbt for the transformation layer
- Cloud deployment (raw storage + managed Postgres)

## Logging

Every script writes to logs/{layer}_{run_id}_{date}.log and also streams to console. Format:

2026-08-09 14:30:22 INFO Starting fetch: 1 page(s) from https://www.arbeitnow.com/api/job-board-api
2026-08-09 14:30:23 INFO Page 1 saved to arbeitnow_json/jobs_20260809_143022_page_1.json
2026-08-09 14:30:24 ERROR Skipping job_id=xyz: invalid posted_time=None

