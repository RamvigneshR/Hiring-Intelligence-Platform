Hiring Intelligence Platform
----------------------------

A data engineering pipeline that ingests live job postings from Arbeitnow API,transforms the raw data and loads structured records into a PostgreSQL using star schema ready for analytics and AI.

Architecture
-------------

Arbeitnow API
- **Raw Layer**		- Fetch JSON pages -> Save to local storage -> Load into raw.raw_jobs
- **Load Layer**	- Extract fields from JSONB payload -> load.load_jobs
- **Stage Layer**	- Clean, parse -> stage.stage_jobs
- **Dim Layer**      - Populate dimension tables (company, location, job title, date)
- **Stage Fact**     - Join stage with dim_tables -> stage.stage_fact_jobs
- **Fact Layer**     - Insert only valid records -> fact.fact_jobs

## Tech Stack

- **Language:**           Python  
- **Database:**           PostgreSQL  
- **ORM / Connector:**    SQLAlchemy  
- **API Source:**         Arbeitnow API  
- **Config Management:**  python-dotenv (.env)  
- **Logging:**            Python logging, file-based  
- **Orchestration:**      Airflow (planned)  
- **Cloud:**              Azure (planned)  
- **Scale:**              PySpark (planned)  
- **Transformation:**     dbt (planned)             

Data Model (Star Schema)
-------------------------
**Fact Table**
- **fact.fact_jobs** - one row per unique job posting per run

**Dimension Tables**
 - **dim.dim_company** - unique companies
 - **dim.dim_location** - city,state,country,remote flag
 - **dim.dim_job_title** - job title + seniority level
 - **dim.dim_date** - full date records from 2020 to 210

**Other Layers**
- **raw.raw_jobs** - raw JSONB from API
- **load.load_jobs** - flattened fields extracted from JSONB
- **stage.stage_jobs** - cleaned and enriched records
- **stage.stage_fact_jobs** - pre-validation layer with is_valid flag

Setup Procedure
----------------

**Step 1: Clone the repository**  
git clone https://github.com/RamvigneshR/Hiring-Intelligence-Platform  
cd Hiring-Intelligence-Platform  

**Step 2: Install dependencies**  
pip install -r requirements.txt

**Step 3: Configure environment**  
cp .env_example .env

**Edit '.env' with your values:**  
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=postgres
DB_PASS=your_password
WEBSITE_URL=https://www.arbeitnow.com/api/job-board-api
PAGES_TO_RUN=5
STORAGE_PATH=data/raw
LOG_DIR=logs
```
**step 4: Create database schemas and tables**
**Run SQL files in this order:**
```
sql/raw.sql
sql/load.sql
sql/stage.sql
sql/dim.sql
sql/fact.sql
```

Pipeline Execution Order
-------------------------
Run scripts in this exact order:

**Step 1 - Fetch from API and save JSON files:**
```python raw_scripts/api_to_files.py```

**Step 2 - Load JSON files into raw.raw_jobs:**
```python raw_scripts/raw_jobs.py```

**Step 3 - Extract fields into load.load_jobs:**
```python load_scripts/load_jobs.py```

**Step 4 - Clean and enriched data into stage.stage_jobs:**
```python stage_scripts/stage_jobs.py```

**Step 5 - Populate dimension tables:**
```
python dim_scripts/dim_date.py
python dim_scripts/dim_company.py
python dim_scripts/dim_location.py
python dim_scripts/dim_job_title.py
```

**Step 6 - Build pre-fact table for validation:**
```python stage_scripts/stage_fact_jobs.py```

**Step 7 - Load valid records into fact.fact_jobs:**
```python fact_scripts/fact_jobs.py```

Features
---------

- **Paginated API ingestion with configurable page count**
- **JSONB storage for full payload preservation**
- **HTML tag removal from job descriptions**
- **Automatic seniority detection(senior / mid / junior) from job titles**
- **Experience level detection(senior / mid / fresher) from description**
- **Location parsing into city,state,country**
- **Date dimension laoding from 2020 to 2100**
- **Foreign key validation before fact table load**
- **Duplicate detection in staging layer**
- **File based logging per layer**

Planned to implement
---------------------

- Apache Airflow DAG for orchestration  
- Azure Data Lake Storage for raw JSON files (replacing local storage)  
- PySpark for large-scale transformation  
- dbt for SQL transformations  
- AI layer for job market trend insights  

Logging
--------
Every layer generates a log file under the 'logs/' directory:  
eg: logs/stage_2025_01_01_120000_20250101.log

Log entries follow the format:  
YYYY-MM-DD HH:MM:SS INFO Step 100: task started  
YYYY-MM-DD HH:MM:SS INFO Step 400: Successfully inserted x rows  
YYYY-MM-DD HH:MM:SS ERROR Step 200: failure detail
