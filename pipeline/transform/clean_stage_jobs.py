import re
from datetime import datetime, timezone
from sqlalchemy import text
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def remove_htmls(text_value:str):
    if text_value is None:
        return None

    replacements = {
        "&lt;": "<",
        "&gt;": ">",
        "&quot;": '"',
        "&#39;": "'",
        "&nbsp;": " ",
        "&amp;": "&",
    }

    result = text_value
    for entity, char in replacements.items():
        result = result.replace(entity, char)
    return result


def remove_styles(text_value: str) -> str:
    if text_value is None:
        return None
    return re.sub(r"([a-zA-Z0-9\s\.\#,>:_-]+\{[^\}]*\}\s*)+", "", text_value)


def remove_html_tags(text_value: str) -> str:
    if text_value is None:
        return None

    text_value = remove_htmls(text_value)
    text_value = remove_styles(text_value)

    text_value = re.sub(r"<style[^>]*>.*?</style>", "", text_value, flags=re.DOTALL | re.IGNORECASE)
    text_value = re.sub(r"<script[^>]*>.*?</script>", "", text_value, flags=re.DOTALL | re.IGNORECASE)

    text_value = re.sub(r"<[^>]+>", "", text_value)

    text_value = re.sub(r"\s+", " ", text_value)
    return text_value.strip()


def derive_experience_level(description: str) -> str:
    if not description:
        return "unspecified"
    desc = description.lower()
    if any(word in desc for word in ["senior", "lead", "principal", "staff", "5+ years", "7+ years"]):
        return "senior"
    if any(word in desc for word in ["junior", "graduate", "entry", "intern", "working student", "werkstudent"]):
        return "fresher"
    return "mid"


def derive_seniority(job_title: str) -> str:
    if not job_title:
        return "unspecified"
    title = job_title.lower()
    if any(word in title for word in ["senior", "sr.", "lead", "head of", "director", "vp", "principal", "chief", "ceo", "cto", "founder"]):
        return "senior"
    if any(word in title for word in ["junior", "jr.", "werkstudent", "working student", "intern", "trainee"]):
        return "junior"
    if any(word in title for word in ["manager", "engineering manager"]):
        return "management"
    return "mid"


def parse_location(location: str, is_remote: bool) -> dict:
    if not location or not location.strip():
        return {"city": None, "state": None, "country": None}

    if is_remote or location.strip().lower() in ("remote job", "remote"):
        return {"city": None, "state": None, "country": None}

    parts = [p.strip() for p in location.split(",") if p.strip()]

    if not parts:
        return {"city": None, "state": None, "country": None}
    elif len(parts) == 1:
        return {"city": parts[0], "state": None, "country": None}
    elif len(parts) == 2:
        return {"city": parts[0], "state": None, "country": parts[1]}
    else:
        return {"city": parts[0], "state": parts[1], "country": parts[2]}


def parse_posted_date(posted_time):
    try:
        posted_date = datetime.fromtimestamp(int(posted_time), tz=timezone.utc).date()
        posted_date_key = int(posted_date.strftime("%Y%m%d"))
        return posted_date, posted_date_key
    except (ValueError, TypeError):
        return None, None


def clean_stage_jobs(start_date: str, end_date: str) -> int:
    validate()
    logger = get_logger("clean_stage_jobs", f"{start_date}_{end_date}")
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stage.stage_jobs RESTART IDENTITY"))

        rows = conn.execute(
            text("""
                SELECT job_id, company_name, role, location, is_remote,
                       posted_time, description, source_url, run_date
                FROM load.load_stage_jobs
                WHERE run_date BETWEEN :start_date AND :end_date
            """),
            {"start_date": start_date, "end_date": end_date},
        ).fetchall()

        inserted = 0
        skipped = 0

        for r in rows:
            posted_date, posted_date_key = parse_posted_date(r.posted_time)

            if posted_date is None:
                logger.error(f"Skipping job_id={r.job_id}: invalid posted_time={r.posted_time}")
                skipped += 1
                continue

            clean_desc = remove_html_tags(r.description)
            loc = parse_location(r.location, r.is_remote)

            conn.execute(
                text("""
                    INSERT INTO stage.stage_jobs (
                        job_id, company_name, job_title, seniority_level,
                        location, city, state, country, is_remote,
                        posted_time, posted_date_key, job_description,
                        experience_level, source_url, run_date
                    ) VALUES (
                        :job_id, :company_name, :job_title, :seniority_level,
                        :location, :city, :state, :country, :is_remote,
                        :posted_time, :posted_date_key, :job_description,
                        :experience_level, :source_url, :run_date
                    )
                """),
                {
                    "job_id": r.job_id,
                    "company_name": r.company_name.strip() if r.company_name else None,
                    "job_title": r.role,
                    "seniority_level": derive_seniority(r.role),
                    "location": r.location,
                    "city": loc["city"],
                    "state": loc["state"],
                    "country": loc["country"],
                    "is_remote": r.is_remote,
                    "posted_time": posted_date,
                    "posted_date_key": posted_date_key,
                    "job_description": clean_desc,
                    "experience_level": derive_experience_level(clean_desc),
                    "source_url": r.source_url,
                    "run_date": r.run_date,
                },
            )
            inserted += 1

    logger.info(f"stage.stage_jobs: {inserted} inserted, {skipped} skipped, range {start_date} to {end_date}")
    return inserted


if __name__ == "__main__":
    import argparse

    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=today)
    parser.add_argument("--end-date", default=today)
    args = parser.parse_args()

    clean_stage_jobs(args.start_date, args.end_date)