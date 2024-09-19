import datetime
from airflow import DAG
from airflow.decorators import task
from textmining_scielo_scrapping.scripts.scrapping import (
    get_magazine_list,
    get_records_list,
    get_records_text,
    save_metadata,
)
from textmining_scielo_scrapping.environment import env

@task
def get_args(**kwargs):
    config = kwargs.get('dag_run').conf
    return (
        config.get('countries', list(env["scielo-path"].keys()))
    )

with DAG(
    'textmining_scielo_scrapping',
    start_date=datetime.datetime(2024, 5, 10),
    schedule_interval=None,
    description='Scrapping for Scielo repository',
) as dag:
    countries = get_args()
    country_magazines = get_magazine_list.expand(country=countries)
    country_records = get_records_list.expand(country_magazines=country_magazines)
    records = get_records_text.expand(country_records=country_records)

    countries >> country_magazines >> country_records >> records
