import datetime
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from textmining_scielo_scrapping.scripts.records_manager import (  # type: ignore
    get_records_list
)
from textmining_scielo_scrapping.scripts.magazine_manager import (  # type: ignore
    process_magazines
)
from textmining_scielo_scrapping.scripts.xml_manager import (  # type: ignore
    get_xml
)
from textmining_scielo_scrapping.scripts.pdf_manager import (  # type: ignore
    get_pdf
)
from textmining_scielo_scrapping.scripts.txt_manager import (  # type: ignore
    get_txt
)
from textmining_scielo_scrapping.scripts.stadistics_manager import (  # type: ignore
    create_statistics_file
)
from textmining_scielo_scrapping.environment import env  # type: ignore


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
    country_magazines = process_magazines.expand(country=countries)
    country_records = get_records_list.expand(country_magazines=country_magazines)
    country_xml = get_xml.expand(metadata=country_records)
    country_txt = get_txt.expand(metadata=country_xml)
    country_pdf = get_pdf.expand(metadata=country_txt)
    create_statistics_file(country_pdf)
