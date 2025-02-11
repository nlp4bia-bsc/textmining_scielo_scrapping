import os
import csv

from airflow.decorators import task  # type: ignore

from textmining_scielo_scrapping.environment import env  # type: ignore
from textmining_scielo_scrapping.scripts.utils import xml_string_to_dict, load_json  # type: ignore
from textmining_scielo_scrapping.scripts.utils_scripts.scielo_stadistics import create_statistics_file_script  # type: ignore

@task()
def create_statistics_file(countries):
    create_statistics_file_script()