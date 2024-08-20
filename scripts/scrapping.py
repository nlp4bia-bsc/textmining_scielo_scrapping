import os
import requests
import fitz
import json
import pandas as pd

from airflow.decorators import task
from airflow.operators.python import get_current_context
from scielo_scrapping.scripts.utils import xml_string_to_dict
from scielo_scrapping.environment import env


scielo_api = env["scielo-api-path"]["xml"]
scielo_front = env["scielo-api-path"]["front"]

def transform_array(input_array):
    output_array = []
    
    for item in input_array:
        if isinstance(item['setSpec'], list) and isinstance(item['setName'], list):
            for spec, name in zip(item['setSpec'], item['setName']):
                output_array.append({
                    "setSpec": spec,
                    "setName": name
                })

        else:
            output_array.append(item)
    
    return output_array


@task(map_index_template="{{ country }}")
def get_magazine_list(country):
    magazine_list_path = env["paths"]["get_magazines_path"]
    scielo_country_path = env["scielo-path"][country]
    url = f"{scielo_country_path}/{scielo_api}?{magazine_list_path}"

    response = requests.get(url, headers=env["headers"])
    magazines_metadata = xml_string_to_dict(response.text)

    magazines_sets_list = transform_array(magazines_metadata["OAI-PMH"]["ListSets"]["set"])

    return {"magazines": magazines_sets_list, "country": country}


def preprocess_json_records(records):
    preprocess_records = []

    for item in records:
        transformed_item = {key.split("dc:")[1]: value for key, value in item["metadata"]["oai-dc:dc"].items() if key.startswith("dc:")}
        preprocess_records.append({**transformed_item, **item["header"]})
    return preprocess_records


@task(map_index_template="{{ country_magazines['country'] }}")
def get_records_list(country_magazines):
    magazine_list = country_magazines["magazines"]
    country = country_magazines["country"]

    records_list_path = env["paths"]["get_records_path"]
    scielo_country_path = env["scielo-path"][country]

    records = {}
    for magazine in magazine_list:

        print(f"Getting records from {magazine['setName']}")
        url = f"{scielo_country_path}/{scielo_api}?{records_list_path}&set={magazine['setSpec']}"

        response = requests.get(url, headers=env["headers"])
        records_metadata = xml_string_to_dict(response.text)
        
        data = records_metadata["OAI-PMH"]
        if "ListRecords" in data.keys():
            records[magazine['setName']] = preprocess_json_records(data["ListRecords"]["record"])
    
    return {"records": records, "country": country}


def extract_window_location(html_text):
    import re
    pattern = r'window.location\s*=\s*"([^"]+)"'
    match = re.search(pattern, html_text)
    if match:
        return match.group(1)
    return None


def download_scielo_article(url, save_path):
    response = requests.get(url, headers=env["headers"])

    if response.status_code == 200:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        with open(save_path, 'wb') as file:
            file.write(response.content)

        return save_path
    else:
        print(f"Error downloading the file: {response.status_code}")
        return None
    

def extract_text_from_pdf(file_path, save_path):
    pdf_document = fitz.open(file_path)
    
    text = ""
    
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text += page.get_text()

    with open(save_path, 'w', encoding='utf-8') as txt_file:
        txt_file.write(text)

    return save_path

def is_exists(identifier, country):
    path = f"/storage/temp/scielo_{country}"
    pdf_path = f"{path}/{identifier}.pdf"
    txt_path = f"{path}/{identifier}.txt"

    return os.path.exists(pdf_path) and os.path.exists(txt_path)


def get_txt_path(record, country):
    try:
        scielo_country_path = env["scielo-path"][country]
        scielo_get_pdf_path = env["paths"]["get_pdf"]
        record_identifier = record["identifier"].split(":")[2]
        main_path = f"/storage/temp/scielo_{country}/{record_identifier}"

        if is_exists(record_identifier, country):
            record['pdf_path'] = f"{main_path}.pdf"
            record['txt_path'] = f"{main_path}.txt"
            return record

        response = requests.get(url = f"{scielo_country_path}/{scielo_front}?{scielo_get_pdf_path}{record_identifier}", headers=env["headers"])
        pdf_url = extract_window_location(response.text)

        if not pdf_url or "None" in pdf_url:
            print(f"Error in url to download: {record_identifier}")
            return None
        
        
        pdf_path = download_scielo_article(pdf_url, f"{main_path}.pdf")
        txt_path = extract_text_from_pdf(pdf_path, f"{main_path}.txt")

        record['pdf_path'] = pdf_path
        record['txt_path'] = txt_path
        return record
    except:
        print(f"Error in: {record}")
        return None


def get_txt_path_bulk(record_list, country):
    return [
        get_txt_path(record, country) for record in record_list
    ]


@task(map_index_template="{{ country_records['country'] }}")
def get_records_text(country_records):
    magazine_list = country_records["records"]
    country = country_records["country"]

    record_path = {}

    for magazine in magazine_list.keys():
        record_path[magazine] = get_txt_path_bulk(magazine_list[magazine], country)

    return {"magazines": record_path, "country": country}


@task(map_index_template="{{ records['country'] }}")
def save_metadata(records):
    data = records["magazines"]
    country = records["country"]
    dfs = []
    
    output_dir = f"/storage/temp/scielo_{country}"
    os.makedirs(output_dir, exist_ok=True)
    
    json_path = f"{output_dir}/raw_data.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)
    print(f"Raw data saved as JSON in {json_path}")
    
    for magazine, list_dict in data.items():
        valid_dicts = [d for d in list_dict if d is not None and d]
        if valid_dicts:
            df = pd.DataFrame(valid_dicts)
            df['magazine'] = magazine
            dfs.append(df)
        else:
            print(f"Warning: No valid data for magazine {magazine}")
    
    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        csv_path = f"{output_dir}/metadata.csv"
        df_final.to_csv(csv_path, index=False)
        print(f"Metadata saved as CSV in {csv_path}")
    else:
        print("No valid data to save as CSV")

    dest_dir_temp = f"/storage/temp/scielo_{country}"
    print(f"Download the new version in https://textmining2.bsc.es/api/pipelines/download?path={dest_dir_temp}")
    