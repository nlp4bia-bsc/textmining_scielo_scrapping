import os
import csv
from pandas.io.xml import preprocess_data
import requests
import fitz
import json
import pandas as pd

from airflow.decorators import task
from airflow.operators.python import get_current_context
from textmining_scielo_scrapping.scripts.utils import xml_string_to_dict
from textmining_scielo_scrapping.environment import env


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
    print(url)

    # Define the path where you want to save the JSON file
    output_dir = "/storage/temp/scielo_metadata"
    os.makedirs(output_dir, exist_ok=True)
    json_file_path = os.path.join(output_dir, f"{country}_magazines.json")

    # Check if the file already exists
    if os.path.exists(json_file_path):
        print(f"File {json_file_path} already exists. Reusing it.")
        return {"magazine_path": json_file_path, "country": country}

    # If the file does not exist, download the data
    response = requests.get(url, headers=env["headers"])
    magazines_metadata = xml_string_to_dict(response.text)

    magazines_sets_list = transform_array(magazines_metadata["OAI-PMH"]["ListSets"]["set"])

    # Write the data to the JSON file with proper encoding
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump({"magazines": magazines_sets_list, "country": country}, json_file, ensure_ascii=False)

    # Return the path of the JSON file
    return {"magazine_path": json_file_path, "country": country}


def preprocess_json_records(records):
    preprocess_records = []

    if isinstance(records, dict):
        records = [records]

    for item in records:
        transformed_item = {key.split("dc:")[1]: value for key, value in item["metadata"]["oai-dc:dc"].items() if key.startswith("dc:")}
        preprocess_records.append({**transformed_item, **item["header"]})
    return preprocess_records


def get_all_records_from_magazine(scielo_country_path, records_list_path, magazine_name, output_dir, resumption_token=None):
    all_records = []
    while True:
        url = f"{scielo_country_path}/{scielo_api}?{records_list_path}&set={magazine_name}"
        url_hash = magazine_name

        if resumption_token:
            url = f"{url}&resumptionToken={resumption_token}"
            url_hash = f"{magazine_name}_{resumption_token}"

        print(url)

        records_file_path = os.path.join(output_dir, f"{url_hash}.json")

        # Check if the URL already exists in the file
        if os.path.exists(records_file_path):
            print(f"Data for URL {url} already exists. Skipping request.")
            # Check if there is a resumptionToken to continue with the next one
            with open(records_file_path, 'r', encoding='utf-8') as json_file:
                existing_data = json.load(json_file)
                if "resumptionToken" in existing_data:
                    resumption_token = existing_data["resumptionToken"]
                else:
                    break  # Exit the loop if no resumptionToken is found
            continue

        response = requests.get(url, headers=env["headers"])
        records_metadata = xml_string_to_dict(response.text)

        if "OAI-PMH" not in records_metadata:
            print(f"Error: 'OAI-PMH' key not found in records_metadata for URL {url}")
            break

        data = records_metadata["OAI-PMH"]

        if "ListRecords" in data.keys():
            print(data["ListRecords"]["record"])
            preprocess_data = preprocess_json_records(data["ListRecords"]["record"])
            all_records.extend(preprocess_data)

            # Write the data to a new file
            with open(records_file_path, 'w', encoding='utf-8') as json_file:
                json.dump(preprocess_data, json_file, ensure_ascii=False)

            if "resumptionToken" in data["ListRecords"].keys():
                resumption_token = data["ListRecords"]["resumptionToken"]
            else:
                break
        else:
            break

    return all_records

@task(map_index_template="{{ country_magazines['country'] }}")
def get_records_list(country_magazines):
    # Read the JSON file created by the previous function
    json_file_path = country_magazines["magazine_path"]

    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    magazine_list = data["magazines"]
    country = data["country"]

    records_list_path = env["paths"]["get_records_path"]
    scielo_country_path = env["scielo-path"][country]

    # Define the path where you want to save the JSON files
    output_dir = os.path.join("/storage/temp/scielo_metadata", f"{country}_scielo_metadata")
    os.makedirs(output_dir, exist_ok=True)

    # State file to track progress
    state_file_path = os.path.join(output_dir, f"{country}_state.json")

    # Load state if it exists
    if os.path.exists(state_file_path):
        with open(state_file_path, 'r', encoding='utf-8') as state_file:
            state = json.load(state_file)
    else:
        state = {"processed_magazines": [], "country": country}

    # Fetch records for each magazine and update the state incrementally
    for magazine in magazine_list:
        if magazine['setName'] not in state["processed_magazines"]:
            print(f"Getting records from {magazine['setName']}")
            get_all_records_from_magazine(
                scielo_country_path,
                records_list_path,
                magazine['setSpec'],
                output_dir
            )
            state["processed_magazines"].append(magazine['setName'])

            # Save state after processing each magazine
            with open(state_file_path, 'w', encoding='utf-8') as state_file:
                json.dump(state, state_file, ensure_ascii=False)

    # Return the path of the CSV file
    return {"records_path": output_dir, "country": country}


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

def get_txt_path(record, country, output_dir):
    try:
        scielo_country_path = env["scielo-path"][country]
        scielo_get_pdf_path = env["paths"]["get_pdf"]
        record_identifier = record["identifier"].split(":")[2]
        main_path = os.path.join(output_dir, record_identifier)

        if is_exists(record_identifier, country):
            record['pdf_path'] = f"{main_path}.pdf"
            record['txt_path'] = f"{main_path}.txt"
            return record

        response = requests.get(url=f"{scielo_country_path}/{scielo_front}?{scielo_get_pdf_path}{record_identifier}", headers=env["headers"])
        pdf_url = extract_window_location(response.text)

        if not pdf_url or "None" in pdf_url:
            print(f"Error in url to download: {record_identifier}")
            return None

        pdf_path = download_scielo_article(pdf_url, f"{main_path}.pdf")
        txt_path = extract_text_from_pdf(pdf_path, f"{main_path}.txt")

        record['pdf_path'] = pdf_path
        record['txt_path'] = txt_path
        return record
    except Exception as e:
        print(f"Error in: {record}, Exception: {e}")
        return None


def get_txt_path_bulk(record_list, country, output_dir):
    return [
        get_txt_path(record, country, output_dir) for record in record_list
    ]


@task(map_index_template="{{ country_records['country'] }}")
def get_records_text(country_records):
    input_dir = country_records["records_path"]
    country = country_records["country"]

    # Define the path for the output directory
    output_dir = os.path.join(os.path.dirname(input_dir), f"{country}_scielo_records")
    os.makedirs(output_dir, exist_ok=True)

    # Define the path for the metadata file
    metadata_file_path = os.path.join(output_dir, f"{country}_record_metadata.json")

    # Load existing metadata if it exists
    if os.path.exists(metadata_file_path):
        with open(metadata_file_path, 'r', encoding='utf-8') as metadata_file:
            metadata = json.load(metadata_file)
    else:
        metadata = {"records": {}, "country": country}

    iteration_count = 0
    files_to_process = len(os.listdir(input_dir))
    files_processed = 0

    # Iterate over all JSON files in the input directory
    for filename in os.listdir(input_dir):
        print(f"{files_processed}/{files_to_process-1}: Processing file {filename}")
        if filename.endswith(".json") and filename != f"{country}_record_metadata.json":
            file_path = os.path.join(input_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as json_file:
                records = json.load(json_file)

            for record in records:
                if not isinstance(record, dict):
                    print(f"Record is not a dictionary. Skipping... Record: {record}")
                    continue

                identifier_parts = record.get("identifier", "").split(":")
                if len(identifier_parts) < 3:
                    print(f"Identifier {record['identifier']} does not have enough parts. Skipping...")
                    print(f"Record: {record}")
                    continue

                record_identifier = identifier_parts[2]
                if record_identifier not in metadata["records"]:
                    pdf_path = os.path.join(output_dir, f"{record_identifier}.pdf")
                    txt_path = os.path.join(output_dir, f"{record_identifier}.txt")

                    if os.path.exists(pdf_path) and os.path.exists(txt_path):
                        print(f"Record {record_identifier} already exists. Adding to metadata and skipping...")
                        record['pdf_path'] = pdf_path
                        record['txt_path'] = txt_path
                        metadata["records"][record_identifier] = record
                    else:
                        updated_record = get_txt_path(record, country, output_dir)
                        if updated_record:
                            metadata["records"][record_identifier] = updated_record

                    iteration_count += 1

                    if iteration_count % 10 == 0:
                        with open(metadata_file_path, 'w', encoding='utf-8') as metadata_file:
                            json.dump(metadata, metadata_file, ensure_ascii=False, indent=4)
        files_processed += 1

    if iteration_count % 10 != 0:
        with open(metadata_file_path, 'w', encoding='utf-8') as metadata_file:
            json.dump(metadata, metadata_file, ensure_ascii=False, indent=4)

    return metadata_file_path


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
