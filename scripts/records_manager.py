import os
import csv
import json
import requests
import urllib.parse
import re

from airflow.decorators import task  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
from urllib.parse import urlparse, parse_qs

from textmining_scielo_scrapping.environment import env  # type: ignore
from textmining_scielo_scrapping.scripts.utils import xml_string_to_dict, load_json  # type: ignore

scielo_api = env["scielo-api-path"]["xml"]
scielo_front = env["scielo-api-path"]["front"]

def extract_pid(url):
    parsed = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed.query)
    return query_params.get("pid", [""])[0]

def preprocess_json_records(records):
    """
    Procesa cada registro extraído del XML, extrayendo solo las etiquetas
    que comienzan con 'dc:' (quitando el prefijo) y combinándolo con la información del header.
    """
    preprocess_records = []
    if isinstance(records, dict):
        records = [records]
    for item in records:
        if "metadata" in item and "oai-dc:dc" in item["metadata"]:
            transformed_item = {
                key.split("dc:")[1]: value
                for key, value in item["metadata"]["oai-dc:dc"].items()
                if key.startswith("dc:")
            }
        else:
            transformed_item = {}
        record_data = {**item.get("header", {}), **transformed_item}
        full_url = record_data.get("identifier", "")
        pid = extract_pid(full_url)
        record_data["identifier"] = pid
        record_data["url"] = full_url
        preprocess_records.append(record_data)
    return preprocess_records

def get_all_records_from_magazine(scielo_country_path, records_list_path, magazine_id, resumption_token=None):
    """
    Descarga (usando paginación con resumptionToken) todos los registros (artículos)
    de una revista identificada por magazine_id.
    """
    all_records = []
    while True:
        url = f"{scielo_country_path}/{scielo_api}?{records_list_path}&set={magazine_id}"
        if resumption_token:
            url = f"{url}&resumptionToken={resumption_token}"
        print("Solicitando:", url)
        
        response = requests.get(url, headers=env["headers"])
        if response.status_code != 200:
            print(f"Error {response.status_code} al obtener registros para {magazine_id}")
            break

        records_metadata = xml_string_to_dict(response.text)
        data = records_metadata.get("OAI-PMH", {})
        
        if "ListRecords" in data:
            records = data["ListRecords"].get("record", [])
            processed_records = preprocess_json_records(records)
            all_records.extend(processed_records)
            
            token = data["ListRecords"].get("resumptionToken")
            if token:
                if isinstance(token, dict):
                    resumption_token = token.get("#text", "")
                else:
                    resumption_token = token
                if not resumption_token:
                    break
            else:
                break
        else:
            break
    return all_records

def process_records_for_magazine(scielo_country_path, records_list_path, magazine, country):
    """
    Para la revista indicada:
      1. Descarga todos los registros (artículos) mediante OAI-PMH usando el setSpec indicado.
      2. Agrega a cada registro la clave "articles" con el total de artículos para esa revista.
         (Si no se encuentran registros, se asigna 0.)
      3. Genera un CSV para la revista con la metadata en:
         /storage/temp/scielov2/{country}/scielo_records_metadata/records_metadata_{magazine_id}.csv
      4. Durante la iteración, si algún registro no tiene el campo "setSpec", se le asigna el valor de magazine["setSpec"].
    Retorna la lista de registros (artículos) procesados.
    """
    magazine_id = magazine["setSpec"]
    records = get_all_records_from_magazine(scielo_country_path, records_list_path, magazine_id)
    if not records:
        print(f"No se encontraron registros para {magazine['magazine_name']}. Se asigna 0 artículos.")
        records = []  # Aseguramos que sea una lista
    total_articles = len(records)
    for record in records:
        record["articles"] = total_articles
        if not record.get("setSpec", "").strip():
            record["setSpec"] = magazine["setSpec"]

    # Guardar archivo CSV para la revista en la carpeta scielo_records_metadata
    csv_folder = f"/storage/temp/scielov2/{country}/scielo_records_metadata"
    os.makedirs(csv_folder, exist_ok=True)
    csv_filename = os.path.join(csv_folder, f"records_metadata_{magazine_id}.csv")
    all_keys = set()
    for record in records:
        # Si no tiene setSpec, se asigna el magazine_id (del objeto magazine)
        if not record.get("setSpec", "").strip():
            record["setSpec"] = magazine["setSpec"]
        all_keys.update(record.keys())
    fieldnames = list(all_keys)
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
         writer.writeheader()
         for record in records:
             writer.writerow(record)
    print(f"CSV guardado para la revista: {csv_filename}")
    return records

def update_magazine_csv(magazine_rows, magazine_csv_file):
    """
    Reescribe el CSV de revistas usando el diccionario magazine_rows.
    Cada registro debe tener al menos las columnas: issn, magazine_name y articles.
    """
    if magazine_rows:
        first_key = next(iter(magazine_rows))
        fieldnames = list(magazine_rows[first_key].keys())
    else:
        fieldnames = ["issn", "magazine_name", "articles"]
    with open(magazine_csv_file, "w", newline="", encoding="utf-8") as csvfile:
         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
         writer.writeheader()
         for row in magazine_rows.values():
             writer.writerow(row)

def compile_articles_csv(country):
    """
    Itera por todos los CSV de artículos por revista en:
       /storage/temp/scielov2/{country}/scielo_records_metadata
    y crea un archivo CSV consolidado en:
       /storage/temp/scielov2/{country}/scielo_records_metadata_compiled.csv.
    Durante la iteración, si alguna fila no tiene el campo "setSpec", se le asigna el valor
    extraído del nombre del archivo (asumiendo el formato: records_metadata_{magazine_id}.csv).
    """
    folder = os.path.join(f"/storage/temp/scielov2/{country}", "scielo_records_metadata")
    compiled_file = os.path.join(f"/storage/temp/scielov2/{country}", "scielo_records_metadata_compiled.csv")
    if not os.path.exists(folder):
        print(f"No existe la carpeta de artículos para {country}.")
        return
    all_rows = []
    all_fieldnames = set()
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            magazine_id = filename[len("records_metadata_"):-len(".csv")]
            filepath = os.path.join(folder, filename)
            with open(filepath, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if not row.get("setSpec", "").strip():
                        row["setSpec"] = magazine_id
                    all_rows.append(row)
                    all_fieldnames.update(row.keys())
    all_fieldnames = list(all_fieldnames)
    with open(compiled_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)
    print(f"Compiled articles CSV creado: {compiled_file}")

def create_states_file(country):
    """
    A partir del CSV compilado (scielo_records_metadata_compiled.csv) se genera (o actualiza)
    el archivo de estados: scielo_{country}_records_states.csv.
    Se incluyen las columnas: setSpec, id, path_xml, path_pdf, path_txt.
    En esta versión se actualiza cada registro usando el CSV compilado, de forma que 
    todo registro tenga su setSpec correspondiente, incluso si ya existían de ejecuciones anteriores.
    """
    compiled_csv = os.path.join(f"/storage/temp/scielov2/{country}", "scielo_records_metadata_compiled.csv")
    state_csv_file = os.path.join(f"/storage/temp/scielov2/{country}", f"scielo_{country}_records_states.csv")
    
    # 1. Cargar las entradas del CSV compilado en un diccionario (clave: identifier)
    compiled_entries = {}
    if os.path.exists(compiled_csv):
        with open(compiled_csv, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rec_id = row.get("identifier", "").strip()
                spec = row.get("setSpec", "").strip()
                # Si por alguna razón el setSpec sigue vacío, se asigna "general"
                if not spec:
                    spec = "general"
                    row["setSpec"] = spec
                compiled_entries[rec_id] = row
    else:
        print(f"No se encontró el CSV compilado: {compiled_csv}")
    
    # 2. Cargar las entradas existentes del archivo de estados en un diccionario (clave: id)
    state_entries = {}
    if os.path.exists(state_csv_file):
        with open(state_csv_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rec_id = row.get("id", "").strip()
                state_entries[rec_id] = row
    
    # 3. Actualizar (o agregar) las entradas a partir del CSV compilado
    # Se usa el identificador (identifier en el CSV compilado y "id" en el estado) como clave
    for rec_id, comp_row in compiled_entries.items():
        spec = comp_row.get("setSpec", "general").strip()
        if rec_id in state_entries:
            # Actualizar el setSpec del registro en el estado, incluso si ya existía
            state_entries[rec_id]["setSpec"] = spec
        else:
            state_entries[rec_id] = {
                "setSpec": spec,
                "id": rec_id,
                "path_xml": "",
                "path_pdf": "",
                "path_txt": ""
            }
    
    # 4. Revisar cualquier entrada restante en state_entries que pudiera no tener setSpec
    for rec_id, state_row in state_entries.items():
        if not state_row.get("setSpec", "").strip():
            state_row["setSpec"] = "general"
    
    # 5. Escribir el archivo de estados actualizado
    fieldnames = ["setSpec", "id", "path_xml", "path_pdf", "path_txt"]
    with open(state_csv_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in state_entries.values():
            writer.writerow(entry)
    print(f"Archivo de estados actualizado y guardado: {state_csv_file}")


def update_magazine_stats(output_folder, country, magazine_rows, magazine_csv_file):
    """
    Toma el archivo de estados (scielo_{country}_records_states.csv), cuenta los registros por revista
    (usando el campo setSpec) y actualiza la columna 'articles' en el CSV de revistas.
    Nota: en el CSV de revistas el identificador principal es 'issn' (del front),
    pero la API almacena el identificador en 'issn_api'. Se asume que en el archivo de estados,
    el campo setSpec corresponde al valor de 'issn_api'.
    """
    import os
    states_file = os.path.join(output_folder, f"scielov2/{country}/scielo_{country}_records_states.csv")
    if os.path.exists(states_file):
        counts = {}
        # Contar la cantidad de registros (artículos) por setSpec en el archivo de estados.
        with open(states_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                spec = row.get("setSpec", "").strip()
                if not spec:
                    spec = "general"
                counts[spec] = counts.get(spec, 0) + 1

        # Construir un mapeo de issn_api a la clave usada en magazine_rows.
        # En magazine_rows, la clave es el valor de 'issn' (del front), pero cada registro tiene también
        # la columna 'issn_api'. Usaremos ese valor para hacer la correspondencia.
        api_to_key = {}
        for key, row in magazine_rows.items():
            api_val = row.get("issn", "").strip()
            if api_val:
                api_to_key[api_val] = key

        # Actualizar magazine_rows: para cada setSpec (del estado) si coincide con algún issn_api,
        # se actualiza la cantidad de artículos.
        for spec, cnt in counts.items():
            if spec in api_to_key:
                key = api_to_key[spec]
                magazine_rows[key]["articles"] = str(cnt)
            else:
                # Opcional: si por algún motivo el valor de setSpec coincide con el front (issn)
                # se actualiza de esa forma.
                if spec in magazine_rows:
                    magazine_rows[spec]["articles"] = str(cnt)
        update_magazine_csv(magazine_rows, magazine_csv_file)
        print("CSV de revistas actualizado usando la cantidad de artículos únicos del archivo de estados.")
    else:
        print(f"No se encontró el archivo de estados: {states_file}")

@task(map_index_template="{{ country_magazines['country'] }}")
def get_records_list(country_magazines):
    """
    Procesa el listado de revistas para un país dado:
      1. Para cada revista se descarga la metadata (artículos) y se guarda un CSV individual.
      2. Se crea/actualiza el CSV de revistas (scielo_{country}_magazines.csv) sin asignar aún la cantidad de artículos.
      3. Si se vuelve a ejecutar el script y el CSV individual de registros existe y contiene registros, se salta el procesamiento de esa revista.
      4. Si ocurre algún problema al procesar una revista, se asigna 0 en "articles" para esa revista.
      5. Una vez procesados todos los magazines y creados sus CSV, se compilan los artículos y se crea (o actualiza)
         el archivo de estados a partir del CSV compilado.
      6. Finalmente, se actualiza el CSV de revistas usando la cantidad de artículos únicos (sin duplicados)
         contados en el archivo de estados, y se actualiza el listado original (magazine_list_data) con el nuevo setSpec.
    """
    magazine_list = country_magazines["magazines"]
    country = country_magazines["country"]
    records_list_path = env["paths"]["get_records_path"]
    scielo_country_path = env["scielo-path"][country]
    
    total_magazines = len(magazine_list)
    
    output_folder = f"/storage/temp/scielov2/{country}"
    magazine_csv_file = os.path.join(output_folder, f"scielo_{country}_magazines.csv")
    magazine_rows = {}
    if os.path.exists(magazine_csv_file):
        with open(magazine_csv_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                key = row.get("issn", "").strip()
                magazine_rows[key] = row
    else:
        for magazine in magazine_list:
            key = magazine.get("setSpec")
            magazine_rows[key] = {"issn": key, "magazine_name": magazine.get("magazine_name"), "articles": "0"}
    
    for idx, magazine in enumerate(magazine_list, start=1):
        setSpec = magazine["issn"]
        csv_folder_records = f"/storage/temp/scielov2/{country}/scielo_records_metadata"
        magazine_csv_filename = os.path.join(csv_folder_records, f"records_metadata_{setSpec}.csv")
        # Verificar si el CSV individual existe y contiene registros (excluyendo la cabecera)
        if os.path.exists(magazine_csv_filename):
            with open(magazine_csv_filename, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                distinct_ids = { row["identifier"].strip() for row in reader if row.get("identifier") and row["identifier"].strip() }
                record_count = len(distinct_ids)
                if record_count > 0:
                    # Actualiza la cantidad de artículos para esa revista
                    magazine_rows[setSpec]["articles"] = str(record_count)
                    # Vuelve a escribir el CSV de revistas para reflejar el nuevo conteo
                    update_magazine_csv(magazine_rows, magazine_csv_file)
                    print(f"[{idx}/{total_magazines}] Saltando revista {magazine['magazine_name']} (ID: {setSpec}) ya procesada con {record_count} artículos")
                    continue

        print(f"[{idx}/{total_magazines}] Procesando registros de la revista: {magazine['magazine_name']} (ID: {setSpec})")
        try:
            records = process_records_for_magazine(scielo_country_path, records_list_path, magazine, country)
        except Exception as e:
            print(f"Error procesando la revista {magazine['magazine_name']} (ID: {setSpec}): {e}")
            magazine_rows[setSpec]["articles"] = "0"
            update_magazine_csv(magazine_rows, magazine_csv_file)
            continue
        
        if os.path.exists(magazine_csv_filename):
            with open(magazine_csv_filename, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                distinct_ids = { row["identifier"].strip() for row in reader if row.get("identifier") and row["identifier"].strip() }
            record_count = len(distinct_ids)
            # Actualiza la cantidad de artículos para esa revista
            magazine_rows[setSpec]["articles"] = str(record_count)
            # Vuelve a escribir el CSV de revistas para reflejar el nuevo conteo
            update_magazine_csv(magazine_rows, magazine_csv_file)
            print(f"Actualizado '{magazine['magazine_name']}' con {record_count} artículos distintos.")
    
    compile_articles_csv(country)
    create_states_file(country)
    update_magazine_stats(output_folder, country, magazine_rows, magazine_csv_file)
    return {"magazines": magazine_list, "country": country}
