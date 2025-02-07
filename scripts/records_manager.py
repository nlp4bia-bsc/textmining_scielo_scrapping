import os
import csv
import json
import requests
import urllib.parse

from airflow.decorators import task  # type: ignore

from textmining_scielo_scrapping.environment import env  # type: ignore
from textmining_scielo_scrapping.scripts.utils import xml_string_to_dict, load_json  # type: ignore

scielo_api = env["scielo-api-path"]["xml"]
scielo_front = env["scielo-api-path"]["front"]

# Se asume que ya tienes definidas en tu entorno:
# - env: diccionario de configuración (con "paths", "scielo-path", "headers", etc.)
# - scielo_api: cadena con el endpoint OAI-PMH (por ejemplo, "oai/request")
# - xml_string_to_dict(texto_xml): función que convierte el XML a diccionario

def extract_pid(url):
    """
    Extrae el valor del parámetro 'pid' de la URL.
    Si no se encuentra, retorna una cadena vacía.
    """
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
            
            # Si existe resumptionToken se continúa; de lo contrario se sale del ciclo
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
      1. Descarga todos los registros (artículos) mediante OAI-PMH.
      2. Agrega a cada registro la clave "articles" con el total de artículos para esa revista.
         (Si no se encuentran registros, se asigna 0.)
      3. Genera un CSV para la revista con la metadata en:
         /storage/temp/scielov2/{country}/scielo_records_metadata/records_metadata_{magazine_id}.csv
         
    Retorna la lista de registros (artículos) procesados.
    """
    magazine_id = magazine["setSpec"]
    records = get_all_records_from_magazine(scielo_country_path, records_list_path, magazine_id)
    
    if not records:
        print(f"No se encontraron registros para {magazine['setName']}. Se asigna 0 artículos.")
        records = []  # Aseguramos que sea una lista
    total_articles = len(records)
    for record in records:
        record["articles"] = total_articles

    # Guardar archivo CSV para la revista en la carpeta scielo_records_metadata
    csv_folder = f"/storage/temp/scielov2/{country}/scielo_records_metadata"
    os.makedirs(csv_folder, exist_ok=True)
    csv_filename = os.path.join(csv_folder, f"records_metadata_{magazine_id}.csv")
    all_keys = set()
    for record in records:
        all_keys.update(record.keys())
    fieldnames = list(all_keys)
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
         writer.writeheader()
         for record in records:
             writer.writerow(record)
    print(f"CSV guardado para la revista: {csv_filename}")
    return records

def update_magazine_csv(country, magazine_rows, magazine_csv_file):
    """
    Reescribe el CSV de revistas usando el diccionario magazine_rows.
    Cada registro debe tener al menos las columnas: issn, magazine_name y articles.
    """
    if magazine_rows:
        # Se obtienen los fieldnames basados en la primera entrada (o se definen)
        first_key = next(iter(magazine_rows))
        fieldnames = list(magazine_rows[first_key].keys())
    else:
        fieldnames = ["issn", "magazine_name", "articles"]
    with open(magazine_csv_file, "w", newline="", encoding="utf-8") as csvfile:
         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
         writer.writeheader()
         for row in magazine_rows.values():
             writer.writerow(row)

def create_states_file(country):
    """
    Itera sobre los CSV de cada revista en la carpeta scielov2/{country}/scielo_records_metadata y
    genera (o actualiza) el archivo de estados: records_{country}_states.csv.
    Se incluyen las columnas: setSpec, id, path_xml, path_pdf, path_txt.
    Si el archivo de estados ya existe, se mantienen las entradas existentes y se agregan únicamente
    aquellas nuevas (evitando duplicados usando la clave (setSpec, id)).
    """
    records_folder = f"/storage/temp/scielov2/{country}/scielo_records_metadata"
    state_csv_file = os.path.join(f"/storage/temp/scielov2/{country}", f"scielo_{country}_records_states.csv")
    
    # Cargar entradas existentes si el archivo de estados ya existe
    state_entries = {}
    if os.path.exists(state_csv_file):
        with open(state_csv_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                key = (row.get("setSpec", "").strip(), row.get("id", "").strip())
                state_entries[key] = row

    # Iterar por los CSV de cada revista y agregar entradas nuevas
    if os.path.exists(records_folder):
        for filename in os.listdir(records_folder):
            if filename.endswith(".csv"):
                filepath = os.path.join(records_folder, filename)
                with open(filepath, newline="", encoding="utf-8") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        setSpec = row.get("setSpec", "").strip()
                        rec_id = row.get("identifier", "").strip()
                        key = (setSpec, rec_id)
                        if key not in state_entries:
                            state_entries[key] = {
                                "setSpec": setSpec,
                                "id": rec_id,
                                "path_xml": "",
                                "path_pdf": "",
                                "path_txt": ""
                            }
    
    # Escribir (o reescribir) el archivo de estados con la unión de entradas existentes y nuevas
    fieldnames = ["setSpec", "id", "path_xml", "path_pdf", "path_txt"]
    with open(state_csv_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in state_entries.values():
            writer.writerow(entry)
    print(f"Archivo de estados guardado: {state_csv_file}")

@task(map_index_template="{{ country_magazines['country'] }}")
def get_records_list(country_magazines):
    """
    Procesa el listado de revistas para un país dado:
      1. Para cada revista se descarga la metadata (artículos) y se guarda un CSV individual.
      2. Inmediatamente se actualiza el CSV de revistas (scielo_{country}_magazines.csv) asignando
         la cantidad de artículos en la columna "articles" para cada revista procesada.
      3. Si se vuelve a ejecutar el script y en el CSV de revistas la columna "articles" ya tiene un valor
         mayor que 0 Y existe el CSV de artículos respectivo, se saltan esas revistas.
      4. Si ocurre cualquier problema al procesar una revista, se detiene el proceso y a esa revista se le asigna 0 en "articles".
      5. Una vez procesados todos los magazines y creados sus CSV, se llama a la función que itera por estos archivos
         para crear (o actualizar) el archivo de estados.
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
            magazine_rows[key] = {"issn": key, "magazine_name": magazine.get("setName"), "articles": "0"}
    
    for idx, magazine in enumerate(magazine_list, start=1):
        setSpec = magazine["setSpec"]
        current_articles = magazine_rows.get(setSpec, {}).get("articles")
        csv_folder_records = f"/storage/temp/scielov2/{country}/scielo_records_metadata"
        magazine_csv_filename = os.path.join(csv_folder_records, f"records_metadata_{setSpec}.csv")
        try:
            # Si ya existe un valor mayor que 0 Y el CSV de artículos existe, se salta el procesamiento.
            if (current_articles and current_articles.strip() != "" and int(current_articles) > 0 
                    and os.path.exists(magazine_csv_filename)):
                print(f"[{idx}/{total_magazines}] Saltando revista {magazine['setName']} (ID: {setSpec}) ya procesada con {current_articles} artículos")
                continue
        except ValueError:
            pass
        
        print(f"[{idx}/{total_magazines}] Procesando registros de la revista: {magazine['setName']} (ID: {setSpec})")
        try:
            records = process_records_for_magazine(scielo_country_path, records_list_path, magazine, country)
            count = len(records) if records else 0
            magazine_rows[setSpec]["articles"] = str(count)
        except Exception as e:
            print(f"Error procesando la revista {magazine['setName']} (ID: {setSpec}): {e}")
            magazine_rows[setSpec]["articles"] = "0"
            update_magazine_csv(country, magazine_rows, magazine_csv_file)
            # Detener el proceso ante cualquier error
            break
        
        update_magazine_csv(country, magazine_rows, magazine_csv_file)
    
    # Una vez procesados todos los magazines, se crea (o actualiza) el archivo de estados.
    create_states_file(country)
    
    return {"country": country}