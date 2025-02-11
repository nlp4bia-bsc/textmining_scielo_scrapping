import os
import re
import csv
import requests
import xml.etree.ElementTree as ET

from airflow.decorators import task  # type: ignore
from textmining_scielo_scrapping.environment import env  # type: ignore

# Variables globales (asegúrate de tener definidas estas claves en env)
scielo_front = env["scielo-api-path"]["front"]

def download_scielo_article(url, save_path):
    """
    Descarga el contenido de 'url' y lo guarda en 'save_path'.  
    Retorna save_path si la descarga es exitosa, o None en caso de error.
    """
    response = requests.get(url, headers=env["headers"])
    if response.status_code == 200:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as file:
            file.write(response.content)
        return save_path
    else:
        print(f"Error downloading the file: {response.status_code}")
        return None

def extract_window_location(html_text):
    import re
    pattern = r'window.location\s*=\s*"([^"]+)"'
    match = re.search(pattern, html_text)
    if match:
        return match.group(1)
    return None

def list_state_rows(state_file):
    """Lee el archivo de estado CSV y retorna una lista de diccionarios."""
    rows = []
    with open(state_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append(row)
    return rows

def save_state_rows(state_file, rows):
    """Sobrescribe el archivo de estado CSV con la lista de diccionarios 'rows'."""
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = ["setSpec", "id", "path_xml", "path_pdf", "path_txt"]
    with open(state_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Archivo de estado actualizado: {state_file}")

@task(map_index_template="{{ metadata['country'] }}")
def get_pdf(metadata):
    """
    Descarga los PDF de los artículos según el archivo de estado.

    Lógica:
      1. Se lee el archivo de estado (records_{country}_states.csv) del país.
      2. Para cada registro se verifica si la columna 'path_pdf' ya tiene valor y el archivo
         correspondiente existe en la carpeta 'scielo_records_pdf'. Si es así, se salta ese artículo.
      3. Si no, se intenta construir la URL primaria para obtener el PDF usando:
             scielo_country_path + "/" + scielo_front + "?{env['paths']['get_pdf']}" + article_id
         y se descarga la página; se extrae la URL del PDF usando extract_window_location.
      4. Se encapsula el bloque “primario” en un try/except; si ocurre cualquier error (o
         la URL extraída resulta inválida o la descarga falla), se activa el mecanismo secundario:
             a. Se construye la URL fallback:
                   {scielo_country_path}/scielo.php?script=sci_abstract&pid={article_id}&lng=es&nrm=iso
             b. Se descarga esa página y se busca, mediante una expresión regular, el contenido
                del meta tag "citation_pdf_url".
      5. Con la URL del PDF (ya sea primaria o extraída vía fallback) se intenta descargar el PDF
         usando download_scielo_article y se guarda en /storage/temp/scielov2/{country}/scielo_records_pdf
         con el nombre {article_id}.pdf.
      6. Se actualiza la columna 'path_pdf' del registro con la ruta relativa al archivo descargado.
      7. Se imprime el avance.
      8. En caso de error en cualquiera de los pasos, se imprime el error y se continúa con el siguiente artículo.
    """
    country = metadata["country"]
    output_folder = f"/storage/temp/scielov2/{country}"
    pdf_folder = os.path.join(output_folder, "scielo_records_pdf")
    if not os.path.exists(pdf_folder):
        os.makedirs(pdf_folder)
    
    state_file = os.path.join(output_folder, f"scielo_{country}_records_states.csv")
    if not os.path.exists(state_file):
        print(f"No se encontró el archivo de estado {state_file}.")
        return {"status": "Error", "message": "No state file found."}
    
    state_rows = list_state_rows(state_file)
    total = len(state_rows)
    print(f"Total de registros en estado: {total}")
    
    downloaded_count = 0
    scielo_country_path = env["scielo-path"][country]
    scielo_get_path_pdf = env["paths"]["get_pdf"]
    
    for idx, row in enumerate(state_rows, start=1):
        article_id = row.get("id", "").strip()
        setSpec = row.get("setSpec", "").strip()
        expected_filename = f"{article_id}.pdf"
        expected_filepath = os.path.join(pdf_folder, expected_filename)
        
        # Si ya existe PDF (según estado o archivo en disco), se salta
        if row.get("path_pdf", "").strip() and os.path.exists(os.path.join(output_folder, row.get("path_pdf", "").strip())):
            print(f"Skipping article {article_id} (setSpec: {setSpec}) – PDF already exists.")
            continue
        if os.path.exists(expected_filepath):
            rel_path = os.path.relpath(expected_filepath, output_folder)
            row["path_pdf"] = rel_path
            print(f"Found existing PDF for article {article_id} – updating state.")
            continue
        
        pdf_url = None
        # Intentar el método primario: obtener la URL desde la página de descarga
        try:
            primary_url = f"{scielo_country_path}/{scielo_front}?{scielo_get_path_pdf}{article_id}"
            print(f"Downloading article {idx}/{total} (setSpec: {setSpec}) from primary URL: {primary_url}")
            response = requests.get(primary_url, headers=env["headers"], timeout=15)
            if response.status_code != 200:
                raise Exception(f"Primary request returned status code {response.status_code}")
            pdf_url = extract_window_location(response.text)
            if not pdf_url or "None" in pdf_url:
                raise Exception("PDF URL extraction from primary response failed")
            # Intentar descargar el PDF usando la URL primaria
            downloaded_pdf = download_scielo_article(pdf_url, expected_filepath)
            if not downloaded_pdf:
                raise Exception("PDF download using primary URL failed")
            print(f"Primary PDF download succeeded for article {article_id}")
        except Exception as primary_error:
            print(f"Primary PDF method failed for article {article_id}: {primary_error}")
            # Intentar el método fallback: usar la página de abstract
            fallback_url = f"{scielo_country_path}/scielo.php?script=sci_abstract&pid={article_id}"
            print(f"Attempting fallback URL for article {article_id}: {fallback_url}")
            try:
                fallback_response = requests.get(fallback_url, headers=env["headers"], timeout=15)
                if fallback_response.status_code != 200:
                    raise Exception(f"Fallback request returned status code {fallback_response.status_code}")
                # Buscar meta tag citation_pdf_url en la respuesta fallback
                meta_pattern = r'<meta\s+[^>]*name=["\']citation_pdf_url["\'][^>]*content=["\']([^"\']+)["\']'
                meta_match = re.search(meta_pattern, fallback_response.text, re.IGNORECASE)
                if meta_match:
                    pdf_url = meta_match.group(1)
                    print(f"Extracted PDF URL from fallback: {pdf_url}")
                else:
                    raise Exception("Could not extract PDF URL from fallback page")
                downloaded_pdf = download_scielo_article(pdf_url, expected_filepath)
                if not downloaded_pdf:
                    raise Exception("PDF download using fallback URL failed")
                print(f"Fallback PDF download succeeded for article {article_id}")
            except Exception as fallback_error:
                print(f"Fallback method failed for article {article_id}: {fallback_error}")
                continue  # Pasar al siguiente artículo
        
        # Si se llegó hasta acá, la descarga fue exitosa (ya sea primaria o fallback)
        rel_path = os.path.relpath(expected_filepath, output_folder)
        row["path_pdf"] = rel_path
        downloaded_count += 1
        print(f"Progress: {idx}/{total} articles processed.")

    # Actualizar el archivo de estado con las nuevas rutas en path_pdf
    save_state_rows(state_file, state_rows)
    print(f"PDF download complete. {downloaded_count} articles downloaded out of {total}.")
    return {"status": "complete", "downloaded": downloaded_count, "total": total, "country": country}
