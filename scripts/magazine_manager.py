import csv
import re
import requests  # type: ignore
import os
from urllib.parse import urlparse, parse_qs

from airflow.decorators import task  # type: ignore
from bs4 import BeautifulSoup  # type: ignore

from textmining_scielo_scrapping.environment import env  # type: ignore
from textmining_scielo_scrapping.scripts.utils import xml_string_to_dict, load_json  # type: ignore

scielo_api = env["scielo-api-path"]["xml"]
scielo_front = env["scielo-api-path"]["front"]

def fix_encoding(text):
    """
    Si se detecta que el texto contiene secuencias propias de mala decodificación
    (como 'Ã'), se re-encoda desde latin1 a utf-8.
    """
    if "Ã" in text:
        try:
            return text.encode("latin1").decode("utf-8")
        except Exception:
            pass
    return text

def get_magazine_details(set_spec, country):
    """
    Dado el identificador (setSpec o ISSN) de una revista (del front), construye la URL
    y extrae los siguientes datos:
      - issn (del front)
      - magazine_name (por ejemplo, tomado del atributo alt del logo)
      - description (la misión de la revista)
      - publisher (quién la publica)
      - link_magazine (el enlace al sitio oficial de la revista)
    """
    scielo_country_path = env["scielo-path"][country]
    url = f"{scielo_country_path}/{scielo_front}?script=sci_serial&pid={set_spec}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error al obtener la revista {set_spec}: {response.status_code}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    # --- Extraer datos de la sección "journalInfo" ---
    journal_info = soup.find("div", class_="journalInfo")

    # 1. Publisher: se busca en el <strong class="journalTitle">
    publisher = None
    if journal_info:
        strong = journal_info.find("strong", class_="journalTitle")
        if strong:
            publisher = strong.get_text(strip=True)

    # 2. ISSN: se busca en el <span class="issn">
    issn = None
    if journal_info:
        issn_span = journal_info.find("span", class_="issn")
        if issn_span:
            text_issn = issn_span.get_text(" ", strip=True)
            m = re.search(r"versi[oó]n\s*impresa.*?ISSN\s*([\d-]+)", text_issn, re.IGNORECASE)
            if m:
                issn = m.group(1)
            else:
                m = re.search(r"ISSN\s*([\d-]+)", text_issn)
                if m:
                    issn = m.group(1)

    # 3. Description (Misión)
    description = None
    if journal_info:
        small_mision = journal_info.find("small", string=lambda t: t and ("Misión" in t or "Mission" in t))
        if small_mision:
            p = small_mision.find_next("p")
            if p:
                description = p.get_text(" ", strip=True)

    # --- Extraer magazine_name (nombre de la revista) ---
    magazine_name = None
    journal_logo = soup.find("div", class_="journalLogo")
    if journal_logo:
        img = journal_logo.find("img")
        if img and img.has_attr("alt"):
            magazine_name = fix_encoding(img["alt"].strip())

    # --- Extraer link_magazine (sitio oficial) ---
    link_magazine = None
    left_col = soup.find("div", class_="leftCol")
    if left_col:
        a_tag = left_col.find("a", string=lambda t: t and "sitio de la revista" in t.lower())
        if a_tag and a_tag.has_attr("href"):
            link_magazine = a_tag["href"].strip()

    return {
        "issn": issn if issn else set_spec,  # En caso de no encontrar el ISSN, usamos el set_spec
        "magazine_name": magazine_name,
        "description": description,
        "publisher": publisher,
        "link_magazine": link_magazine,
        "articles": ""  # Valor por defecto, se usará en pasos posteriores
    }

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

def get_magazine_from_front(country):
    """
    Extrae el listado de revistas del front. Cada revista se identifica con:
      - setSpec: PID obtenido desde el front
      - name: nombre de la revista (se corrige la codificación)
    """
    print("Obteniendo listado desde el front...")
    get_magazines_path_front = env["paths"]["get_magazines_path_front"]
    scielo_country_path = env["scielo-path"][country]
    url = f"{scielo_country_path}/{scielo_front}?{get_magazines_path_front}"
    print(url)
    respuesta = requests.get(url)
    respuesta.raise_for_status()
    html = respuesta.text
    soup = BeautifulSoup(html, 'html.parser')
    revistas = []
    for enlace in soup.find_all('a', href=True):
        href = enlace['href']
        if 'script=sci_serial' in href:
            partes = urlparse(href)
            parametros = parse_qs(partes.query)
            pid = parametros.get('pid', [None])[0]
            nombre = fix_encoding(enlace.get_text(strip=True))
            revistas.append({
                'setSpec': pid,
                'name': nombre
            })
    return {"magazines": revistas, "country": country}

def get_magazine_list(country):
    """
    Obtiene el listado de revistas usando la API.
    Se retorna un diccionario con la lista de revistas y el país.
    Cada revista tiene:
      - setSpec (desde la API)
      - setName (nombre de la revista)
    """
    try:
        print("Obteniendo listado desde la API...")
        deprecated_path = f"/storage/temp/scielo_metadata/{country}_magazines.json"
        if os.path.exists(deprecated_path):
            deprecated_list = load_json(deprecated_path)
            deprecated_magazine_list = [item for item in deprecated_list["magazines"] if item["setSpec"] != "openaire"]
            deprecated_list['magazines'] = deprecated_magazine_list
            print(f"Deprecated list: {deprecated_list['magazines']}")
            return deprecated_list
        magazine_list_path = env["paths"]["get_magazines_path"]
        scielo_country_path = env["scielo-path"][country]
        url = f"{scielo_country_path}/{scielo_api}?{magazine_list_path}"
        print(url)
        response = requests.get(url, headers=env["headers"])
        magazines_metadata = xml_string_to_dict(response.text)
        magazines_sets_list = transform_array(magazines_metadata["OAI-PMH"]["ListSets"]["set"])
        magazines_sets_list_filtered = [item for item in magazines_sets_list if item["setSpec"] != "openaire"]
        return {"magazines": magazines_sets_list_filtered, "country": country}
    except Exception as e:
        print("Error obteniendo listado por API:", e)
        return get_magazine_from_front(country)

def generate_csv(magazines_details, output_file):
    """
    Genera el CSV incluyendo las columnas:
      - issn: valor obtenido desde el front.
      - issn_api: valor obtenido desde la API.
      - magazine_name, description, publisher, link_magazine, articles.
    """
    fieldnames = ["issn", "issn_api", "magazine_name", "description", "publisher", "link_magazine", "articles"]
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for md in magazines_details:
            if "articles" not in md:
                md["articles"] = ""
            if "issn_api" not in md:
                md["issn_api"] = ""
            writer.writerow(md)

@task(map_index_template="{{ country }}")
def process_magazines(country):
    """
    Función principal actualizada que:
      1. Carga (si existe) el CSV con metadata de revistas.
      2. Obtiene el listado de revistas desde el front (con setSpec del front)
         y desde la API (con setSpec de la API).
      3. Une ambas fuentes usando el nombre de la revista (normalizado) para asignar:
           - issn: valor del front.
           - issn_api: valor del API.
      4. Si la revista no existe en el CSV, se procesa la metadata extraída del front.
      5. Guarda el CSV actualizado.
    """
    output_folder = f"/storage/temp/scielov2/{country}"
    output_file = f"{output_folder}/scielo_{country}_magazines.csv"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # --- Paso 1: Cargar CSV existente (si lo hay) ---
    existing_details = {}
    if os.path.exists(output_file):
        print(f"CSV encontrado en {output_file}. Cargando metadata existente...")
        with open(output_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Usamos el nombre de la revista normalizado en minúsculas como clave
                magazine_name = fix_encoding(row.get("magazine_name", "").strip())
                if magazine_name:
                    existing_details[magazine_name.lower()] = row
    else:
        print("CSV no encontrado, iniciando con lista vacía.")

    # --- Paso 2: Obtener listados desde el front y la API ---
    front_listing = get_magazine_from_front(country)
    front_magazines = front_listing.get("magazines", [])
    print("Listado obtenido desde el front:", front_magazines)

    api_listing = get_magazine_list(country)
    api_magazines = api_listing.get("magazines", [])
    # Crear diccionario de la API usando el nombre normalizado (setName)
    api_dict = {}
    for mag in api_magazines:
        name_api = fix_encoding(mag.get("setName", "").strip())
        if name_api:
            api_dict[name_api.lower()] = mag.get("setSpec", "").strip()

    # --- Paso 3: Procesar revistas del front y unir datos según el nombre ---
    for front_mag in front_magazines:
        # Extraer y normalizar el nombre del front
        front_name = fix_encoding(front_mag.get("name", "").strip())
        front_name_lower = front_name.lower()
        front_pid = front_mag.get("setSpec", "").strip()  # PID del front

        # Buscar el PID correspondiente del API (si existe) usando el nombre
        api_pid = api_dict.get(front_name_lower, "")

        if front_name_lower not in existing_details:
            print(f"Procesando revista faltante: '{front_name}' - PID front: {front_pid}, PID API: {api_pid}")
            details = get_magazine_details(front_pid, country)
            if details:
                # Usar el nombre extraído del front (ya corregido)
                details["magazine_name"] = front_name
                details["issn"] = front_pid        # ISSN tomado del front
                details["issn_api"] = api_pid        # ISSN obtenido de la API
                existing_details[front_name_lower] = details
            else:
                print(f"Advertencia: no se pudo obtener la metadata de '{front_name}', creando entrada mínima")
                existing_details[front_name_lower] = {
                    "magazine_name": front_name,
                    "issn": front_pid,
                    "issn_api": api_pid,
                    "description": "",
                    "publisher": "",
                    "link_magazine": "",
                    "articles": ""
                }
        else:
            # Si ya existe, actualizar los campos si difieren
            stored_detail = existing_details[front_name_lower]
            if stored_detail.get("issn", "").strip() != front_pid:
                print(f"Actualizando ISSN (front) para '{front_name}': de '{stored_detail.get('issn')}' a '{front_pid}'")
                stored_detail["issn"] = front_pid
            if stored_detail.get("issn_api", "").strip() != api_pid:
                print(f"Actualizando ISSN_API para '{front_name}': de '{stored_detail.get('issn_api')}' a '{api_pid}'")
                stored_detail["issn_api"] = api_pid

    # --- Paso 4: Guardar el CSV actualizado ---
    final_details = list(existing_details.values())
    generate_csv(final_details, output_file)
    print(f"CSV actualizado y guardado en: {output_file}")
    return {"magazines": final_details, "country": country}
