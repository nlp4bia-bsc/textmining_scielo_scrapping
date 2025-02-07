import csv
import re
import requests  # type: ignore
import os

from airflow.decorators import task  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
from urllib.parse import urlparse, parse_qs

from textmining_scielo_scrapping.environment import env  # type: ignore
from textmining_scielo_scrapping.scripts.utils import xml_string_to_dict, load_json  # type: ignore

scielo_api = env["scielo-api-path"]["xml"]
scielo_front = env["scielo-api-path"]["front"]


def get_magazine_details(set_spec, country):
    """
    Dado el identificador (setSpec o ISSN) de una revista, construye la URL
    y extrae los siguientes datos:
      - issn (se busca el ISSN de la versión impresa, si existe)
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

    # 1. Publisher: se encuentra en el <strong class="journalTitle">
    publisher = None
    if journal_info:
        strong = journal_info.find("strong", class_="journalTitle")
        if strong:
            publisher = strong.get_text(strip=True)

    # 2. ISSN: se encuentra en el <span class="issn">
    #    Buscamos el ISSN de la "versión impresa". Si no se encuentra,
    #    usamos el primero que aparezca.
    issn = None
    if journal_info:
        issn_span = journal_info.find("span", class_="issn")
        if issn_span:
            # Obtenemos el texto completo del span
            text_issn = issn_span.get_text(" ", strip=True)
            # Buscar la parte correspondiente a la versión impresa
            m = re.search(r"versi[oó]n\s*impresa.*?ISSN\s*([\d-]+)", text_issn, re.IGNORECASE)
            if m:
                issn = m.group(1)
            else:
                # Si no se encuentra la versión impresa, se toma el primer ISSN encontrado
                m = re.search(r"ISSN\s*([\d-]+)", text_issn)
                if m:
                    issn = m.group(1)

    # 3. Description (Misión): se ubica generalmente después de un <small> que contenga "Misión"
    description = None
    if journal_info:
        small_mision = journal_info.find("small", string=lambda t: t and "Misión" in t or "Mission")
        if small_mision:
            # Suponemos que el siguiente párrafo <p> contiene la misión
            p = small_mision.find_next("p")
            if p:
                description = p.get_text(" ", strip=True)

    # --- Extraer magazine_name (nombre de la revista) ---
    # Por ejemplo, usando el atributo alt del logo de la revista.
    magazine_name = None
    journal_logo = soup.find("div", class_="journalLogo")
    if journal_logo:
        img = journal_logo.find("img")
        if img and img.has_attr("alt"):
            magazine_name = img["alt"].strip()

    # --- Extraer link_magazine (sitio oficial) ---
    # Buscamos en la sección de la columna izquierda el enlace cuyo texto contenga
    # "sitio de la revista".
    link_magazine = None
    left_col = soup.find("div", class_="leftCol")
    if left_col:
        a_tag = left_col.find("a", string=lambda t: t and "sitio de la revista" in t.lower())
        if a_tag and a_tag.has_attr("href"):
            link_magazine = a_tag["href"].strip()

    return {
        "issn": issn,
        "magazine_name": magazine_name,
        "description": description,
        "publisher": publisher,
        "link_magazine": link_magazine
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
    # URL de la página que contiene el listado
    print("Error getting from API...")
    print("Getting list from front")
    get_magazines_path_front = env["paths"]["get_magazines_path_front"]
    scielo_country_path = env["scielo-path"][country]
    url = f"{scielo_country_path}/{scielo_front}?{get_magazines_path_front}"
    print(url)
    # Se descarga la página
    respuesta = requests.get(url)
    respuesta.raise_for_status()  # Lanza una excepción si la descarga falla
    html = respuesta.text

    # Se parsea el HTML con BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    
    revistas = []
    
    # Buscamos todos los enlaces que tengan en su URL 'script=sci_serial'
    for enlace in soup.find_all('a', href=True):
        href = enlace['href']
        if 'script=sci_serial' in href:
            # Se analiza la URL para extraer los parámetros de la query string
            partes = urlparse(href)
            parametros = parse_qs(partes.query)
            # Se extrae el valor del parámetro "pid" (si existe)
            pid = parametros.get('pid', [None])[0]
            # Se obtiene el texto del enlace, que es el nombre de la revista
            nombre = enlace.get_text(strip=True)
            
            revistas.append({
                'setSpec': pid,
                'name': nombre
            })
    
    return {"magazines": revistas, "country": country}


def get_magazine_list(country):
    try:
        deprecated_path = f"/storage/temp/scielo_metadata/{country}_magazines.json"
        if os.path.exists(deprecated_path):
            return load_json(deprecated_path)
        magazine_list_path = env["paths"]["get_magazines_path"]
        scielo_country_path = env["scielo-path"][country]
        url = f"{scielo_country_path}/{scielo_api}?{magazine_list_path}"
        print(url)
        response = requests.get(url, headers=env["headers"])
        magazines_metadata = xml_string_to_dict(response.text)

        print(f"Response: {response}")

        magazines_sets_list = transform_array(magazines_metadata["OAI-PMH"]["ListSets"]["set"])

        return {"magazines": magazines_sets_list, "country": country}
    except:
        return get_magazine_from_front(country)


def generate_csv(magazines_details, output_file):
    """
    Genera un archivo CSV a partir de la lista de diccionarios con los datos de cada revista.
    Las columnas serán: issn, magazine_name, description, publisher, link_magazine.
    """
    fieldnames = ["issn", "magazine_name", "description", "publisher", "link_magazine"]
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for md in magazines_details:
            # Se puede agregar aquí validación de que md no sea None
            if md:
                writer.writerow(md)


@task(map_index_template="{{ country }}")
def process_magazines(country):
    """
    Función principal que:
      1. Obtiene el listado de revistas para el país indicado (usando tu función get_magazine_list).
      2. Para cada revista, extrae los detalles de la página de SciELO.
      3. Genera el CSV con los datos recolectados.
    """
    output_folder = f"/storage/temp/scielov2/{country}"
    output_file = f"{output_folder}/scielo_{country}_magazines.csv"

    if os.path.exists(output_file):
        print(f"Magazines metadata downloaded in: {output_file}")
        magazines_sets_list = []
        with open(output_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                magazines_sets_list.append({
                    "setSpec": row.get("issn", "").strip(),
                    "setName": row.get("magazine_name", "").strip()
                })
        return {"magazines": magazines_sets_list, "country": country}

    magazine_list_data = get_magazine_list(country)
    magazines = magazine_list_data.get("magazines", [])

    magazines_details = []
    for magazine in magazines:
        set_spec = magazine.get("setSpec")
        if not set_spec:
            continue
        print(f"Procesando revista {set_spec} ...")
        details = get_magazine_details(set_spec, country)
        if details:
            magazines_details.append(details)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    generate_csv(magazines_details, output_file)
    print(f"CSV generado: {output_file}")
    return magazine_list_data

