import os
import csv
import json
import requests
import xml.etree.ElementTree as ET

from airflow.decorators import task  # type: ignore
from textmining_scielo_scrapping.environment import env  # type: ignore

# Se asume que en env["paths"]["get_xml"] se encuentra la parte de la URL para obtener el XML.
get_xml_path = env["paths"]["get_xml"]

def xml_to_dict(element):
    """
    Convierte un elemento XML a un diccionario.
    Si existen múltiples hijos con la misma etiqueta, se agrupan en una lista.
    """
    def inner_func(elem):
        children = list(elem)
        # Si no tiene hijos, se retorna su texto (o None si está vacío)
        if not children:
            return elem.text
        result = {}
        for child in children:
            child_value = inner_func(child)
            # Si la etiqueta ya está presente en result
            if child.tag in result:
                # Si ya es una lista, agregamos el nuevo valor
                if isinstance(result[child.tag], list):
                    result[child.tag].append(child_value)
                else:
                    # Convertimos el valor existente en lista y agregamos el nuevo
                    result[child.tag] = [result[child.tag], child_value]
            else:
                result[child.tag] = child_value
        return result
    return {element.tag: inner_func(element)}


def save_json_to_file(data, output_path):
    """Guarda el diccionario 'data' en formato JSON en output_path."""
    with open(output_path, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def list_state_rows(state_file):
    """Lee el archivo de estado CSV y retorna una lista de diccionarios."""
    rows = []
    with open(state_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append(row)
    return rows

def save_state_rows(state_file, rows):
    """Reescribe el archivo de estado CSV con la lista de diccionarios 'rows'."""
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

def fix_multiple_root(xml_bytes):
    """
    Toma el contenido XML (en bytes) y trata de "arreglarlo" para que se pueda parsear.
    
    1. Detecta la codificación declarada en el XML (por ejemplo, "ISO-8859-1") a partir de los primeros 200 bytes.
    2. Usa esa codificación para crear un parser de lxml con recover=True.
    3. Si la recuperación falla, se decodifica el contenido usando la codificación detectada (o "latin1" en caso de error),
       se eliminan todas las declaraciones XML y se remueven caracteres de control (excepto TAB, LF y CR),
       y se envuelve todo el contenido en un único elemento <root>.
    4. Retorna la cadena XML resultante.
    """
    import re
    # Intentar detectar la codificación leyendo los primeros 200 bytes (decodificados provisionalmente en ascii)
    try:
        header = xml_bytes[:200].decode('ascii', errors='replace')
    except Exception as ex:
        header = ""
    match = re.search(r'encoding=["\'](.*?)["\']', header)
    encoding = match.group(1) if match else "utf-8"
    
    # Primero intentamos con lxml usando la codificación detectada
    try:
        from lxml import etree
        parser = etree.XMLParser(recover=True, encoding=encoding)
        tree = etree.fromstring(xml_bytes, parser=parser)
        fixed_xml = etree.tostring(tree, encoding=encoding).decode(encoding)
        return fixed_xml
    except Exception as e:
        print(f"lxml recovery failed: {e}. Usando fallback.")
        # Fallback: decodificar usando la codificación detectada (o latin1 en caso de error)
        try:
            text = xml_bytes.decode(encoding, errors="replace").strip()
        except Exception as ex:
            print(f"Fallback decoding error: {ex}. Usando latin1.")
            text = xml_bytes.decode("latin1", errors="replace").strip()
        # Eliminar declaraciones XML
        text = re.sub(r'<\?xml[^>]+\?>', '', text).strip()
        # Eliminar caracteres de control (excepto TAB, LF y CR)
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)
        fixed_text = f"<root>{text}</root>"
        return fixed_text

@task(map_index_template="{{ metadata['country'] }}")
def get_xml(metadata):
    """
    Tarea que descarga los XML de los artículos, los convierte a JSON y
    guarda los archivos JSON. Actualiza el archivo de estado (records_{country}_states.csv)
    para que en la columna 'path_xml' se guarde la ruta relativa del JSON descargado.

    Condiciones:
      - Se itera por cada registro del estado.
      - Si 'path_xml' ya tiene un valor y el archivo correspondiente existe en la carpeta,
        se salta el artículo.
      - Si no, se descarga el XML usando la URL construida a partir de env['scielo-path'][country] y get_path_xml.
      - Se transforma el XML a un diccionario (usando xml_to_dict) y se guarda como JSON.
      - Se imprime la URL, el avance y el setSpec del artículo.
      - Si ocurre un error (por ejemplo, fallo en la descarga o en la conversión del XML),
        se detiene el proceso y se actualiza el estado con lo acumulado hasta ese momento.
    """
    country = metadata["country"]
    output_folder = f"/storage/temp/scielov2/{country}"
    
    # Ruta del archivo de estado
    state_file = os.path.join(output_folder, f"scielo_{country}_records_states.csv")
    if not os.path.exists(state_file):
        print(f"No se encontró el archivo de estado {state_file}.")
        return {"status": "Error", "message": "No state file found."}
    
    state_rows = list_state_rows(state_file)
    total = len(state_rows)
    print(f"Total de registros en estado: {total}")
    
    downloaded_count = 0
    # Carpeta para guardar los JSON (aunque la llamamos "scielo_records_xml", contendrá JSON)
    json_folder = os.path.join(output_folder, "scielo_records_xml")
    if not os.path.exists(json_folder):
        os.makedirs(json_folder)
    try:
        for idx, row in enumerate(state_rows, start=1):
            article_id = row.get("id", "").strip()
            setSpec = row.get("setSpec", "").strip()
            current_path = row.get("path_xml", "").strip()
            # Construir la ruta esperada para el archivo JSON
            expected_filename = f"{article_id}.json"
            expected_filepath = os.path.join(json_folder, expected_filename)
            # Si ya hay un valor en path_xml y el archivo existe (usando la ruta relativa), se salta
            if current_path and os.path.exists(os.path.join(output_folder, current_path)):
                print(f"Skipping article {article_id} (setSpec: {setSpec}) – JSON already exists.")
                continue
            # O, si el archivo esperado ya existe, se actualiza la columna y se salta
            if os.path.exists(expected_filepath):
                rel_path = os.path.relpath(expected_filepath, output_folder)
                row["path_xml"] = rel_path
                print(f"Found existing JSON for article {article_id} – updating state.")
                continue
            
            # Construir la URL para descargar el XML
            # Se asume que la URL se construye concatenando: scielo-country-path + "/" + get_path_xml + article_id
            url = f"{env['scielo-path'][country]}/{get_xml_path}{article_id}"
            print(f"Downloading article {idx}/{total} (setSpec: {setSpec}): {url}")
            
            response = requests.get(url, headers=env["headers"])
            if not (response.status_code == 200 and "xml" in response.headers.get("Content-Type", "").lower()):
                response = requests.get(url, headers=env["headers"])

            if response.status_code == 200 and "xml" in response.headers.get("Content-Type", "").lower():
                try:
                    # Intentar parsear el XML normalmente
                    root = ET.fromstring(response.content)
                    data = xml_to_dict(root)
                except ET.ParseError as e:
                    # Si ocurre el error "junk after document element", intentamos arreglarlo
                    print(f"XML parse error for article {article_id}: {e}. Attempting to fix the XML...")
                    fixed_xml = fix_multiple_root(response.content)
                    try:
                        root = ET.fromstring(fixed_xml)
                        data = xml_to_dict(root)
                    except ET.ParseError as e2:
                        print(f"Failed to parse fixed XML for article {article_id}: {e2}. Skipping article.")
                        # break
                        continue # TODO: ver casos especificos
                # Guardar el JSON en expected_filepath
                with open(expected_filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                # Actualizar la columna path_xml con la ruta relativa del JSON
                rel_path = os.path.relpath(expected_filepath, output_folder)
                row["path_xml"] = rel_path
                downloaded_count += 1
            else:
                print(f"Failed to fetch XML for article {article_id}, status code: {response.status_code}, content-type: {response.headers.get('Content-Type', 'None')}")
                print(response.content)
                break  # Se detiene el proceso ante un fallo en la descarga
            print(f"Progress: {idx}/{total} articles processed.")
    except Exception as e:
        print(f"Error during JSON download: {e}")
        # En caso de error, se detiene el proceso y se actualiza el estado
    
    # Actualizar el archivo de estado con las nuevas rutas en path_xml
    save_state_rows(state_file, state_rows)
    
    print(f"JSON download complete. {downloaded_count} articles downloaded out of {total}.")
    return {"status": "complete", "downloaded": downloaded_count, "total": total, "country": country}
