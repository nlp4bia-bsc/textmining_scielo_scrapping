import requests
import sys
import xmltodict  # type: ignore
import json

# Se asume que ya tienes definidas estas funciones/variables en tu entorno:
# - xml_string_to_dict(texto_xml): función que convierte un string XML en un diccionario
env = {
    "scielo-path": {
        "argentina": "http://www.scielo.org.ar",
        "bolivia": "http://www.scielo.org.bo",
        "chile": "https://www.scielo.cl",
        "colombia": "http://www.scielo.org.co",
        "costa-rica": "https://www.scielo.sa.cr",
        "cuba": "http://scielo.sld.cu",
        "ecuador": "http://scielo.senescyt.gob.ec",
        "mexico": "https://www.scielo.org.mx",
        "paraguay": "http://scielo.iics.una.py",
        "peru": "http://www.scielo.org.pe",
        "portugal": "https://www.scielo.pt",
        "spain": "https://scielo.isciii.es",
        "south-africa": "https://scielo.org.za",
        "uruguay": "http://www.scielo.edu.uy",
    },
    "scielo-path-not-php":{
        "brazil": "https://www.scielo.br",
        "public-health": "https://scielosp.org",
    },
    "scielo-api-path":{
        "xml": "oai/scielo-oai.php",
        "front": "scielo.php",
    },
    "paths":{
        "get_magazines_path": "verb=ListSets",
        "get_records_path": "verb=ListRecords",
        "get_pdf": "script=sci_pdf&pid=",
        "get_xml": "scieloOrg/php/articleXML.php?pid=",
        "get_magazines_path_front": "script=sci_alphabetic&lng=es&nrm=iso",
    },
    "headers":{
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }
}

def xml_string_to_dict(xml_string):
    try:
        if isinstance(xml_string, str):
            xml_bytes = xml_string.encode('iso-8859-1')
            xml_string_utf8 = xml_bytes.decode('utf-8')
        else:
            xml_string_utf8 = xml_string.decode('utf-8')

        xml_dict = xmltodict.parse(xml_string_utf8)
        return xml_dict
    except Exception as e:
        error_message = str(e)
        # Solo para este error específico se hace skip, de lo contrario se relanza
        if "not well-formed" in error_message or "invalid token" in error_message:
            print(f"Skipping page due to XML conversion error: {e}")
            return {}
        else:
            print(f"Error al convertir XML a dict: {e}")
            return None

def search_xml_for_string(scielo_country_path, scielo_api, records_list_path, target_string):
    """
    Itera sobre las páginas de registros usando la URL construida a partir de:
       scielo_country_path, scielo_api y records_list_path.
    No se añade ningún parámetro "set" (ya que se busca el setSpec dentro del contenido).
    
    Si se encuentra target_string en la respuesta (response.text), se retorna la URL actual.
    En caso contrario, si existe un resumptionToken se continúa iterando; si no, se termina y se retorna None.
    """
    resumption_token = None
    while True:
        # Se construye la URL sin el parámetro &set
        url = f"{scielo_country_path}/{scielo_api}?{records_list_path}"
        if resumption_token:
            url = f"{url}&resumptionToken={resumption_token}"
        print("Solicitando:", url)
        
        response = requests.get(url, headers=env["headers"])
        if response.status_code != 200:
            print(f"Error {response.status_code} al obtener registros")
            break

        # Si se encuentra el string objetivo en el contenido, se retorna la URL
        if target_string in response.text:
            print(f"Se encontró el string objetivo '{target_string}' en la URL: {url}")
            return url

        # Convertir el XML a diccionario para extraer el token de continuación (resumptionToken)
        records_metadata = xml_string_to_dict(response.text)
        data = records_metadata.get("OAI-PMH", {})
        if "ListRecords" in data:
            token = data["ListRecords"].get("resumptionToken")
            if token:
                if isinstance(token, dict):
                    resumption_token = token.get("#text", "")
                else:
                    resumption_token = token
                if not resumption_token:
                    print("No hay token de resumption; terminando el ciclo.")
                    break
            else:
                print("No se encontró token de resumption; terminando el ciclo.")
                break
        else:
            print("No se encontró 'ListRecords' en la respuesta; terminando el ciclo.")
            break

    return None

if __name__ == "__main__":
    # El script requiere al menos un argumento: el string a buscar
    if len(sys.argv) < 2:
        print("Uso: python script.py <target_string> [<country_key>]")
        sys.exit(1)
    target_string = sys.argv[1]
    if len(sys.argv) >= 3:
        country_key = sys.argv[2]
    else:
        country_key = list(env["scielo-path"].keys())[0]

    scielo_country_path = env["scielo-path"][country_key]
    scielo_api = env["scielo-api-path"]["xml"]
    records_list_path = env["paths"]["get_records_path"]

    found_url = search_xml_for_string(scielo_country_path, scielo_api, records_list_path, target_string)
    if found_url:
        print("Se encontró el string objetivo en la URL:", found_url)
    else:
        print("El string objetivo no se encontró en ninguna página.")
