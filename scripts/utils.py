import xmltodict  # type: ignore
import json


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


def load_json(file_path):
    """
    Carga un archivo JSON y lo devuelve como un diccionario.
    :param file_path: Ruta del archivo JSON.
    :return: Diccionario con los datos del JSON.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error al cargar el JSON: {e}")
        return None
