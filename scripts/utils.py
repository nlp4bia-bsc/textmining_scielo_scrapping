import xmltodict  # type: ignore
import json


# def xml_string_to_dict(xml_string):
#     try:
#         if isinstance(xml_string, str):
#             xml_bytes = xml_string.encode('iso-8859-1')
#             xml_string_utf8 = xml_bytes.decode('utf-8')
#         else:
#             xml_string_utf8 = xml_string.decode('utf-8')

#         xml_dict = xmltodict.parse(xml_string_utf8)
#         return xml_dict
#     except Exception as e:
#         error_message = str(e)
#         # Solo para este error específico se hace skip, de lo contrario se relanza
#         if "not well-formed" in error_message or "invalid token" in error_message:
#             print(f"Skipping page due to XML conversion error: {e}")
#             return {}
#         else:
#             print(f"Error al convertir XML a dict: {e}")
#             return None

def xml_string_to_dict(xml_input):
    import re
    """
    Convierte una cadena XML (o bytes) a un diccionario.
    
    Estrategia:
      1. Si la entrada es bytes, se busca la declaración de codificación en el XML (por ejemplo, encoding="ISO-8859-1")
         y se decodifica usando esa codificación; si falla, se utiliza errors='replace'.
      2. Si la entrada es un string, se intenta re-encodear asumiendo que fue interpretado como ISO-8859-1 y luego decodificar a UTF-8.
         Si eso falla, se usa el string original.
      3. Se parsea el XML con xmltodict.parse.
    
    En caso de error de parseo (por XML mal formado o token inválido) se imprime un mensaje y se retorna {},
    de lo contrario se retorna None.
    """
    try:
        if isinstance(xml_input, bytes):
            # Buscar la codificación declarada en el XML (si existe)
            match = re.search(br'<\?xml.*encoding=["\'](.*?)["\']', xml_input)
            if match:
                declared_encoding = match.group(1).decode('ascii', errors='replace')
            else:
                declared_encoding = 'utf-8'
            try:
                xml_decoded = xml_input.decode(declared_encoding)
            except UnicodeDecodeError:
                xml_decoded = xml_input.decode(declared_encoding, errors='replace')
        elif isinstance(xml_input, str):
            # Se asume que el string pudo haber sido decodificado erróneamente.
            # Se intenta re-encodearlo como ISO-8859-1 y decodificar a UTF-8.
            try:
                xml_bytes = xml_input.encode('iso-8859-1')
                xml_decoded = xml_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # Si falla, se usa el string original
                xml_decoded = xml_input
        else:
            print("Tipo de dato no soportado para xml_input")
            return None

        xml_dict = xmltodict.parse(xml_decoded)
        return xml_dict

    except Exception as e:
        error_message = str(e)
        # Si el error es de XML mal formado o token inválido se saltea la página
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
