import xmltodict

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
        print(f"Error al convertir XML a dict: {e}")
        return None