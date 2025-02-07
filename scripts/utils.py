import xmltodict

def xml_string_to_dict(xml_string):
    try:
        # First, attempt to parse as UTF-8 (most common)
        xml_dict = xmltodict.parse(xml_string)
        return xml_dict
    except UnicodeDecodeError:
        try:
            # If UTF-8 fails, try ISO-8859-1
            xml_dict = xmltodict.parse(xml_string.encode('utf-8').decode('iso-8859-1'))
            return xml_dict
        except UnicodeDecodeError as e:
            print(f"Encoding error: Could not decode XML string with UTF-8 or ISO-8859-1. Error: {e}")
        except Exception as e:
            print(f"General error converting XML to dict: {e}")
    except Exception as e:
        print(f"General error converting XML to dict: {e}")

    return None

