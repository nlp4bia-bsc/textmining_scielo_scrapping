import os
import requests
import xml.etree.ElementTree as ET
import json
import re 

def list_files_without_extension(directory):
    files = []
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            files.append(os.path.splitext(filename)[0])
    return sorted(files)

# fetch_xml_and_convert_to_json --- TODO he cambiado el encoding para aceptar también el ISO
def fetch_xml_and_convert_to_json(file_name):
    url = f"https://scielo.isciii.es/scieloOrg/php/articleXML.php?pid={file_name}"
    response = requests.get(url)
    if response.status_code == 200 and 'xml' in response.headers.get('Content-Type', ''):
        # Check if encoding is specified in the headers, default to 'utf-8'
        content_type = response.headers.get('Content-Type', '').lower()
        encoding = 'utf-8'
        if 'iso-8859-1' in content_type:
            encoding = 'iso-8859-1'
        # Decode content based on detected encoding
        xml_content = response.content.decode(encoding)
        # Remove duplicate XML declaration if present
        xml_content = re.sub(r'^<\?xml.*?\?>', '', xml_content, count=1).encode('utf-8')
        try:
            root = ET.fromstring(xml_content)
            return xml_to_dict(root)
        except ET.ParseError as e:
            print(f"Failed to parse XML for {file_name}: {e}")
            print(f"Response content: {xml_content[:200]}...")  # Debugging content
            return None
    else:
        print(f"Failed to fetch XML for {file_name}, status code: {response.status_code}, content type: {response.headers.get('Content-Type')}")
        return None


def xml_to_dict(element):
    def inner_func(element):
        children = list(element)
        if not children:
            return element.text
        result = {}
        for child in children:
            result[child.tag] = inner_func(child)
        return result
    return {element.tag: inner_func(element)}

def save_json_to_file(data, output_path):
    with open(output_path, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def main(input_directory, output_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    files = list_files_without_extension(input_directory)
    total_files = len(files)

    for index, file_name in enumerate(files):
        json_data = fetch_xml_and_convert_to_json(file_name)
        if json_data:
            output_path = os.path.join(output_directory, f"{file_name}.json")
            save_json_to_file(json_data, output_path)

        # Calculate and print the progress
        progress = (index + 1) / total_files * 100
        print(f"Progress: {progress:.2f}% ({index + 1}/{total_files}) - {file_name}")

if __name__ == "__main__":
    input_directory = "/data/str/temp/scielo_metadata/costa-rica_scielo_records_txt"
    output_directory = "/data/str/temp/scielo_metadata/costa-rica_scielo_records_xml"
    main(input_directory, output_directory)
