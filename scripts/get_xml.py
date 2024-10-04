import os
import requests
import xml.etree.ElementTree as ET
import json

def list_files_without_extension(directory):
    files = []
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            files.append(os.path.splitext(filename)[0])
    return sorted(files)

def fetch_xml_and_convert_to_json(file_name):
    url = f"https://scielo.isciii.es/scieloOrg/php/articleXML.php?pid={file_name}"
    response = requests.get(url)
    if response.status_code == 200 and 'xml' in response.headers.get('Content-Type', ''):
        xml_content = response.content
        try:
            root = ET.fromstring(xml_content)
            return xml_to_dict(root)
        except ET.ParseError as e:
            print(f"Failed to parse XML for {file_name}: {e}")
            print(f"Response content: {xml_content[:200]}...")  # Print the first 200 characters for debugging
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
    input_directory = "/Users/pabloarancibiabarahona/Downloads/spain_scielo_records_txt"
    output_directory = "/Users/pabloarancibiabarahona/Downloads/spain_scielo_records_xml"
    main(input_directory, output_directory)
