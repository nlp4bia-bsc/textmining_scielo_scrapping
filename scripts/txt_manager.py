import os
import csv
import json
import re
import pymupdf4llm
import markdown
from bs4 import BeautifulSoup
from airflow.decorators import task  # type: ignore
from textmining_scielo_scrapping.environment import env  # type: ignore

# Función para extraer texto desde el JSON
def extract_body_text(json_file):
    """Extrae y limpia el texto de cualquier clave 'body' en un JSON."""
    def clean_html(html):
        return BeautifulSoup(html, "html.parser").get_text(separator=" ").strip()
    
    def find_body(data):
        extracted_texts = []
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "body":
                    if isinstance(value, str):
                        extracted_texts.append(clean_html(value))
                    elif isinstance(value, list):
                        extracted_texts.extend(clean_html(str(item)) for item in value)
                    elif isinstance(value, dict):
                        extracted_texts.append(clean_html(json.dumps(value)))
                else:
                    extracted_texts.extend(find_body(value))
        elif isinstance(data, list):
            for item in data:
                extracted_texts.extend(find_body(item))
        return extracted_texts

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    results = find_body(data)
    return "\n".join(results) if results else None

# Función para extraer texto desde un PDF
def extract_text_from_pdf(pdf_path):
    try:
        md_text = pymupdf4llm.to_markdown(pdf_path, show_progress=False)
        html_content = markdown.markdown(md_text)
        plain_text = re.sub(r'<[^>]+>', '', html_content)
        return plain_text.strip()
    except Exception as e:
        print(f"Error extracting text from PDF {pdf_path}: {e}")
        return None

# Funciones de manejo de estado (asumidas existentes)
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

@task(map_index_template="{{ metadata['country'] }}")
def get_txt(metadata):
    country = metadata["country"]
    output_folder = f"/storage/temp/scielov2/{country}"
    state_file = os.path.join(output_folder, f"scielo_{country}_records_states.csv")
    
    if not os.path.exists(state_file):
        print(f"No se encontró el archivo de estado {state_file}.")
        return {"status": "Error", "message": "No state file found."}
    
    state_rows = list_state_rows(state_file)
    total = len(state_rows)
    print(f"Total de registros en estado: {total}")
    
    txt_folder = os.path.join(output_folder, "scielo_records_txt")
    os.makedirs(txt_folder, exist_ok=True)
    
    updated_count = 0
    for idx, row in enumerate(state_rows, start=1):
        article_id = row.get("id", "").strip()
        print(f"Procesando artículo {idx}/{total} (ID: {article_id})")
        current_txt_path = row.get("path_txt", "").strip()
        
        # Si ya existe el TXT, se salta el artículo
        if current_txt_path and os.path.exists(os.path.join(output_folder, current_txt_path)):
            print(f"Saltando artículo {article_id} – TXT ya existe.")
            print(f"Progreso: {idx}/{total} artículos procesados.")
            continue
        
        current_xml_path = row.get("path_xml", "").strip()
        json_filepath = os.path.join(output_folder, current_xml_path) if current_xml_path else None
        extracted_text = None
        
        # Intentamos extraer el texto desde el JSON
        if json_filepath and os.path.exists(json_filepath):
            extracted_text = extract_body_text(json_filepath)
            if extracted_text:
                print(f"Texto extraído del JSON para artículo {article_id}.")
            else:
                print(f"No se pudo extraer texto del JSON para artículo {article_id}.")
        
        # Si no se obtuvo texto desde el JSON, se intenta con el PDF
        if extracted_text is None:
            current_pdf_path = row.get("path_pdf", "").strip()
            pdf_filepath = os.path.join(output_folder, current_pdf_path) if current_pdf_path else None
            if pdf_filepath and os.path.exists(pdf_filepath):
                extracted_text = extract_text_from_pdf(pdf_filepath)
                if extracted_text:
                    print(f"Texto extraído del PDF para artículo {article_id}.")
                else:
                    print(f"No se pudo extraer texto del PDF para artículo {article_id}.")
        
        # Si se extrajo texto, se guarda en un archivo TXT
        if extracted_text:
            txt_filename = f"{article_id}.txt"
            txt_filepath = os.path.join(txt_folder, txt_filename)
            with open(txt_filepath, "w", encoding="utf-8") as f:
                f.write(extracted_text)
            row["path_txt"] = os.path.relpath(txt_filepath, output_folder)
            updated_count += 1
            print(f"Archivo TXT creado para artículo {article_id}.")
        else:
            print(f"No se extrajo texto para el artículo {article_id}; se omite.")
            
        print(f"Progreso: {idx}/{total} artículos procesados.")
    
    save_state_rows(state_file, state_rows)
    print(f"Procesamiento de TXT completado. {updated_count} artículos actualizados de {total}.")
    return {"status": "complete", "updated": updated_count, "total": total, "country": country}
