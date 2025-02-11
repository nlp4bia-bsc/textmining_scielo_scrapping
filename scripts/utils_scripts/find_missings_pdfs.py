#!/usr/bin/env python3
import os
import sys
import csv

def get_state_ids(state_file):
    """
    Lee el archivo CSV de estados y retorna un conjunto con los IDs (columna "id").
    """
    state_ids = set()
    try:
        with open(state_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                id_value = row.get("id", "").strip()
                if id_value:
                    state_ids.add(id_value)
    except Exception as e:
        print(f"Error al leer el archivo de estado {state_file}: {e}")
    return state_ids

def get_pdf_file_ids(pdf_folder):
    """
    Lista los archivos PDF en la carpeta y retorna un conjunto con los nombres base (sin extensión).
    """
    pdf_ids = set()
    try:
        for filename in os.listdir(pdf_folder):
            if filename.lower().endswith('.pdf'):
                base_name, _ = os.path.splitext(filename)
                pdf_ids.add(base_name)
    except Exception as e:
        print(f"Error al leer la carpeta de PDF {pdf_folder}: {e}")
    return pdf_ids

def main(country):
    base_folder = f"/data/str/temp/scielov2/{country}"
    state_file = os.path.join(base_folder, f"scielo_{country}_records_states.csv")
    pdf_folder = os.path.join(base_folder, "scielo_records_pdf")
    
    if not os.path.exists(state_file):
        print(f"Archivo de estado no encontrado: {state_file}")
        return
    if not os.path.exists(pdf_folder):
        print(f"Carpeta de PDF no encontrada: {pdf_folder}")
        return
    
    state_ids = get_state_ids(state_file)
    pdf_ids = get_pdf_file_ids(pdf_folder)
    
    missing_ids = pdf_ids - state_ids
    
    if missing_ids:
        missing_ids_sorted = sorted(missing_ids)
        print("Documentos PDF presentes en la carpeta pero no en el listado de IDs del estado:")
        for doc in missing_ids_sorted:
            print(doc)
        total_missing = len(missing_ids_sorted)
        print(f"Total de documentos faltantes: {total_missing}")
        
        choice = input("¿Desea eliminar estos archivos? [Y/n] (por defecto n): ").strip()
        if choice.lower() == "y":
            for doc in missing_ids_sorted:
                pdf_file = os.path.join(pdf_folder, f"{doc}.pdf")
                try:
                    os.remove(pdf_file)
                    print(f"Eliminado: {doc}.pdf")
                except Exception as e:
                    print(f"Error eliminando {doc}.pdf: {e}")
            print("Eliminación completada.")
        else:
            print("No se eliminaron los archivos.")
    else:
        print("Todos los nombres de PDF están presentes en el archivo de estado.")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python3 find_missing_pdfs.py <pais>")
        sys.exit(1)
    country = sys.argv[1]
    main(country)
