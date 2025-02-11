import csv
import os
import sys

def read_ids_from_csv(file_path, column_name):
    """
    Abre el archivo CSV ubicado en file_path y retorna un conjunto (set)
    con los valores únicos de la columna especificada en column_name.
    """
    ids = set()
    try:
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                value = row.get(column_name, "").strip()
                if value:
                    ids.add(value)
    except Exception as e:
        print(f"Error al leer {file_path}: {e}")
    return ids

def main(country):
    # Se asume que la estructura de carpetas es: /storage/temp/scielov2/{country}/
    output_folder = f"/data/str/temp/scielov2/{country}"
    # El CSV de revistas se llama: scielo_{country}_magazines.csv
    magazines_csv = os.path.join(output_folder, f"scielo_{country}_magazines.csv")
    # El CSV de estados se llama: scielo_{country}_records_states.csv
    states_csv = os.path.join(output_folder, f"scielo_{country}_records_states.csv")
    
    if not os.path.exists(magazines_csv):
        print(f"No se encontró el archivo de revistas: {magazines_csv}")
        return
    if not os.path.exists(states_csv):
        print(f"No se encontró el archivo de estados: {states_csv}")
        return

    # Leer los IDs de cada archivo
    magazine_ids = read_ids_from_csv(magazines_csv, "issn")
    state_ids = read_ids_from_csv(states_csv, "setSpec")
    
    # Calcular las diferencias
    diff_magazines_not_in_states = magazine_ids - state_ids
    diff_states_not_in_magazines = state_ids - magazine_ids

    print("IDs en el CSV de revistas (issn) pero NO en el CSV de estados (setSpec):")
    if diff_magazines_not_in_states:
        for id_val in sorted(diff_magazines_not_in_states):
            print(id_val)
    else:
        print("Ninguno.")

    print("\nIDs en el CSV de estados (setSpec) pero NO en el CSV de revistas (issn):")
    if diff_states_not_in_magazines:
        for id_val in sorted(diff_states_not_in_magazines):
            print(id_val)
    else:
        print("Ninguno.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python compare_country_csv.py <country>")
        sys.exit(1)
    country = sys.argv[1]
    main(country)
