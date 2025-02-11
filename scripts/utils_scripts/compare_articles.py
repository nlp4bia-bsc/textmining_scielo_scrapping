#!/usr/bin/env python3
import os
import csv
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(
        description="Script para comparar la cantidad de artículos por revista entre el archivo de estados y el de revistas."
    )
    parser.add_argument("country", help="País (por ejemplo, 'br', 'cl', etc.)")
    args = parser.parse_args()
    country = args.country

    # Definir las rutas de los archivos según el país
    base_folder = f"/data/str/temp/scielov2/{country}"
    magazine_csv_file = os.path.join(base_folder, f"scielo_{country}_magazines.csv")
    state_csv_file = os.path.join(base_folder, f"scielo_{country}_records_states.csv")

    if not os.path.exists(magazine_csv_file):
        print(f"El archivo de revistas no existe: {magazine_csv_file}")
        sys.exit(1)
    if not os.path.exists(state_csv_file):
        print(f"El archivo de estados no existe: {state_csv_file}")
        sys.exit(1)

    # 1. Cargar el archivo de estados y contar artículos por revista (agrupados por setSpec)
    states_counts = {}
    sin_revista_count = 0  # Artículos sin setSpec asignado
    states_total = 0       # Total de filas en el archivo de estados
    with open(state_csv_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            states_total += 1
            spec = row.get("setSpec", "").strip()
            if not spec:
                sin_revista_count += 1
            else:
                states_counts[spec] = states_counts.get(spec, 0) + 1

    # 2. Cargar el archivo de revistas y construir un diccionario de revistas
    #    Se toma como identificador para la unión el valor de "issn_api" (o "issn" si está vacío)
    magazine_data = {}
    with open(magazine_csv_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Se prefiere issn_api para hacer la correspondencia con setSpec del archivo de estados
            mag_id = row.get("issn_api", "").strip()
            if not mag_id:
                mag_id = row.get("issn", "").strip()
            mag_name = row.get("magazine_name", "").strip()
            try:
                articles_mag = int(row.get("articles", "0").strip())
            except ValueError:
                articles_mag = 0
            magazine_data[mag_id] = {
                "magazine_name": mag_name,
                "articles_magazine": articles_mag
            }

    # 3. Para cada revista se obtiene:
    #     - Número de artículos contados en el archivo de estados.
    #     - Número de artículos definidos en el archivo de revistas.
    #     - La diferencia (Estados - Revistas).
    print("Diferencias por revista:")
    header_format = "{:<20} {:<30} {:>10} {:>10} {:>10}"
    print(header_format.format("Magazine ID", "Magazine Name", "Estados", "Revistas", "Diff"))
    print("-" * 90)
    total_diff = 0
    for mag_id, data in magazine_data.items():
        estados_count = states_counts.get(mag_id, 0)
        revistas_count = data["articles_magazine"]
        diff = estados_count - revistas_count
        total_diff += diff
        print(header_format.format(mag_id, data["magazine_name"][:30], estados_count, revistas_count, diff))

    # 4. Identificar artículos en el archivo de estados que no pertenecen a ninguna revista
    sin_revista_states = 0
    for spec, count in states_counts.items():
        if spec not in magazine_data:
            sin_revista_states += count
    # Incluir aquellos artículos que no tenían setSpec asignado
    sin_revista_states += sin_revista_count

    print("\nArtículos sin revista asociada: {}".format(sin_revista_states))
    
    # 5. Mostrar total de filas en el archivo de estados y comparar con la suma de artículos en revistas
    total_revistas = sum(data["articles_magazine"] for data in magazine_data.values())
    print("\nTotal de filas en el archivo de estados: {}".format(states_total))
    print("Suma de 'articles' en el archivo de revistas: {}".format(total_revistas))
    print("Diferencia (Estados - Revistas): {}".format(states_total - total_revistas))
    print("Diferencia total acumulada por revista (según agrupación): {}".format(total_diff))

if __name__ == "__main__":
    main()
