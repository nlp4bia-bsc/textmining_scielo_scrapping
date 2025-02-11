import os
import csv


def create_statistics_file_script():
    """
    Crea un archivo CSV de estadísticas en /storage/temp/scielov2/download_statistics.csv.
    Por cada país (obtenido de env["scielo-path"].keys()) se calcula:
      - country: nombre del país
      - magazines_count: cantidad de revistas (desde scielo_{country}_magazines.csv)
      - magazines_with_errors: cantidad de revistas con 'articles' 0 o no definido
      - articles_count: suma total de artículos (columna 'articles')
      - xml_count: cantidad de archivos JSON en scielo_records_xml
      - xml_size_gb: tamaño total en GB de esos archivos
      - pdf_count, pdf_size_gb: (si existe la carpeta scielo_records_pdf; sino 0)
      - txt_count, txt_size_gb: (si existe la carpeta scielo_records_txt; sino 0)
      - files_full_processed: cantidad de artículos con xml_path no vacío en el archivo de estados
      - files_with_errors: articles_count - files_full_processed
      - percentage_files_with_errors: (files_with_errors / articles_count)*100 (o 0 si articles_count == 0)
    Finalmente se añade una fila “TOTAL” que suma (o calcula) cada métrica a nivel global.
    """
    base_folder = "/data/str/temp/scielov2"

    if not os.path.exists(base_folder):
        base_folder = "/storage/temp/scielov2"

    stats_file = os.path.join(base_folder, "scielo_statistics.csv")
    countries = ["argentina","bolivia","chile","colombia","costa-rica","cuba","ecuador","mexico","paraguay","peru","portugal","spain","south-africa","uruguay"]
    stats_rows = []
    
    for country in countries:
        country_folder = os.path.join(base_folder, country)
        magazines_csv = os.path.join(country_folder, f"scielo_{country}_magazines.csv")
        magazines_count = 0
        magazines_with_errors = 0
        articles_count = 0
        if os.path.exists(magazines_csv):
            with open(magazines_csv, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    magazines_count += 1
                    try:
                        articles = int(row.get("articles", "0").strip())
                    except:
                        articles = 0
                    articles_count += articles
                    if articles == 0:
                        magazines_with_errors += 1
        else:
            print(f"Magazine CSV no encontrado para {country}.")
        
        # XML (JSON) folder
        xml_folder = os.path.join(country_folder, "scielo_records_xml")
        if os.path.exists(xml_folder):
            xml_files = [f for f in os.listdir(xml_folder) if f.endswith(".json")]
            xml_count = len(xml_files)
            total_xml_size_gb = sum(os.path.getsize(os.path.join(xml_folder, f)) for f in xml_files)
            xml_size_gb = total_xml_size_gb / (1024**3)
        else:
            xml_count = 0
            xml_size_gb = 0
        
        # PDF folder
        pdf_folder = os.path.join(country_folder, "scielo_records_pdf")
        if os.path.exists(pdf_folder):
            pdf_files = [f for f in os.listdir(pdf_folder) if f.lower().endswith(".pdf")]
            pdf_count = len(pdf_files)
            total_pdf_size_gb = sum(os.path.getsize(os.path.join(pdf_folder, f)) for f in pdf_files)
            pdf_size_gb = total_pdf_size_gb / (1024**3)
        else:
            pdf_count = 0
            pdf_size_gb = 0
        
        # TXT folder
        txt_folder = os.path.join(country_folder, "scielo_records_txt")
        if os.path.exists(txt_folder):
            txt_files = [f for f in os.listdir(txt_folder) if f.lower().endswith(".txt")]
            txt_count = len(txt_files)
            total_txt_size_gb = sum(os.path.getsize(os.path.join(txt_folder, f)) for f in txt_files)
            txt_size_gb = total_txt_size_gb / (1024**3)
        else:
            txt_count = 0
            txt_size_gb = 0
        
        # Files full processed: se cuentan las filas en el archivo de estado con xml_path no vacío
        state_csv = os.path.join(country_folder, f"scielo_{country}_records_states.csv")
        files_full_processed = 0
        if os.path.exists(state_csv):
            with open(state_csv, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if row.get("path_xml", "").strip() and row.get("path_pdf", "").strip():
                        files_full_processed += 1
        else:
            files_full_processed = 0
        
        files_with_errors = articles_count - files_full_processed
        if articles_count > 0:
            percentage_files_with_errors = (files_with_errors / articles_count) * 100
        else:
            percentage_files_with_errors = 0
        
        stats_rows.append({
            "country": country,
            "magazines_count": magazines_count,
            "magazines_with_errors": magazines_with_errors,
            "articles_count": articles_count,
            "xml_count": xml_count,
            "xml_size_gb": round(xml_size_gb, 3),
            "pdf_count": pdf_count,
            "pdf_size_gb": round(pdf_size_gb, 3),
            "txt_count": txt_count,
            "txt_size_gb": round(txt_size_gb, 3),
            "files_full_processed": files_full_processed,
            "files_with_errors": files_with_errors,
            "percentage_files_with_errors": round(percentage_files_with_errors, 2)
        })
    
    # Calcular totales (sumatoria de cada campo numérico)
    total_stats = {
        "country": "TOTAL",
        "magazines_count": sum(row["magazines_count"] for row in stats_rows),
        "magazines_with_errors": sum(row["magazines_with_errors"] for row in stats_rows),
        "articles_count": sum(row["articles_count"] for row in stats_rows),
        "xml_count": sum(row["xml_count"] for row in stats_rows),
        "xml_size_gb": round(sum(row["xml_size_gb"] for row in stats_rows), 3),
        "pdf_count": sum(row["pdf_count"] for row in stats_rows),
        "pdf_size_gb": round(sum(row["pdf_size_gb"] for row in stats_rows), 3),
        "txt_count": sum(row["txt_count"] for row in stats_rows),
        "txt_size_gb": round(sum(row["txt_size_gb"] for row in stats_rows), 3),
        "files_full_processed": sum(row["files_full_processed"] for row in stats_rows),
        "files_with_errors": sum(row["files_with_errors"] for row in stats_rows),
        "percentage_files_with_errors": 0  # Se calculará a partir de articles_count.
    }
    if total_stats["articles_count"] > 0:
        total_stats["percentage_files_with_errors"] = round(
            (total_stats["files_with_errors"] / total_stats["articles_count"]) * 100, 2
        )
    else:
        total_stats["percentage_files_with_errors"] = 0
    
    stats_rows.append(total_stats)
    
    fieldnames = ["country", "magazines_count", "magazines_with_errors", "articles_count",
                  "xml_count", "xml_size_gb", "pdf_count", "pdf_size_gb", "txt_count", "txt_size_gb",
                  "files_full_processed", "files_with_errors", "percentage_files_with_errors"]
    with open(stats_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in stats_rows:
            writer.writerow(row)
    print(f"Archivo de estadísticas creado: {stats_file}")

if __name__ == '__main__':
    create_statistics_file_script()