import argparse
import subprocess
import tempfile
import json
from datetime import datetime
import os
from tqdm import tqdm
import csv

def fecha_a_timestamp(fecha_str):
    dt = datetime.strptime(fecha_str, "%d-%m-%Y-%H:%M:%S")
    return int(dt.timestamp() * 1000)

def dynamodb_item_to_dict(item):
    result = {}
    for k, v in item.items():
        if isinstance(v, dict):
            result[k] = list(v.values())[0]
        else:
            result[k] = v
    return result

def ejecutar_comando(table_name, fecha_inicio_ts=None, fecha_fin_ts=None, product_id=None):
    """Ejecuta el comando aws-dynamodb-parallel-scan y retorna la ruta al archivo temporal generado."""

    comando = [
        "aws-dynamodb-parallel-scan",
        "--table-name", table_name,
        "--total-segments", "1000",
    ]

    if fecha_inicio_ts is not None and fecha_fin_ts is not None and product_id is not None:
        filter_expr = "tstamp BETWEEN :ts_ini AND :ts_fin AND productId = :pid"
        attr_values = {
            ":ts_ini": {"N": str(fecha_inicio_ts)},
            ":ts_fin": {"N": str(fecha_fin_ts)},
            ":pid": {"S": str(product_id)}
        }
        comando += [
            "--filter-expression", filter_expr,
            "--expression-attribute-values", json.dumps(attr_values)
        ]

    with tempfile.NamedTemporaryFile(delete=False, mode="w+", suffix=".jsonl") as temp_file:
        with subprocess.Popen(comando, stdout=temp_file, stderr=subprocess.PIPE) as proc:
            _, stderr = proc.communicate()
            if proc.returncode != 0:
                raise Exception(stderr.decode())
        return temp_file.name

def procesar_y_escribir_csv(ruta_jsonl, output_csv):
    with open(ruta_jsonl, "r") as f_in, open(output_csv, "w", newline='', encoding="utf-8") as f_out:
        writer = None
        count = 0
        for line in tqdm(f_in, desc="[✓] Escribiendo CSV en tiempo real"):
            try:
                response = json.loads(line)
                for item in response.get("Items", []):
                    flat_item = dynamodb_item_to_dict(item)
                    if writer is None:
                        fieldnames = list(flat_item.keys())
                        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                        writer.writeheader()
                    writer.writerow(flat_item)
                    count += 1
            except json.JSONDecodeError:
                continue
    return count

def main():
    parser = argparse.ArgumentParser(
        description="Extractor paralelo de logs de DynamoDB. \n"
                    "Ejemplos:\n"
                    "  python3 extract_massive_data.py --table-name my-table --output out.csv\n"
                    "  python3 extract_massive_data.py --table-name my-table --fecha-inicio '24-04-2025-10:00:00' \
      --fecha-fin '25-04-2025-10:00:00' --product-id 4 --output logs_4.csv"
    )
    parser.add_argument("--table-name", required=True, help="Nombre de la tabla DynamoDB")
    parser.add_argument("--fecha-inicio", help="Formato: DD-MM-YYYY-HH:MM:SS")
    parser.add_argument("--fecha-fin", help="Formato: DD-MM-YYYY-HH:MM:SS")
    parser.add_argument("--product-id", help="ID del producto a filtrar")
    parser.add_argument("--output", required=True, help="Archivo de salida CSV")
    args = parser.parse_args()

    try:
        ts_ini = ts_fin = None
        if args.fecha_inicio and args.fecha_fin and args.product_id:
            ts_ini = fecha_a_timestamp(args.fecha_inicio)
            ts_fin = fecha_a_timestamp(args.fecha_fin)
        print("[⌛] Ejecutando escaneo paralelo...")
        archivo_jsonl = ejecutar_comando(args.table_name, ts_ini, ts_fin, args.product_id)

        print("[📦] Procesando y escribiendo CSV...")
        total = procesar_y_escribir_csv(archivo_jsonl, args.output)

        if total == 0:
            print("[✘] No se encontraron elementos.")
        else:
            print(f"[✔] {total} elementos guardados en {args.output}")

        os.remove(archivo_jsonl)

    except Exception as e:
        print(f"[✘] Error: {str(e)}")

if __name__ == "__main__":
    main()
