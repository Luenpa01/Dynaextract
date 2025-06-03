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

def ejecutar_comando(fecha_inicio_ts, fecha_fin_ts, product_id):
    filter_expr = "tstamp BETWEEN :ts_ini AND :ts_fin AND productId = :pid"
    attr_values = {
        ":ts_ini": {"N": str(fecha_inicio_ts)},
        ":ts_fin": {"N": str(fecha_fin_ts)},
        ":pid": {"S": str(product_id)}
    }

    with tempfile.NamedTemporaryFile(delete=False, mode="w+", suffix=".jsonl") as temp_file:
        comando = [
            "aws-dynamodb-parallel-scan",
            "--table-name", "save-logs-cobranza-information-pdn",
            "--total-segments", "1000",
            "--filter-expression", filter_expr,
            "--expression-attribute-values", json.dumps(attr_values)
        ]
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
    parser = argparse.ArgumentParser(description="Extractor paralelo de logs de DynamoDB")
    parser.add_argument("--fecha-inicio", required=True, help="Formato: DD-MM-YYYY-HH:MM:SS")
    parser.add_argument("--fecha-fin", required=True, help="Formato: DD-MM-YYYY-HH:MM:SS")
    parser.add_argument("--product-id", required=True, help="ID del producto a filtrar")
    parser.add_argument("--output", required=True, help="Archivo de salida CSV")
    args = parser.parse_args()

    try:
        ts_ini = fecha_a_timestamp(args.fecha_inicio)
        ts_fin = fecha_a_timestamp(args.fecha_fin)
        print("[⌛] Ejecutando escaneo paralelo...")
        archivo_jsonl = ejecutar_comando(ts_ini, ts_fin, args.product_id)

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