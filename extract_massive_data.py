import argparse
import subprocess
import shutil
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

def ejecutar_y_escribir_csv(table_name, output_csv, fecha_inicio_ts=None, fecha_fin_ts=None, product_id=None):
    """Ejecuta aws-dynamodb-parallel-scan escribiendo JSONL y CSV en tiempo real."""

    if shutil.which("aws-dynamodb-parallel-scan") is None:
        raise FileNotFoundError("aws-dynamodb-parallel-scan not found; please install it")

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

    with tempfile.NamedTemporaryFile(delete=False, mode="w+", suffix=".jsonl") as temp_file, \
         open(output_csv, "w", newline='', encoding="utf-8") as f_out:
        print(f"[i] Temporary JSONL: {temp_file.name}")
        writer = None
        count = 0
        with subprocess.Popen(comando, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
            for line in tqdm(proc.stdout, desc="[✓] Escribiendo CSV en tiempo real"):
                temp_file.write(line)
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

            stderr = proc.stderr.read()
            if proc.wait() != 0:
                raise Exception(stderr)

    return count, temp_file.name

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

    filtros = [args.fecha_inicio, args.fecha_fin, args.product_id]
    if 0 < sum(v is not None for v in filtros) < 3:
        parser.error("--fecha-inicio, --fecha-fin y --product-id deben usarse juntos")

    try:
        ts_ini = ts_fin = None
        if args.fecha_inicio and args.fecha_fin and args.product_id:
            ts_ini = fecha_a_timestamp(args.fecha_inicio)
            ts_fin = fecha_a_timestamp(args.fecha_fin)
        print("[⌛] Ejecutando escaneo y escribiendo CSV...")
        total, archivo_jsonl = ejecutar_y_escribir_csv(
            args.table_name,
            args.output,
            ts_ini,
            ts_fin,
            args.product_id,
        )

        if total == 0:
            print("[✘] No se encontraron elementos.")
        else:
            print(f"[✔] {total} elementos guardados en {args.output}")

        os.remove(archivo_jsonl)

    except FileNotFoundError as e:
        print(f"[✘] {e}")
    except Exception as e:
        print(f"[✘] Error: {str(e)}")

if __name__ == "__main__":
    main()
