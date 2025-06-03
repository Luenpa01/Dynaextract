# Dynaextract

Herramienta para extraer masivamente datos de tablas DynamoDB utilizando `aws-dynamodb-parallel-scan`.
Mientras se ejecuta el escaneo, el CSV se va generando en tiempo real.

## Requisitos

- Python 3 y el paquete `tqdm` (`pip install tqdm`)
- La herramienta `aws-dynamodb-parallel-scan` disponible en el `PATH`
- Credenciales de AWS configuradas con `aws configure`

Al iniciar el proceso se muestra la ruta de un archivo temporal `.jsonl` que se crea en el
directorio temporal del sistema. Este archivo se elimina automáticamente al finalizar.

## Uso

### Escaneo completo de una tabla
```bash
python3 extract_massive_data.py --table-name NOMBRE_TABLA --output archivo.csv
```

### Escaneo filtrado por fechas y `productId`
```bash
python3 extract_massive_data.py \
  --table-name NOMBRE_TABLA \
  --fecha-inicio "24-04-2025-10:00:00" \
  --fecha-fin "25-04-2025-10:00:00" \
  --product-id 4 \
  --output logs_4.csv
```

Los argumentos de fecha deben proporcionarse en el formato `DD-MM-YYYY-HH:MM:SS`.
Si se desea aplicar filtros, `--fecha-inicio`, `--fecha-fin` y `--product-id` deben
especificarse siempre juntos.
