import argparse
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Numeric, Date

MUST_HAVE_COLUMNS = ["id", "name", "company_id", "amount", "status", "created_at", "paid_at"]

def make_engine():
    # Construye la URL de conexión desde variables de entorno
    username = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    database = os.getenv("PGDATABASE")
    conn_url = f"postgresql+psycopg://{username}:{password}@{host}:{port}/{database}"
    return create_engine(conn_url, future=True)

def cli():
    parser = argparse.ArgumentParser(
        description="Sube un CSV 'crudo' al esquema destino en Postgres (por defecto raw.raw_purchases)."
    )
    parser.add_argument("--input", required=True, help="Ruta del CSV (dentro del contenedor).")
    parser.add_argument("--table", default="raw_purchases", help="Tabla destino (sin esquema).")
    parser.add_argument("--schema", default="raw", help="Esquema destino (default: raw).")
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="replace",
        help="Qué hacer si la tabla ya existe."
    )
    opts = parser.parse_args()

    # Lectura del CSV: tipos básicos y parseo de fechas
    try:
        frame = pd.read_csv(
            opts.input,
            dtype={
                "id": "string",
                "name": "string",
                "company_id": "string",
                "status": "string",
            },
            parse_dates=["created_at", "paid_at"],  # formato esperado: YYYY-MM-DD
            dayfirst=False
        )
    except Exception as exc:
        print(f"[ERROR] No se pudo abrir el CSV '{opts.input}': {exc}", file=sys.stderr)
        sys.exit(1)

    # Normalizar encabezados y validación mínima
    frame.columns = [c.strip().lower() for c in frame.columns]

    missing = [c for c in MUST_HAVE_COLUMNS if c not in frame.columns]
    if missing:
        print(f"[ERROR] Faltan columnas requeridas: {missing}", file=sys.stderr)
        sys.exit(2)

    # Limpieza ligera en “crudo” (sin lógica de negocio):
    # - Eliminar columnas totalmente vacías
    frame = frame.dropna(axis=1, how="all")

    # - amount: evitar infinitos que luego truenan en Postgres
    frame["amount"] = frame["amount"].replace([float("inf"), float("-inf")], pd.NA)

    # - Trim a columnas de texto típicas
    text_cols = ("id", "name", "company_id", "status")
    for col in text_cols:
        if col in frame.columns:
            frame[col] = frame[col].astype("string").str.strip()

    # Nota: 'paid_at' puede venir nulo (no rellenamos)

    # Mapeo de tipos para la tabla RAW
    pg_types = {
        "id": String(64),
        "name": String(130),
        "company_id": String(64),
        "amount": String,          # se conserva como texto en RAW
        "status": String(30),
        "created_at": Date(),
        "paid_at": Date(),
    }

    engine = make_engine()

    # Crear esquema si no existe
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {opts.schema};"))
        conn.commit()

    # Carga a Postgres
    try:
        frame.to_sql(
            opts.table,
            con=engine,
            schema=opts.schema,
            if_exists=opts.if_exists,
            index=False,
            dtype=pg_types,
        )
    except Exception as exc:
        print(f"[ERROR] Falló la escritura en Postgres: {exc}", file=sys.stderr)
        sys.exit(3)

    print(f"[OK] {len(frame):,} filas cargadas en {opts.schema}.{opts.table}")

if __name__ == "__main__":
    cli()
