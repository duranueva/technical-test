import argparse
import os
import sys
import warnings
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

MUST_HAVE_COLUMNS = [
    "id", "name", "company_id", "amount", "status", "created_at", "paid_at"
]

DDL = """
CREATE TABLE IF NOT EXISTS companies (
    id VARCHAR(64) PRIMARY KEY,
    company_name VARCHAR(130)
);
CREATE TABLE IF NOT EXISTS charges (
    id VARCHAR(64) PRIMARY KEY,
    company_id VARCHAR(64) NOT NULL REFERENCES companies(id),
    amount DECIMAL(16,2),
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL
);
CREATE INDEX IF NOT EXISTS idx_charges_company_id ON charges(company_id);
CREATE INDEX IF NOT EXISTS idx_charges_created_at ON charges(created_at);
"""

# Límite para DECIMAL(16,2): 16 dígitos en total con 2 decimales -> < 10^14 antes del punto
DECIMAL16_2_ABS_MAX = Decimal("1e14")  # si abs() >= 1e14 no cabe en DECIMAL(16,2)

def env(name: str, default=None, required: bool = False):
    """
    Toma una variable de entorno y falla bonito si hace falta.
    """
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        print(f"[ERROR] Falta la variable de entorno: {name}", file=sys.stderr)
        sys.exit(1)
    return value

def make_engine_for_db(dbname: str, autocommit: bool = False) -> Engine:
    """
    Crea un Engine de SQLAlchemy para la base {dbname}.
    """
    user = env("PGUSER", required=True)
    password = env("PGPASSWORD", required=True)
    host = env("PGHOST", "localhost")
    port = env("PGPORT", "5432")
    driver = "postgresql+psycopg"
    url = f"{driver}://{user}:{password}@{host}:{port}/{dbname}"
    if autocommit:
        return create_engine(url, isolation_level="AUTOCOMMIT", future=True)
    return create_engine(url, future=True)

def ensure_database_exists(target_db: str):
    """
    Si la DB destino no existe, la creo. Si existe, lo digo y seguimos.
    """
    admin = make_engine_for_db("postgres", autocommit=True)
    with admin.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :d"),
            {"d": target_db}
        ).scalar()
        if not exists:
            print(f"[INFO] Creando base de datos '{target_db}'…")
            conn.execute(text(f'CREATE DATABASE "{target_db}" ENCODING \'UTF8\';'))
        else:
            print(f"[INFO] La base de datos '{target_db}' ya existe.")

def run_ddl(engine: Engine):
    """
    Aplico el DDL arriba. Divido por ';' y ejecuto cada sentencia no vacía.
    """
    with engine.begin() as conn:
        for stmt in DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s + ";"))

def normalize_amount(x):
    """
    Normalizo 'amount':
      - Devuelvo Decimal con 2 decimales (ROUND_HALF_UP) si entra en DECIMAL(16,2).
      - Devuelvo None si viene nulo, es inválido o se sale de rango (se insertará NULL).
    """
    if pd.isna(x) or x == "":
        return None
    try:
        d = Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

    # Fuera de rango para DECIMAL(16,2)
    if d.copy_abs() >= DECIMAL16_2_ABS_MAX:
        return None

    # Cuantizo a 2 decimales estilo banco
    return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def load_data(engine: Engine, df_companies: pd.DataFrame, df_charges: pd.DataFrame, mode: str):
    """
    Inserto en companies y charges.
    - replace: TRUNCATE y recargo (idempotente a nivel resultado).
    - append: ON CONFLICT DO NOTHING para no duplicar ids.
    """
    with engine.begin() as conn:
        if mode == "replace":
            # Truncar respetando FK en un solo statement
            conn.execute(text("TRUNCATE TABLE charges, companies RESTART IDENTITY CASCADE;"))
            print("[INFO] Tablas truncadas (replace).")

        # companies
        cmp_records = df_companies.to_dict(orient="records")
        if cmp_records:
            conn.execute(
                text("""
                    INSERT INTO companies (id, company_name)
                    VALUES (:id, :company_name)
                    ON CONFLICT (id) DO NOTHING
                """),
                cmp_records
            )

        # charges
        chg_records = df_charges.to_dict(orient="records")
        if chg_records:
            conn.execute(
                text("""
                    INSERT INTO charges (id, company_id, amount, status, created_at, updated_at)
                    VALUES (:id, :company_id, :amount, :status, :created_at, :updated_at)
                    ON CONFLICT (id) DO NOTHING
                """),
                chg_records
            )

def main():
    parser = argparse.ArgumentParser(
        description="Transforma un CSV a warehouse.public.companies/charges"
    )
    parser.add_argument("--input", required=True, help="Ruta al CSV (e.g., datasets/extracted.csv)")
    parser.add_argument(
        "--if-exists",
        choices=["append", "replace"],
        default="append",
        help="Comportamiento de carga si ya hay datos: 'append' (no duplica ids) o 'replace' (trunca y recarga)"
    )
    parser.add_argument("--database", default="warehouse", help="Base de datos destino (default: warehouse)")
    args = parser.parse_args()

    # 1) DB lista
    ensure_database_exists(args.database)

    # 2) Conexión y tablas listas
    engine = make_engine_for_db(args.database)
    run_ddl(engine)

    # 3) Leer CSV (tipos básicos + parse de fechas)
    try:
        df = pd.read_csv(
            args.input,
            dtype={
                "id": "string",
                "name": "string",
                "company_id": "string",
                "status": "string",
            },
            parse_dates=["created_at", "paid_at"],
            keep_default_na=True,
            na_values=["", "null", "None", "NA", "N/A"]
        )
    except FileNotFoundError:
        print(f"[ERROR] No encontré el archivo: {args.input}", file=sys.stderr)
        sys.exit(1)

    # 4) Sanity check de columnas
    missing = [c for c in MUST_HAVE_COLUMNS if c not in df.columns]
    if missing:
        print(f"[ERROR] Faltan columnas en el CSV: {missing}", file=sys.stderr)
        sys.exit(1)

    # 5) Limpieza mínima
    n_before = len(df)
    df = df.dropna(subset=["id", "company_id", "created_at"])
    dropped_key = n_before - len(df)
    if dropped_key:
        print(f"[INFO] Filas descartadas por id/company_id/created_at nulos: {dropped_key}")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        df["amount_norm"] = df["amount"].apply(normalize_amount)

    # Contar montos inválidos/fuera de rango que se irán como NULL
    invalid_amounts = (df["amount_norm"].isna() & df["amount"].notna())
    n_invalid_amounts = int(invalid_amounts.sum())
    if n_invalid_amounts:
        print("[WARN] "
              f"{n_invalid_amounts} filas con 'amount' inválido o fuera de rango -> "
              "se insertarán como NULL en charges.amount")

    # 6) companies
    df_companies = (
        df[["company_id", "name"]]
        .dropna(subset=["company_id"])
        .drop_duplicates(subset=["company_id"])
        .rename(columns={"company_id": "id", "name": "company_name"})
        .astype({"id": "string", "company_name": "string"})
    )

    # 7) charges
    df_charges = pd.DataFrame({
        "id": df["id"].astype("string"),
        "company_id": df["company_id"].astype("string"),
        "amount": df["amount_norm"],  # dentro de rango o NULL
        "status": df["status"].fillna("unknown").astype("string"),
        "created_at": df["created_at"],
        # Si 'paid_at' es NaT, lo dejo en None para que vaya como NULL
        "updated_at": df["paid_at"].where(df["paid_at"].notna(), None),
    }).dropna(subset=["id", "company_id", "status", "created_at"])

    # 8) Cargar al warehouse
    load_data(engine, df_companies, df_charges, args.if_exists)

    # 9) Métricas para cerrar con broche de oro
    with engine.connect() as conn:
        cnt_cmp = conn.execute(text("SELECT COUNT(*) FROM companies;")).scalar_one()
        cnt_chg = conn.execute(text("SELECT COUNT(*) FROM charges;")).scalar_one()
        print(f"[OK] Transform completado. companies={cnt_cmp} | charges={cnt_chg}")

if __name__ == "__main__":
    main()
