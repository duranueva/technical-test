import os
import pandas as pd
from sqlalchemy import create_engine

OUTPUT_FILE = "datasets/extracted.csv"

def build_engine():
    username = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    hostname = os.getenv("PGHOST")
    db_port  = os.getenv("PGPORT")
    database = os.getenv("PGDATABASE")

    conn_str = f"postgresql+psycopg://{username}:{password}@{hostname}:{db_port}/{database}"
    return create_engine(conn_str, future=True)

def extract_data():
    engine = build_engine()
    # traer datos de la tabla en esquema raw
    purchases_df = pd.read_sql("SELECT * FROM raw.raw_purchases", con=engine)
    
    # guardar en CSV
    purchases_df.to_csv(OUTPUT_FILE, index=False)
    print(f"[DONE] Se extrajeron {len(purchases_df):,} registros en {OUTPUT_FILE}")
    return purchases_df

if __name__ == "__main__":
    extract_data()
