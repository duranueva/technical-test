# Technical Test – Data Pipeline (Postgres + Docker) & Python (Sección 2)

## Stack

* **Docker / Docker Compose**
* **PostgreSQL 16**
* **pgAdmin 4**
* **Python 3.11** (en contenedor `etl`) con: `pandas`, `sqlalchemy[psycopg]`, `pyarrow` (opcional)

## Prerrequisitos

* macOS / Windows / Linux con **Docker Desktop** instalado.
* (Opcional) **VS Code**.

## Estructura del proyecto

```
technical-test/
├─ docker-compose.yml
├─ .env                        # credenciales locales 
├─ datasets/
│  ├─ input.csv                # dataset de entrada (copiar aquí)
│  └─ extracted.csv            # generado por extracción (1.2)
├─ etl/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ src/
│     ├─ load_raw.py           # 1.1
│     ├─ extract.py            # 1.2
│     └─ transform.py          # 1.3 + 1.4
└─ sql/
   └─ (opcional, DDL/diagramas)
```

---

## 0) Configuración rápida

1. **Clonar y entrar al proyecto:**

```bash
git clone https://github.com/duranueva/technical-test.git
cd technical-test
```

2. **Crear archivo `.env`**

### MacOS / Linux

```bash
cat > .env << 'EOF'
PGUSER=postgres
PGPASSWORD=postgres
PGDATABASE=warehouse
PGHOST=db
PGPORT=5432

PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=admin
EOF
```

### Windows (PowerShell)

```powershell
@'
PGUSER=postgres
PGPASSWORD=postgres
PGDATABASE=warehouse
PGHOST=db
PGPORT=5432

PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=admin
'@ | Out-File -FilePath .env -Encoding utf8
```


---

## 1) Levantar servicios (Postgres + pgAdmin)

```bash
docker compose up -d db pgadmin
```

* Postgres en `localhost:5432`
* pgAdmin en `http://localhost:8080` (login: `admin@example.com` / `admin`)



---

## 2) Construir la imagen del ETL

```bash
docker compose build etl
```

---

## 3) Sección 1 — Data Pipeline

### 1.1 Carga RAW a Postgres

Carga el CSV **en crudo** a `raw.raw_purchases`:

```bash
docker compose run --rm etl python src/load_raw.py --input datasets/input.csv --table raw_purchases --schema raw --if-exists replace
```

Verificar:

```bash
docker compose exec db psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM raw.raw_purchases;"
```

### 1.2 Extracción (CSV)

Exporta a CSV desde RAW (genera datasets/extracted.csv):

```bash
docker compose run --rm etl python src/extract.py
```

### 1.3 + 1.4 Transformación y Dispersión

Transforma al esquema objetivo y carga a tablas finales **companies** y **charges** con FK:

```bash
docker compose run --rm etl  python src/transform.py  --input datasets/extracted.csv  --if-exists replace
```

Verificar:

```bash
docker compose exec db psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM companies;"
```

```bash
docker compose exec db psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM charges;"
```

**El diagrama de las tablas creadas.**

<p align="center">
  <img src="/img/mr.png" alt="Description" width="1000">
</p> 


### 1.5 Vista: total transaccionado por día y compañía

```bash
docker compose exec db psql -U postgres -d warehouse -c "
CREATE OR REPLACE VIEW v_daily_totals AS
SELECT
  DATE(c.created_at) AS day,
  comp.company_name,
  comp.id AS company_id,
  SUM(c.amount) AS total_amount
FROM charges c
JOIN companies comp 
  ON comp.id = c.company_id
GROUP BY DATE(c.created_at), comp.company_name, comp.id;
"
docker compose exec db psql -U postgres -d warehouse -c "SELECT * FROM v_daily_totals ORDER BY day DESC LIMIT 10;"
```

🔹 **Copiar y ejecutar según tu sistema operativo**:

* **Mac/Linux (bash/zsh):** puedes copiar exactamente el bloque de arriba.
* **Windows (PowerShell):** copia y pega los siguientes:

```powershell
docker compose exec db psql -U postgres -d warehouse -c "CREATE OR REPLACE VIEW v_daily_totals AS SELECT DATE(c.created_at) AS day, comp.company_name, comp.id AS company_id, SUM(c.amount) AS total_amount FROM charges c JOIN companies comp ON comp.id = c.company_id GROUP BY 1,2,3;"
```
```powershell
docker compose exec db psql -U postgres -d warehouse -c "SELECT * FROM v_daily_totals ORDER BY day DESC LIMIT 10;"
```

> **Nota**: si `psql` abre un “pager”, sal con la tecla **`q`**.





---

## 4) Sección 2 — Python (número faltante 1..100)


```bash
docker compose run --rm etl python src/missing_number.py --extract 37
```

*(El CLI acepta `--extract N` para simular la extracción y luego calcula el faltante.)*

---

## 5) pgAdmin (opcional)

1. Abrir `http://localhost:8080`, login con `.env`.
2. Add New Server → **Host**: `db`, **Port**: `5432`, **User**: `postgres`, **Password**: `postgres`.
3. Explora esquemas/tablas (`raw.raw_purchases`, `public.companies`, `public.charges`, vista `v_daily_totals`).

---


## 6) Limpieza

```bash
docker compose down -v
```

---

## Observaciones importantes y Analisis

### Sección 1
* **ETL robusto y repetible**  
  El ETL está preparado para ejecutarse múltiples veces sin duplicar datos finales. Para manejar tipos, el `.csv` se cargó primero en **pandas** y de ahí a Postgres, lo que facilita inspeccionar y tipar correctamente durante el ingreso. Hubo complicaciones con la columna **`amount`**: algunos valores eran muy grandes, así que en **RAW** se guardaron como **String** y se procesaron como número en **Transformación**, con validaciones y límites antes de cargar en **`charges.amount DECIMAL(16,2)`**.  

* **Lenguaje: Python**  
  Elegí **Python** porque tiene librerías muy sólidas para el tratamiento de datos (**pandas, sqlalchemy**) y porque es un lenguaje rápido de escribir, ampliamente usado en ciencia de datos e ingeniería de datos. Esto permitió implementar el pipeline de extracción y transformación de manera clara y legible.  

* **Formato: CSV**  
  Elegí mantener la extracción final en **CSV** porque el dataset era relativamente pequeño y tabular, lo que hace que CSV sea suficiente y universalmente compatible. Aunque formatos como Parquet hubieran sido más eficientes para grandes volúmenes, aquí primaba la **simplicidad** y facilidad de inspección de resultados.  

* **Retos durante la extracción**  
  Durante la fase de extracción, uno de los principales retos fue la **homogeneidad de tipos de datos**:  
  - `amount` tenía valores demasiado grandes para el tipo definido inicialmente en Postgres.  
  - Algunas fechas (`created_at`, `paid_at`) venían vacías o con distintos formatos, lo que requirió parseo y validación.  
  Estos casos se resolvieron cargando primero a pandas y luego normalizando antes de escribir en la base final.  

* **Transformación de datos**  
  En esta fase se aplicaron reglas de limpieza adicionales:  
  - Se descartaron valores corruptos en columnas clave (`id`, `company_id`, `status`).  

* **Elección por Postgres**  
  La elección viene por diversos motivos: se trata una base de datos relacional madura, ampliamente usada en la industria, que garantiza integridad y consistencia de los datos, así como consultas analíticas complejas fácilmente, además de ser accesible para todos por ser open source, así como también la simplicidad de utilizarla para los puntos 1.1 Carga de informacion y 1.4 Dispersión  



### Sección 2
* **Eficiencia en tiempo/espacio**  
  Evalué distintas opciones y usé el enfoque con **XOR** para calcular el número faltante en **O(1)** de espacio y **O(n)** de tiempo, lo que es óptimo y simple. Incluyo a continuación fuentes que consulté.  



### Decisión por Stack
* Dado que el tiempo se consideraba libre, opté por utilizar herramientas más robustas (que suelen usarse en un entorno de producción) como lo son PostgreSQL, pgAdmin y Docker. De ser por un tiempo más limitado habría optado por Notebooks de Python (Google Colab) y SQLite como base de datos, que permiten hacer el trabajo para esta prueba, pero consideraba que podía mostrar más mis habilidades con un stack más amplio.  


---

## Fuentes

* [Find the missing number in an array: Optimal Approach 2](https://takeuforward.org/arrays/find-the-missing-number-in-an-array/)

---

## Notas finales

* El proyecto se puede ejecutar en cualquier SO con Docker.
* Las credenciales están en `.env` para simplificar la reproducción local.
* Por simplicidad, el dataset fue renombrado y se encuentra como: `datasets/input.csv`.
