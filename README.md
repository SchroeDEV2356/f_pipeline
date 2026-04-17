# 📊 f_pipeline — Pipeline ETL + Dashboard de Criptomonedas

Dashboard web con datos en vivo extraídos automáticamente desde la API pública de CoinGecko, procesados con un pipeline ETL profesional y desplegados en Streamlit Community Cloud.

**[→ Ver dashboard en vivo](https://TU_URL.streamlit.app)**

---

## ¿Qué hace este proyecto?

Cada 6 horas, un pipeline automatizado:

1. **Extrae** los datos de las top 50 criptomonedas por capitalización desde CoinGecko
2. **Transforma** y valida los datos (tipado, limpieza, control de calidad)
3. **Guarda** el resultado como Parquet en el repositorio
4. **Actualiza** el dashboard público automáticamente

Todo sin intervención manual.

---

## Stack tecnológico

| Capa | Herramienta |
|---|---|
| Lenguaje | Python 3.12 |
| Entorno local | Conda (conda-forge) |
| HTTP / Extracción | requests, urllib3 |
| Transformación | pandas, pyarrow |
| Base de datos local | DuckDB |
| Dashboard | Streamlit, Plotly |
| Automatización | GitHub Actions |
| Deploy | Streamlit Community Cloud |
| OS de desarrollo | WSL2 Ubuntu / Windows 11 |

---

## Estructura del proyecto

```
f_pipeline/
├── src/
│   ├── __init__.py        # hace src/ importable como paquete
│   ├── extract.py         # Paso 1: extracción desde CoinGecko API
│   ├── transform.py       # Paso 2: limpieza, tipado y validación
│   ├── load.py            # Paso 3: carga en DuckDB (uso local)
│   ├── pipeline.py        # Orquestador: encadena los 3 pasos
│   └── dashboard.py       # Dashboard Streamlit (local + cloud)
├── data/
│   ├── raw/               # JSON crudos de cada extracción
│   └── processed/         # Parquet limpios (commiteados por Actions)
├── logs/
│   └── pipeline.log       # Registro de ejecuciones locales
├── .github/
│   └── workflows/
│       └── pipeline.yml   # GitHub Actions: schedule cada 6 horas
├── .streamlit/
│   └── config.toml        # Configuración de Streamlit (headless, puerto)
├── requirements.txt       # Dependencias para Streamlit Cloud y Actions
├── environment.yml        # Entorno conda para desarrollo local *
├── .env.example           # Plantilla de variables de entorno
└── .gitignore
```

> \* `environment.yml` vive solo en tu máquina local — no se sube al repositorio.

---

## Instalación local

### Prerequisitos

- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) o Anaconda
- Git

### Pasos

```bash
# 1. Clonar el repositorio
git clone https://github.com/TU_USUARIO/f_pipeline.git
cd f_pipeline

# 2. Crear y activar el entorno conda
conda env create -f environment.yml
conda activate first_pipeline

# 3. Configurar variables de entorno
cp .env.example .env
# El archivo .env ya tiene valores por defecto funcionales;
# no necesitas modificar nada para empezar.

# 4. Crear carpetas de datos y logs
mkdir -p data/raw data/processed logs
```

---

## Uso

### Correr el pipeline completo

```bash
# Desde la raíz del proyecto, con el entorno activado
python -m src.pipeline
```

Esto ejecuta extracción → transformación → carga en orden.  
Si cualquier paso falla, el pipeline se detiene y registra el error en `logs/pipeline.log`.

### Correr pasos individuales

```bash
python src/extract.py      # solo extracción
python src/transform.py    # solo transformación
python src/load.py         # solo carga en DuckDB
```

### Lanzar el dashboard localmente

```bash
streamlit run src/dashboard.py
# Abre http://localhost:8501 en el navegador
```

---

## Automatización (GitHub Actions)

El archivo `.github/workflows/pipeline.yml` define un job que corre automáticamente:

- **Cada 6 horas** (schedule cron: `0 */6 * * *` UTC)
- **Manualmente** desde GitHub → Actions → *Run workflow*

El job instala dependencias, corre `extract.py` y `transform.py`, y commitea el Parquet resultante al repositorio. Streamlit Cloud detecta el nuevo commit y actualiza el dashboard automáticamente.

Para cambiar la frecuencia, edita la línea `cron:` en `pipeline.yml`:

```yaml
- cron: "0 */6 * * *"    # cada 6 horas (actual)
- cron: "0 8 * * *"      # cada día a las 08:00 UTC
- cron: "0 8 * * 1"      # cada lunes a las 08:00 UTC
```

---

## Variables de entorno

Copia `.env.example` como `.env` y ajusta si es necesario:

```env
API_BASE_URL=https://api.coingecko.com/api/v3
COINS_LIMIT=50
RAW_DATA_DIR=data/raw
PROCESSED_DATA_DIR=data/processed
DB_PATH=data/pipeline.duckdb
```

El archivo `.env` nunca se sube a Git (está en `.gitignore`).

---

## Fuente de datos

**[CoinGecko API](https://docs.coingecko.com)** — API pública y gratuita, sin necesidad de registro ni API key. Endpoint utilizado: `/coins/markets`.

Campos extraídos por moneda: `id`, `symbol`, `name`, `current_price`, `market_cap`, `market_cap_rank`, `total_volume`, `high_24h`, `low_24h`, `price_change_percentage_24h`, `circulating_supply`, `last_updated`.

---

## Decisiones de diseño

**¿Por qué Parquet y no CSV?**
Parquet preserva tipos de dato (float, datetime, int nullable), comprime automáticamente (~70% menos peso que CSV) y es el formato estándar de la industria de datos.

**¿Por qué DuckDB localmente?**
Base de datos analítica embebida en un solo archivo, sin servidor. Permite SQL completo sobre los datos acumulados y es ideal para dashboards de escala media.

**¿Por qué el dashboard lee Parquet en la nube?**
Streamlit Cloud no tiene acceso a la base de datos local. GitHub Actions commitea el Parquet al repositorio y el dashboard lo lee directamente — sin necesidad de servidor de base de datos en producción.

**¿Por qué `INSERT OR IGNORE` con clave `(id, last_updated)`?**
`id` solo no es único porque la misma moneda aparece en cada extracción. La clave compuesta garantiza que solo entran registros genuinamente nuevos, haciendo el pipeline idempotente.

---

## Licencia

MIT
