# 📊 🇲🇽 f_pipeline — Empleo Formal en México (IMSS 2019–2026)

Dashboard web con datos históricos de empleo formal en México, extraídos de los archivos de Datos Abiertos del IMSS, procesados con un pipeline ETL profesional y desplegados en Streamlit Community Cloud.

**[→ Ver dashboard en vivo](https://sch-fpipeline.streamlit.app/)**

---

## ¿Qué muestra este dashboard?

87 meses de empleo formal en México (enero 2019 – marzo 2026), desagregados por sector económico según el catálogo de clasificación de empresas del IMSS. Las visualizaciones permiten explorar:

- Evolución del empleo total con el período de pandemia marcado (marzo 2020 – mayo 2023)
- Comparación de crecimiento por sector entre 2019 y el último mes disponible
- Calidad del empleo: permanentes vs eventuales por sector
- Masa salarial acumulada por sector económico

---

## Fuente de datos

**[Datos Abiertos IMSS](http://datos.imss.gob.mx)** — Dataset ASG (Asegurados por sector económico). Archivos CSV mensuales con puestos de trabajo registrados ante el IMSS, desagregados por sector económico, entidad federativa, municipio, sexo, rango de edad y rango salarial.

**Cobertura actual:** enero 2019 – marzo 2026 (87 meses · 9 sectores económicos)

**Sectores cubiertos:**

| Código | Sector |
|---| ---|
| 0 | Agricultura, ganadería y pesca |
| 1 | Industrias extractivas |
| 2 | Industrias de transformación |
| 3 | Industria de la construcción |
| 4 | Electricidad y agua |
| 5 | Comercio |
| 6 | Transportes y comunicaciones |
| 7 | Servicios a empresas y personas |
| 8 | Servicios sociales y comunales |
| 9 | No especificado |

---

## Stack tecnológico

| Capa | Herramienta |
|---|---|
| Lenguaje | Python 3.12 |
| Entorno local | Conda (conda-forge) |
| Procesamiento de datos | pandas, pyarrow |
| Base de datos local | DuckDB |
| Dashboard | Streamlit, Plotly |
| Control de versiones | Git / GitHub |
| Deploy | Streamlit Community Cloud |
| OS de desarrollo | WSL2 Ubuntu / Windows 11 |

---

## Estructura del proyecto

```
f_pipeline/
├── src/
│   ├── __init__.py            # hace src/ importable como paquete
│   ├── extract.py             # extracción genérica desde API REST
│   ├── transform.py           # transformación y validación de datos
│   ├── load.py                # carga en DuckDB (uso local)
│   ├── pipeline.py            # orquestador: encadena los pasos ETL
│   └── dashboard.py           # dashboard Streamlit — empleo formal IMSS
├── tools/
│   ├── download_and_process_asg.py   # descarga y procesa CSVs del IMSS por rango de años
│   └── process_imss_asg.py           # procesa un CSV individual del IMSS
├── data/
│   ├── raw/                   # JSONs crudos de extracciones genéricas
│   └── processed/
│       └── imss_asg_historico.parquet  # historial de empleo IMSS (commiteado)
├── logs/
│   └── pipeline.log           # registro de ejecuciones locales
├── .github/
│   └── workflows/
│       └── pipeline.yml       # GitHub Actions (solo ejecución manual)
├── .streamlit/
│   └── config.toml            # configuración Streamlit (headless, puerto)
├── requirements.txt           # dependencias para Streamlit Cloud y Actions
├── environment.yml            # entorno conda para desarrollo local *
├── .env.example               # plantilla de variables de entorno
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
git clone https://github.com/SchroeDEV2356/f_pipeline.git
cd f_pipeline
 
# 2. Crear y activar el entorno conda
conda env create -f environment.yml
conda activate first_pipeline
 
# 3. Configurar variables de entorno
cp .env.example .env
 
# 4. Crear carpetas necesarias
mkdir -p data/raw data/processed logs
```

---

## Uso

### Ver el dashboard localmente

```bash
conda activate first_pipeline
streamlit run src/dashboard.py
# Abre http://localhost:8501 en el navegador
```

### Actualizar datos del IMSS (mensual)

El IMSS publica el archivo de cada mes aproximadamente el día 6 del mes siguiente. Para incorporar un nuevo mes al historial:

```bash
conda activate first_pipeline
cd ~/f_pipeline
 
# Descarga y procesa los meses pendientes del año actual
python tools/download_and_process_asg.py 2026 2026
 
# Subir el Parquet actualizado al repositorio
git add -f data/processed/imss_asg_historico.parquet
git commit -m "data: añadir empleo IMSS [mes] 2026"
git push
```

Streamlit Cloud detecta el nuevo commit y actualiza el dashboard automáticamente.

### Procesar un CSV descargado manualmente

Si prefieres descargar el archivo tú mismo desde `datos.imss.gob.mx`:

```bash
python tools/process_imss_asg.py ruta/al/asg-2026-04-30.csv
```

El script agrega 4.6 millones de filas a 9 filas (una por sector), añade el resultado al Parquet histórico y te indica cuándo puedes borrar el CSV original.

---

## Flujo de datos

```
IMSS datos.imss.gob.mx
  → CSV mensual (~390 MB por mes)
    → tools/download_and_process_asg.py
      → agregación: 4.6M filas → 9 filas por sector
        → data/processed/imss_asg_historico.parquet (~40 KB)
          → git push
            → Streamlit Cloud actualiza el dashboard
```

---

## Catálogo de columnas del Parquet histórico

| Columna | Descripción |
|---|---|
| `sector_economico_1` | Código numérico del sector (0–9) |
| `sector_nombre` | Nombre legible del sector |
| `asegurados` | Total de personas con relación laboral registrada |
| `ta` | Puestos de trabajo afiliados totales |
| `teu` | Trabajadores eventuales urbanos |
| `tec` | Trabajadores eventuales del campo |
| `tpu` | Trabajadores permanentes urbanos |
| `tpc` | Trabajadores permanentes del campo |
| `masa_sal_ta` | Masa salarial base de cotización |
| `eventuales` | `teu + tec` (calculado) |
| `permanentes` | `tpu + tpc` (calculado) |
| `pct_eventual` | % de empleo eventual sobre el total (calculado) |
| `anio` | Año del registro |
| `mes` | Mes del registro |
| `periodo` | Fecha como timestamp (día 1 de cada mes) |

---

## Decisiones de diseño

**¿Por qué agregar en el script y no guardar el CSV completo?**
Cada archivo CSV del IMSS pesa ~390 MB y contiene 4.6 millones de filas con granularidad por municipio, consultorio, sexo y rango de edad. Para tendencias nacionales por sector esa granularidad es innecesaria. La agregación a 9 filas por mes reduce el dato de 390 MB a ~500 bytes, haciendo viable mantener 87 meses en un Parquet de 40 KB.

**¿Por qué Parquet commiteado en el repo y no una base de datos en la nube?**
Streamlit Community Cloud (plan gratuito) no incluye base de datos persistente. Commitear el Parquet al repositorio es la solución más simple y confiable: GitHub actúa como almacén de datos versionado, cualquier colaborador puede reproducir el dashboard clonando el repo, y no hay dependencias de servicios externos.

**¿Por qué el workflow de GitHub Actions es solo manual?**
El IMSS no tiene una API REST que permita consultas automáticas — sus datos son archivos mensuales que se publican manualmente. El pipeline automático no aplica para esta fuente. El workflow queda disponible para ejecución manual en caso de necesitar correr tareas puntuales en el entorno de GitHub.

**¿Por qué `tools/` y no `src/`?**
Los scripts de descarga y procesamiento son herramientas de operación — se corren manualmente cuando hay datos nuevos. No forman parte del flujo de la aplicación web. Separarlos en `tools/` deja claro que son utilidades de mantenimiento, no componentes del dashboard.

---

## Actualización de datos

| Frecuencia | Acción |
|---|---|
| Mensual (~día 6) | Correr `download_and_process_asg.py` y hacer push del Parquet |
| Anual (enero) | Correr `download_and_process_asg.py YYYY YYYY` para el año nuevo |

---

## Licencia

MIT
