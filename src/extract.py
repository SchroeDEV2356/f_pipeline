"""
extract.py — Paso 1: Extracción de datos
=========================================
Fuente : CoinGecko API (gratuita, sin registro ni API key)
Qué hace: descarga las top N criptomonedas por capitalización
          y guarda el JSON crudo en disco con timestamp.

Por qué CoinGecko:
  - Datos financieros reales que cambian cada minuto
  - No requiere cuenta ni key → ideal para aprender
  - Documentación clara: https://docs.coingecko.com
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
# load_dotenv() lee el archivo .env y mete cada línea como variable de entorno.
# os.getenv("X", "valor_por_defecto") lo usa, o usa el default si no existe.
# Esto nos permite cambiar el comportamiento SIN tocar el código.

load_dotenv()

BASE_URL    = os.getenv("API_BASE_URL", "https://api.coingecko.com/api/v3")
COINS_LIMIT = int(os.getenv("COINS_LIMIT", "50"))
RAW_DIR     = Path(os.getenv("RAW_DATA_DIR", "data/raw"))


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
# Por qué logging en vez de print():
#   - print() no guarda nada; si algo falla de madrugada, no hay rastro.
#   - logging escribe en consola Y en archivo simultáneamente.
#   - Cada línea lleva timestamp → puedes reconstruir exactamente qué pasó.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.StreamHandler(),                   # → consola
        logging.FileHandler("logs/pipeline.log"),  # → archivo
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SESIÓN HTTP CON REINTENTOS
# ─────────────────────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    """
    Crea una sesión HTTP reutilizable con política de reintentos automáticos.

    Por qué sesión y no requests.get() directo:
      - Reutiliza la conexión TCP → más rápido en múltiples llamadas.
      - Permite adjuntar headers y reintentos a TODOS los requests de una vez.

    Por qué reintentos:
      - Las APIs públicas limitan cuántas veces puedes llamarlas por minuto
        (rate limit). Si llegas al límite, responden con error 429.
      - La red puede fallar momentáneamente.
      - backoff_factor=1 significa que espera 1s, luego 2s, luego 4s
        antes de reintentar → no saturamos la API.
    """
    session = requests.Session()

    retry_policy = Retry(
        total=3,                              # máximo 3 intentos en total
        backoff_factor=1,                     # espera exponencial entre intentos
        status_forcelist=[429, 500, 502, 503, 504],  # códigos que merecen reintento
        allowed_methods=["GET"],              # solo reintentar GETs (son seguros)
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)

    # Identificarse como cliente es buena práctica; algunas APIs lo requieren.
    session.headers.update({"User-Agent": "first-pipeline/1.0"})

    return session


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN
# ─────────────────────────────────────────────────────────────────────────────

def fetch_markets(session: requests.Session) -> list[dict]:
    """
    Llama al endpoint /coins/markets y devuelve la lista de monedas.

    Regla de oro de la capa Extract:
      Devuelve los datos EXACTAMENTE como llegaron de la API, sin modificar.
      Si transformas aquí y hay un bug, no puedes saber si el error fue
      tuyo o de la fuente. Los datos crudos son la fuente de verdad.

    raise_for_status() convierte un error HTTP (404, 500…) en una excepción
    de Python, para que el bloque try/except del orquestador lo capture.
    """
    log.info("Iniciando extracción — endpoint: /coins/markets")

    params = {
        "vs_currency": "usd",          # precios en dólares
        "order":       "market_cap_desc",  # ordenadas por capitalización
        "per_page":    COINS_LIMIT,    # cuántas monedas traer
        "page":        1,
        "sparkline":   False,          # sin datos de minigrafica (ahorra peso)
    }

    response = session.get(
        f"{BASE_URL}/coins/markets",
        params=params,
        timeout=15,  # si la API tarda más de 15s, cancelamos y reintentamos
    )
    response.raise_for_status()

    data = response.json()
    log.info("Extracción exitosa — registros recibidos: %d", len(data))
    return data


# ─────────────────────────────────────────────────────────────────────────────
# PERSISTENCIA RAW
# ─────────────────────────────────────────────────────────────────────────────

def save_raw(data: list[dict]) -> Path:
    """
    Guarda los datos crudos en disco como JSON con timestamp en el nombre.

    Por qué timestamp en el nombre del archivo:
      - Cada ejecución crea un archivo NUEVO, nunca sobreescribe.
      - Tienes el historial completo de todas las extracciones.
      - Si algo sale mal en transformación, puedes volver al raw original.
      - ISO 8601 (20240115T143022Z) ordena bien alfabéticamente.

    Por qué JSON y no CSV aquí:
      - La API ya devuelve JSON; convertirlo ahora sería transformar.
      - JSON preserva tipos: números, nulos, listas anidadas.
      - En el Paso 3 (Transform) decidiremos el formato final.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath  = RAW_DIR / f"coingecko_markets_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log.info("Datos guardados en: %s  (%d bytes)", filepath, filepath.stat().st_size)
    return filepath


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Punto de entrada: llama a las funciones en orden y captura errores.

    Por qué separar run() de las funciones individuales:
      - Cada función hace UNA sola cosa (principio de responsabilidad única).
      - Puedes probar fetch_markets() sola sin guardar nada.
      - run() es el "director de orquesta"; las demás son músicos.

    Los except específicos van primero, el genérico al final.
    Así sabemos exactamente qué salió mal sin adivinar.
    """
    log.info("═══ Iniciando pipeline — Paso 1: Extracción ═══")
    start = time.perf_counter()

    try:
        session  = build_session()
        data     = fetch_markets(session)
        filepath = save_raw(data)

        elapsed = time.perf_counter() - start
        log.info("Paso 1 completado en %.2f s → %s", elapsed, filepath)

    except requests.exceptions.HTTPError as e:
        log.error("Error HTTP de la API: %s", e)
        raise

    except requests.exceptions.ConnectionError:
        log.error("Sin conexión o la API no está disponible")
        raise

    except requests.exceptions.Timeout:
        log.error("La API tardó más de 15 s en responder")
        raise

    except Exception as e:
        log.exception("Error inesperado: %s", e)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# EJECUCIÓN DIRECTA
# ─────────────────────────────────────────────────────────────────────────────
# if __name__ == "__main__" significa:
#   "Solo ejecuta esto si alguien corre el archivo directamente
#    (python extract.py), no si otro módulo lo importa."
# Esto permite reutilizar las funciones de arriba en otros scripts.

if __name__ == "__main__":
    run()
