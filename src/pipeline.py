"""
pipeline.py — Orquestador principal
=====================================
Qué hace: ejecuta extract → transform → load en orden,
          deteniéndose si cualquier paso falla.

Por qué un orquestador separado y no un solo script grande:
  - Cada script sigue haciendo UNA sola cosa y puede correrse solo.
  - Este archivo es el "director": sabe el orden, captura errores
    de cada etapa por separado, y reporta exactamente dónde falló.
  - Cuando en el futuro añadas un Paso 4.5 (ej. un reporte por email),
    solo añades una línea aquí sin tocar nada más.

Cómo correrlo:
  python src/pipeline.py
"""

import logging
import time
from datetime import datetime, timezone

# Importamos las funciones run() de cada módulo.
# Cada una hace su trabajo y lanza una excepción si algo sale mal.
import src.extract   as extract_mod
import src.transform as transform_mod
import src.load      as load_mod


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log"),
    ],
)
log = logging.getLogger(__name__)


def run_pipeline() -> None:
    inicio = time.perf_counter()
    log.info("╔══════════════════════════════════════════╗")
    log.info("║   PIPELINE COMPLETO — %s   ║",
             datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    log.info("╚══════════════════════════════════════════╝")

    pasos = [
        ("Extracción",     extract_mod.run),
        ("Transformación", transform_mod.run),
        ("Carga",          load_mod.run),
    ]

    for nombre, fn in pasos:
        try:
            fn()
        except Exception as e:
            # Si un paso falla, registramos el error y detenemos todo.
            # No tiene sentido transformar si la extracción falló,
            # ni cargar si la transformación produjo datos corruptos.
            log.error("Pipeline detenido en '%s': %s", nombre, e)
            raise SystemExit(1)   # código de salida 1 = error (GitHub Actions lo detecta)

    elapsed = time.perf_counter() - inicio
    log.info("Pipeline completado en %.2f s", elapsed)


if __name__ == "__main__":
    run_pipeline()