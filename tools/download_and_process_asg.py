"""
download_and_process_asg.py — Descarga y procesa archivos ASG del IMSS
=======================================================================
USO:
    python download_and_process_asg.py 2019 2025

    Eso descargará, procesará y borrará los 84 CSVs de enero 2019
    a diciembre 2025, dejando un solo Parquet acumulado en:
        data/processed/imss_asg_historico.parquet

CÓMO FUNCIONA:
    1. Construye la URL de cada mes usando el patrón conocido del IMSS
    2. Descarga el CSV a un archivo temporal en /tmp (no en Downloads)
    3. Agrega 4.6M filas → 9 filas por sector
    4. Añade al Parquet acumulado
    5. Borra el CSV temporal inmediatamente
    6. Pasa al siguiente mes

    En ningún momento tienes más de 1 CSV en disco (~500 MB pico).
    El Parquet crece ~7 KB por mes — menos de 1 MB al terminar.

TIEMPO ESTIMADO:
    ~20-40 min para 84 meses dependiendo de tu conexión.
    El script puede interrumpirse y retomarse: ya salta automáticamente
    los meses que detecta en el Parquet acumulado.
"""

import sys
import calendar
import logging
import tempfile
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────────────────
# CATÁLOGO DE SECTORES (mismo que en process_imss_asg.py)
# ─────────────────────────────────────────────────────────────────────────────

CATALOGO_SECTORES = {
    0: "Agricultura, ganadería y pesca",
    1: "Industrias extractivas",
    2: "Industrias de transformación",
    3: "Industria de la construcción",
    4: "Electricidad y agua",
    5: "Comercio",
    6: "Transportes y comunicaciones",
    7: "Servicios a empresas y personas",
    8: "Servicios sociales y comunales",
    9: "No especificado",
}


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SESIÓN HTTP CON REINTENTOS
# ─────────────────────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    """
    Sesión con reintentos automáticos y timeout.
    El IMSS a veces responde lento — la política de reintentos evita
    que el script se caiga por un error transitorio de red.
    backoff_factor=2 → espera 2s, 4s, 8s entre intentos.
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9",
        "Referer": "http://datos.imss.gob.mx/",
    })
    return session


# ─────────────────────────────────────────────────────────────────────────────
# CONSTRUCCIÓN DE URLs
# ─────────────────────────────────────────────────────────────────────────────

def ultimo_dia(anio: int, mes: int) -> int:
    """
    Devuelve el último día del mes.
    calendar.monthrange(2024, 2) devuelve (3, 29) — el 29 es el último día.
    Así manejamos automáticamente años bisiestos y meses de 28/30/31 días.
    """
    return calendar.monthrange(anio, mes)[1]


def construir_url(anio: int, mes: int) -> str:
    """
    Construye la URL de descarga del IMSS para un mes específico.
    Patrón verificado: http://datos.imss.gob.mx/sites/default/files/asg-YYYY-MM-DD.csv
    """
    dia = ultimo_dia(anio, mes)
    return f"http://datos.imss.gob.mx/sites/default/files/asg-{anio}-{mes:02d}-{dia:02d}.csv"


def generar_periodos(anio_inicio: int, anio_fin: int) -> list[tuple[int, int]]:
    """
    Genera lista de (año, mes) para todo el rango solicitado.
    No incluye meses futuros — se detiene en el mes actual.
    """
    from datetime import date
    hoy = date.today()
    periodos = []
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            # No intentar descargar meses que aún no existen
            if date(anio, mes, 1) >= date(hoy.year, hoy.month, 1):
                return periodos
            periodos.append((anio, mes))
    return periodos


# ─────────────────────────────────────────────────────────────────────────────
# DESCARGA
# ─────────────────────────────────────────────────────────────────────────────

def descargar_csv(session: requests.Session, url: str, dest: Path) -> bool:
    """
    Descarga el CSV a un archivo temporal con barra de progreso simple.
    Devuelve True si la descarga fue exitosa, False si el archivo no existe
    en el servidor (el IMSS no publica todos los meses en todos los años).

    stream=True: descarga en chunks para no cargar el archivo completo
    en memoria antes de guardarlo. Esencial para archivos de 500 MB.
    """
    try:
        r = session.get(url, timeout=120, stream=True)
        if r.status_code == 404:
            log.warning("Archivo no disponible en el servidor: %s", url)
            return False
        r.raise_for_status()

        total = int(r.headers.get("content-length", 0))
        descargado = 0

        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # chunks de 1 MB
                f.write(chunk)
                descargado += len(chunk)
                if total:
                    pct = descargado / total * 100
                    print(f"\r  Descargando... {descargado/1e6:.0f} MB / {total/1e6:.0f} MB ({pct:.0f}%)", end="", flush=True)

        print()  # salto de línea tras la barra de progreso
        log.info("Descarga completa: %.1f MB", dest.stat().st_size / 1e6)
        return True

    except requests.exceptions.RequestException as e:
        log.error("Error al descargar %s: %s", url, e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# PROCESAMIENTO (mismo núcleo que process_imss_asg.py)
# ─────────────────────────────────────────────────────────────────────────────

def procesar_csv(csv_path: Path, anio: int, mes: int) -> pd.DataFrame:
    cols_usar = [
        "sector_economico_1", "asegurados", "ta",
        "teu", "tec", "tpu", "tpc", "masa_sal_ta",
    ]
    df = pd.read_csv(
        csv_path, sep="|", encoding="latin1",
        low_memory=False, usecols=cols_usar,
    )
    for col in cols_usar[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["sector_economico_1"])
    df["sector_economico_1"] = df["sector_economico_1"].astype(int)

    agg = df.groupby("sector_economico_1")[cols_usar[1:]].sum().reset_index()
    agg["sector_nombre"] = agg["sector_economico_1"].map(CATALOGO_SECTORES).fillna("Desconocido")
    agg["anio"]    = anio
    agg["mes"]     = mes
    agg["periodo"] = pd.Timestamp(year=anio, month=mes, day=1)

    return agg


def guardar_acumulado(df_nuevo: pd.DataFrame, out_path: Path) -> int:
    if out_path.exists():
        df_total = pd.concat(
            [pd.read_parquet(out_path), df_nuevo], ignore_index=True
        ).drop_duplicates(subset=["anio", "mes", "sector_economico_1"], keep="last")
    else:
        df_total = df_nuevo

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_total.to_parquet(out_path, index=False, engine="pyarrow")
    return len(df_total)


# ─────────────────────────────────────────────────────────────────────────────
# LEER PERÍODOS YA PROCESADOS
# ─────────────────────────────────────────────────────────────────────────────

def periodos_existentes(out_path: Path) -> set[tuple[int, int]]:
    """
    Lee el Parquet acumulado y devuelve los (año, mes) ya procesados.
    Esto permite reanudar el script si se interrumpió: simplemente salta
    los meses que ya están en el Parquet.
    """
    if not out_path.exists():
        return set()
    df = pd.read_parquet(out_path, columns=["anio", "mes"])
    return set(zip(df["anio"].tolist(), df["mes"].tolist()))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 3:
        print("Uso: python download_and_process_asg.py AÑO_INICIO AÑO_FIN")
        print("Ejemplo: python download_and_process_asg.py 2019 2025")
        sys.exit(1)

    anio_inicio = int(sys.argv[1])
    anio_fin    = int(sys.argv[2])
    out_path    = Path("data/processed/imss_asg_historico.parquet")

    periodos  = generar_periodos(anio_inicio, anio_fin)
    existentes = periodos_existentes(out_path)
    pendientes = [p for p in periodos if p not in existentes]

    log.info("Períodos solicitados: %d | Ya procesados: %d | Pendientes: %d",
             len(periodos), len(existentes), len(pendientes))

    if not pendientes:
        log.info("Todo el rango ya está en el Parquet acumulado. Nada que hacer.")
        return

    session  = build_session()
    ok = 0
    errores  = []

    for i, (anio, mes) in enumerate(pendientes, 1):
        url = construir_url(anio, mes)
        log.info("[%d/%d] %d-%02d → %s", i, len(pendientes), anio, mes, url)

        # Archivo temporal — se borra automáticamente aunque el script falle
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            descargado = descargar_csv(session, url, tmp_path)
            if not descargado:
                errores.append((anio, mes, "404 - no disponible"))
                continue

            df_agg = procesar_csv(tmp_path, anio, mes)
            total  = guardar_acumulado(df_agg, out_path)
            log.info("✓ %d-%02d procesado | Parquet: %d filas totales", anio, mes, total)
            ok += 1

        except Exception as e:
            log.error("Error en %d-%02d: %s", anio, mes, e)
            errores.append((anio, mes, str(e)))

        finally:
            # Borrar siempre el CSV temporal, sin importar si hubo error
            if tmp_path.exists():
                tmp_path.unlink()

        # Pausa entre descargas — evita saturar el servidor del IMSS
        # y reduce el riesgo de que bloqueen las peticiones
        if i < len(pendientes):
            time.sleep(2)

    # ── Resumen final ──────────────────────────────────────────────────────
    print("\n" + "═" * 50)
    print(f"  Procesados exitosamente : {ok}")
    print(f"  Errores / no disponibles: {len(errores)}")
    if out_path.exists():
        df_final = pd.read_parquet(out_path)
        print(f"  Filas en el Parquet     : {len(df_final)}")
        print(f"  Tamaño del Parquet      : {out_path.stat().st_size / 1024:.1f} KB")
        periodos_ok = sorted(df_final.groupby(["anio", "mes"]).groups.keys())
        print(f"  Rango cubierto          : {periodos_ok[0][0]}-{periodos_ok[0][1]:02d} "
              f"→ {periodos_ok[-1][0]}-{periodos_ok[-1][1]:02d}")
    if errores:
        print("\n  Períodos con error:")
        for anio, mes, motivo in errores:
            print(f"    {anio}-{mes:02d}: {motivo}")
    print("═" * 50)


if __name__ == "__main__":
    main()