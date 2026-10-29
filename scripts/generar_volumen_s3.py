"""Genera un volumen demostrativo grande directamente en Amazon S3.

No inserta documentos en MongoDB ni modifica las zonas canonicas del pipeline.
Usa RAW existente como semilla y crea un objeto independiente con descripciones,
recomendaciones y contenido de prueba configurable.

Ejemplo:
    python scripts/generar_volumen_s3.py --source-date 2026-10-19 --target-gb 1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from bson import json_util

sys.path.append(str(Path(__file__).resolve().parents[1]))
from data_lake_storage import DataLakeStorage


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]


def argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera volumen de demostracion en S3")
    parser.add_argument(
        "--source-date",
        default=os.getenv("S3_DEMO_SOURCE_DATE"),
        required=not os.getenv("S3_DEMO_SOURCE_DATE"),
        help="Fecha de la particion RAW existente, por ejemplo 2026-10-19",
    )
    parser.add_argument(
        "--target-gb",
        type=float,
        default=float(os.getenv("S3_DEMO_TARGET_GB", "1")),
        help="Tamano objetivo en GiB; por defecto 1",
    )
    parser.add_argument(
        "--record-bytes",
        type=int,
        default=int(os.getenv("S3_DEMO_RECORD_BYTES", "32768")),
        help="Tamano aproximado del contenido adicional por registro",
    )
    return parser.parse_args()


def cargar_semillas(storage: DataLakeStorage, source_date: str) -> list[dict]:
    key = f"raw/movies/fecha_carga={source_date}/movies.jsonl"
    semillas = [
        json_util.loads(line)
        for line in storage.iter_lines(key)
        if line.strip()
    ]
    if not semillas:
        raise RuntimeError(f"No hay registros en s3://{storage.bucket}/{storage._key(key)}")
    print(f"[DEMO] Semillas leidas: {len(semillas)} desde {storage.uri_for(key)}")
    return semillas


def crear_registro(seed: dict, numero: int, record_bytes: int) -> dict:
    titulo = str(seed.get("title") or "Pelicula sin titulo")
    generos = seed.get("genres") or ["Sin genero"]
    descripcion = str(seed.get("fullplot") or seed.get("plot") or "Descripcion no disponible")
    recomendacion = [
        f"{titulo} - recomendacion relacionada {indice + 1} por genero {generos[0]}"
        for indice in range(5)
    ]
    contenido_base = (
        f"Ficha ampliada de {titulo}. Genero principal: {generos[0]}. "
        f"Descripcion: {descripcion}. Recomendaciones: {'; '.join(recomendacion)}. "
    )
    contenido = (contenido_base * ((record_bytes // len(contenido_base)) + 1))[:record_bytes]
    return {
        "demo_id": numero,
        "movie_id": str(seed.get("_id")),
        "titulo": titulo,
        "generos": generos,
        "descripcion": descripcion,
        "recomendaciones": recomendacion,
        "contenido_demo": contenido,
        "fecha_generacion_utc": datetime.now(timezone.utc).isoformat(),
    }


def generar_volumen(storage: DataLakeStorage, semillas: list[dict], target_bytes: int, record_bytes: int) -> str:
    fecha = datetime.now(timezone.utc).date().isoformat()
    key = f"raw/demo/movies/fecha_carga={fecha}/movies_demo.jsonl"
    temporal_path: Path | None = None
    registros = 0
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as temporal:
            temporal_path = Path(temporal.name)
            while temporal.tell() < target_bytes:
                seed = semillas[registros % len(semillas)]
                registro = crear_registro(seed, registros + 1, record_bytes)
                temporal.write(json.dumps(registro, ensure_ascii=False))
                temporal.write("\n")
                registros += 1
        storage.upload_file(str(temporal_path), key)
    finally:
        if temporal_path:
            temporal_path.unlink(missing_ok=True)
    print(f"[DEMO] Registros generados: {registros}")
    print(f"[DEMO] Volumen publicado en: {storage.uri_for(key)}")
    return key


def main() -> None:
    args = argumentos()
    if args.target_gb <= 0 or args.record_bytes <= 0:
        raise ValueError("target-gb y record-bytes deben ser mayores que cero.")
    storage = DataLakeStorage()
    semillas = cargar_semillas(storage, args.source_date)
    target_bytes = int(args.target_gb * 1024**3)
    generar_volumen(storage, semillas, target_bytes, args.record_bytes)


if __name__ == "__main__":
    main()
