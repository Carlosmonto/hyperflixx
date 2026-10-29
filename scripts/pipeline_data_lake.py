"""Pipeline de Data Lake: MongoDB Atlas -> RAW -> PROCESSED -> CURATED.

Ejecutar desde la raiz del proyecto:
    python scripts/pipeline_data_lake.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from bson import json_util

sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb
from data_lake_storage import DataLakeStorage


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]


def clave_particion(zona: str, fecha_carga: str, nombre_archivo: str) -> str:
    return f"{zona}/movies/fecha_carga={fecha_carga}/{nombre_archivo}"


def extraer_raw(db, almacenamiento: DataLakeStorage, fecha_carga: str) -> str:
    """Extrae movies sin transformaciones; BSON extendido conserva ObjectId y fechas."""
    clave = clave_particion("raw", fecha_carga, "movies.jsonl")
    almacenamiento.write_lines(
        clave,
        (json_util.dumps(documento) for documento in db.movies.find({})),
    )
    print(f"[RAW] Archivo original guardado en: {almacenamiento.uri_for(clave)}")
    return clave


def leer_raw(almacenamiento: DataLakeStorage, clave: str) -> list[dict]:
    """Lee el JSON Lines BSON extendido que se generó en RAW."""
    return [json_util.loads(linea) for linea in almacenamiento.iter_lines(clave) if linea.strip()]


def transformar_processed(registros: list[dict]) -> pd.DataFrame:
    """Limpia, aplana y tipa los datos sin agregar métricas de negocio."""
    df = pd.json_normalize(registros, sep="_")
    columnas = [
        "_id", "title", "year", "released", "runtime", "type", "rated", "genres",
        "countries", "languages", "imdb_rating", "imdb_votes", "tomatoes_viewer_rating",
        "tomatoes_viewer_numReviews", "awards_wins", "awards_nominations",
    ]
    df = df.reindex(columns=columnas)
    df = df.rename(columns={
        "_id": "movie_id", "title": "titulo", "year": "anio", "released": "fecha_estreno",
        "runtime": "duracion_minutos", "type": "tipo", "rated": "clasificacion",
        "genres": "generos", "countries": "paises", "languages": "idiomas",
        "imdb_rating": "calificacion_imdb", "imdb_votes": "votos_imdb",
        "tomatoes_viewer_rating": "calificacion_audiencia",
        "tomatoes_viewer_numReviews": "resenas_audiencia",
        "awards_wins": "premios_ganados", "awards_nominations": "nominaciones",
    })
    df["movie_id"] = df["movie_id"].astype(str)
    df["titulo"] = df["titulo"].fillna("Sin titulo").astype(str).str.strip()
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["fecha_estreno"] = pd.to_datetime(df["fecha_estreno"], errors="coerce", utc=True)
    for columna in ["duracion_minutos", "votos_imdb", "resenas_audiencia", "premios_ganados", "nominaciones"]:
        df[columna] = pd.to_numeric(df[columna], errors="coerce").fillna(0).astype("int64")
    for columna in ["calificacion_imdb", "calificacion_audiencia"]:
        df[columna] = pd.to_numeric(df[columna], errors="coerce").fillna(0.0)
    for columna in ["generos", "paises", "idiomas"]:
        df[columna] = df[columna].apply(lambda valor: valor if isinstance(valor, list) else [])
    return df.drop_duplicates(subset="movie_id")


def construir_curated(df: pd.DataFrame, fecha_carga: str) -> pd.DataFrame:
    """Crea una tabla analitica con dimensiones simples y KPIs para BI."""
    curated = df.copy()
    curated["genero_principal"] = curated["generos"].apply(lambda valores: valores[0] if valores else "Sin genero")
    curated["pais_principal"] = curated["paises"].apply(lambda valores: valores[0] if valores else "Sin pais")
    curated["decada"] = (curated["anio"] // 10 * 10).astype("Int64")
    curated["popularidad_imdb"] = curated["calificacion_imdb"] * curated["votos_imdb"]
    curated["fecha_carga"] = fecha_carga
    return curated[
        ["movie_id", "titulo", "anio", "decada", "fecha_estreno", "tipo", "clasificacion",
         "duracion_minutos", "genero_principal", "pais_principal", "calificacion_imdb",
         "votos_imdb", "calificacion_audiencia", "resenas_audiencia", "premios_ganados",
         "nominaciones", "popularidad_imdb", "fecha_carga"]
    ]


def guardar_parquet(
    df: pd.DataFrame, almacenamiento: DataLakeStorage, zona: str, fecha_carga: str
) -> str:
    clave = clave_particion(zona, fecha_carga, "movies.parquet")
    almacenamiento.write_parquet(df, clave)
    print(f"[{zona.upper()}] {len(df)} registros guardados en: {almacenamiento.uri_for(clave)}")
    return clave


def ejecutar_pipeline() -> None:
    fecha_carga = datetime.now(timezone.utc).date().isoformat()
    almacenamiento = DataLakeStorage()
    print(f"[PIPELINE] Backend del Data Lake: {almacenamiento.backend}")
    preferencia_lectura = os.getenv("DATA_LAKE_READ_PREFERENCE", "secondaryPreferred")
    cliente, db = conectar_mongodb(read_preference=preferencia_lectura, validar=False)
    try:
        raw = extraer_raw(db, almacenamiento, fecha_carga)
        processed = transformar_processed(leer_raw(almacenamiento, raw))
        guardar_parquet(processed, almacenamiento, "processed", fecha_carga)
        guardar_parquet(construir_curated(processed, fecha_carga), almacenamiento, "curated", fecha_carga)
        print("[PIPELINE] Finalizado: RAW, PROCESSED y CURATED disponibles para BI.")
    finally:
        cliente.close()


if __name__ == "__main__":
    ejecutar_pipeline()
