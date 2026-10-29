"""Genera un catalogo Excel de la arquitectura y el diccionario de datos.

Inspecciona las colecciones reales de MongoDB Atlas, toma un documento de muestra,
aplana objetos anidados, detecta indices y exporta dos hojas Excel:

    - Resumen Arquitectonico
    - Diccionario de Datos

Ejecutar desde la raiz:
    python scripts/generar_catalogo.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from bson import ObjectId

sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb


PROJECT_ROOT = Path(__file__).resolve().parents[1]

COLLECTION_DESCRIPTIONS = {
    "movies": "Catalogo OLTP de peliculas, generos, ratings, reparto y premios.",
    "comments": "Comentarios de usuarios asociados a peliculas.",
    "users": "Usuarios registrados del dataset sample_mflix.",
    "theaters": "Cines con informacion de ubicacion y geolocalizacion.",
    "sessions": "Sesiones del dump original de sample_mflix.",
    "embedded_movies": "Version embebida del catalogo incluida en el dump original.",
    "peliculas_analiticas": "Capa OLAP desnormalizada y enriquecida desde movies/comments.",
    "dim_tiempo": "Dimension calendario derivada de las fechas de comentarios.",
    "dim_usuario": "Dimension de usuarios que comentan, agregada por email.",
    "dim_pelicula": "Dimension analitica del catalogo de peliculas.",
    "fact_comentarios": "Hechos de comentarios con llaves hacia las dimensiones.",
    "agg_comentarios_diarios": "Cubo precalculado de comentarios por dia.",
    "agg_genero_decada": "Cubo precalculado por genero y decada.",
    "agg_genero_top": "Top de peliculas por genero.",
    "agg_pais_rating": "Rating promedio y cantidad de peliculas por pais.",
    "agg_genero_premios": "Tasa de peliculas premiadas por genero.",
}

FIELD_DESCRIPTIONS = {
    "_id": "Identificador interno unico de MongoDB.",
    "movie_id": "Identificador de la pelicula relacionada.",
    "pelicula_id": "Llave hacia la dimension de peliculas.",
    "comentario_id": "Identificador unico del comentario.",
    "usuario_id": "Llave natural del usuario, normalmente el email.",
    "fecha_id": "Fecha de negocio en formato YYYY-MM-DD.",
    "title": "Titulo original de la pelicula.",
    "titulo": "Titulo normalizado de la pelicula.",
    "year": "Anio original de lanzamiento.",
    "anio": "Anio normalizado para analitica.",
    "genres": "Lista de generos de la pelicula.",
    "generos": "Lista normalizada de generos.",
    "genero": "Genero individual usado en una agregacion.",
    "date": "Fecha en que se publico el comentario.",
    "fecha_estreno": "Fecha de estreno de la pelicula.",
    "fecha_carga": "Fecha UTC en que se genero la particion del Data Lake.",
    "email": "Correo del usuario que realizo el comentario.",
    "name": "Nombre mostrado por el usuario.",
    "nombre": "Nombre normalizado o descriptivo.",
    "text": "Contenido textual del comentario.",
    "calificacion_imdb": "Calificacion IMDb normalizada.",
    "votos_imdb": "Cantidad de votos recibidos en IMDb.",
    "total_comentarios": "Cantidad total de comentarios relacionados.",
    "cantidad": "Medida numerica de hechos, normalmente uno por comentario.",
    "longitud_texto": "Cantidad de caracteres del comentario.",
    "es_fin_de_semana": "Indica si la fecha corresponde a sabado o domingo.",
    "segmento": "Segmento de actividad calculado para el usuario.",
}


def classify_collection(name: str) -> str:
    if name in {"movies", "comments", "users", "theaters", "sessions", "embedded_movies"}:
        return "OLTP"
    if name == "peliculas_analiticas":
        return "OLAP"
    if name.startswith(("dim_", "fact_", "agg_")):
        return "Data Warehouse"
    return "Otra"


def flatten_document(value: Any, prefix: str = "") -> dict[str, Any]:
    """Aplana solo objetos; conserva arrays como un campo Array."""
    if not isinstance(value, dict):
        return {prefix: value}

    flattened: dict[str, Any] = {}
    for key, child in value.items():
        field = f"{prefix}.{key}" if prefix else key
        if isinstance(child, dict):
            flattened.update(flatten_document(child, field))
        else:
            flattened[field] = child
    return flattened


def infer_type(value: Any) -> str:
    if value is None:
        return "Unknown"
    if isinstance(value, ObjectId):
        return "String (UUID)"
    if isinstance(value, datetime):
        return "Timestamp"
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Double"
    if isinstance(value, list):
        return "Array"
    if isinstance(value, dict):
        return "Object"
    return "String"


def collect_index_fields(collection) -> set[str]:
    fields: set[str] = set()
    for index in collection.list_indexes():
        fields.update(index.get("key", {}).keys())
    return fields


def is_foreign_key(field: str) -> str:
    if field == "_id":
        return "No"
    return "Si" if field.split(".")[-1].endswith("_id") else "No"


def field_description(field: str, collection: str) -> str:
    short_name = field.split(".")[-1]
    if short_name in FIELD_DESCRIPTIONS:
        return FIELD_DESCRIPTIONS[short_name]
    if short_name.startswith("fecha_"):
        return "Fecha derivada o de control del proceso de datos."
    if short_name.startswith("calificacion_"):
        return "Calificacion normalizada para analitica."
    if short_name.startswith("total_"):
        return "Metrica agregada de cantidad total."
    return f"Campo de la coleccion {collection}."


def collection_stats(db, collection_name: str) -> tuple[int, float]:
    collection = db[collection_name]
    documents = collection.estimated_document_count()
    try:
        stats = db.command("collStats", collection_name)
        size_bytes = stats.get("size", stats.get("storageSize", 0))
    except Exception:
        size_bytes = 0
    return documents, round(size_bytes / 1024 / 1024, 2)


def build_catalog(db) -> tuple[pd.DataFrame, pd.DataFrame]:
    architecture_rows: list[dict[str, Any]] = []
    dictionary_rows: list[dict[str, Any]] = []

    collection_names = sorted(db.list_collection_names())
    for collection_name in collection_names:
        collection = db[collection_name]
        documents, size_mb = collection_stats(db, collection_name)
        index_fields = collect_index_fields(collection)
        sample = collection.find_one() or {}
        flattened = flatten_document(sample)

        architecture_rows.append({
            "Coleccion": collection_name,
            "Zona Arquitectonica": classify_collection(collection_name),
            "Cantidad de Documentos": documents,
            "Tamano Estimado (MB)": size_mb,
            "Campos Detectados": len(flattened),
            "Indices Detectados": len(index_fields),
            "Descripcion": COLLECTION_DESCRIPTIONS.get(
                collection_name, f"Coleccion {collection_name} detectada en Atlas."
            ),
        })

        for field, value in sorted(flattened.items()):
            short_name = field.split(".")[-1]
            dictionary_rows.append({
                "Coleccion": collection_name,
                "Campo": field,
                "Tipo de Dato": infer_type(value),
                "Es Primary Key": "Si" if field == "_id" else "No",
                "Es Foreign Key": is_foreign_key(field),
                "Esta Indexado": "Si" if field in index_fields else "No",
                "Valor de Muestra": "[Array]" if isinstance(value, list) else str(value)[:120],
                "Descripcion de Negocio": field_description(short_name, collection_name),
            })

    return pd.DataFrame(architecture_rows), pd.DataFrame(dictionary_rows)


def format_workbook(output: Path) -> None:
    from openpyxl.styles import Alignment, Font, PatternFill

    with pd.ExcelFile(output, engine="openpyxl") as workbook:
        sheet_names = workbook.sheet_names

    from openpyxl import load_workbook

    workbook = load_workbook(output)
    header_fill = PatternFill("solid", fgColor="1F4E78")
    for sheet_name in sheet_names:
        sheet = workbook[sheet_name]
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
        for cell in sheet[1]:
            cell.font = Font(color="FFFFFF", bold=True)
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")
        for column_cells in sheet.columns:
            column_letter = column_cells[0].column_letter
            maximum = max(len(str(cell.value or "")) for cell in column_cells[:100])
            sheet.column_dimensions[column_letter].width = min(max(maximum + 2, 12), 55)
    workbook.save(output)


def export_catalog(architecture: pd.DataFrame, dictionary: pd.DataFrame, output: Path) -> None:
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        architecture.to_excel(writer, sheet_name="Resumen Arquitectonico", index=False)
        dictionary.to_excel(writer, sheet_name="Diccionario de Datos", index=False)
    format_workbook(output)


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera el catalogo Excel de sample_mflix.")
    parser.add_argument("--output", type=Path, default=None, help="Ruta opcional del XLSX de salida.")
    args = parser.parse_args()
    output = args.output or PROJECT_ROOT / f"Catalogo_Datos_MongoDataLake_{datetime.now():%Y%m%d}.xlsx"

    cliente, db = conectar_mongodb()
    try:
        print(f"[CATALOGO] Inspeccionando {len(db.list_collection_names())} colecciones...")
        architecture, dictionary = build_catalog(db)
        export_catalog(architecture, dictionary, output)
        print(f"[CATALOGO] Archivo generado: {output.relative_to(PROJECT_ROOT)}")
        print(f"[CATALOGO] Colecciones: {len(architecture)} | Campos: {len(dictionary)}")
    finally:
        cliente.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())