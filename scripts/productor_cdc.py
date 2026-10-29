"""Productor CDC para MongoDB Atlas sobre sample_mflix.movies.

Genera eventos reales de insercion, actualizacion y eliminacion para que el
Change Stream del consumidor en scripts/ingesta_streaming.py los capture.

Ejemplos:
    python scripts/productor_cdc.py --op insert
    python scripts/productor_cdc.py --op update --movie-id "<ObjectId>"
    python scripts/productor_cdc.py --op delete --movie-id "<ObjectId>"
    python scripts/productor_cdc.py --op all --iterations 3 --delay 3
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from bson import ObjectId

sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb


def pelicula_base(titulo: str, anio: int = 2026, genero: str = "Drama") -> dict:
    """Plantilla compatible con documentos reales de sample_mflix.movies."""
    ahora = datetime.now(timezone.utc)
    return {
        "_id": ObjectId(),
        "title": titulo,
        "year": anio,
        "genres": [genero, "Adventure"],
        "runtime": 112,
        "cast": ["Actor A", "Actor B", "Actor C"],
        "countries": ["USA"],
        "directors": ["Director CDC"],
        "writers": ["Writer CDC"],
        "fullplot": "Película de prueba generada por el productor CDC del proyecto Mongo Data Lake.",
        "released": ahora,
        "lastUpdated": ahora,
        "imdb": {"rating": 7.7, "votes": 15420},
        "source": "cdc_producer",
    }


def normalizar_objectid(valor):
    """Convierte un _id string en ObjectId para la colección movies."""
    if valor is None:
        return None
    if isinstance(valor, ObjectId):
        return valor
    try:
        return ObjectId(str(valor))
    except Exception:
        return str(valor)


def obtener_pelicula_por_titulo(db, titulo: str):
    return db.movies.find_one({"title": titulo, "source": "cdc_producer"}, sort=[("_id", -1)])


def emitir_insercion(db, titulo: str = "CDC Demo Movie") -> ObjectId:
    """Inserta una pelicula nueva para generar un evento insert."""
    documento = pelicula_base(titulo)
    resultado = db.movies.insert_one(documento)
    print(f"[PRODUCER] INSERT -> _id={resultado.inserted_id} title={titulo}")
    return resultado.inserted_id


def emitir_actualizacion(db, movie_id: ObjectId | str | None = None, titulo: str | None = None) -> ObjectId:
    """Actualiza un documento para generar un evento update."""
    if movie_id is None:
        if titulo:
            doc = obtener_pelicula_por_titulo(db, titulo)
        else:
            doc = db.movies.find_one({"source": "cdc_producer"}, sort=[("_id", -1)])
        if not doc:
            raise RuntimeError("No existe una película de prueba para actualizar. Ejecuta primero --op insert.")
        movie_id = doc["_id"]
    else:
        movie_id = normalizar_objectid(movie_id)

    ahora = datetime.now(timezone.utc)
    resultado = db.movies.update_one(
        {"_id": movie_id},
        {
            "$set": {
                "lastUpdated": ahora,
                "year": 2027,
                "genres": ["Drama", "Mystery", "Thriller"],
                "imdb.rating": 8.9,
                "imdb.votes": 25800,
                "fullplot": "Película actualizada por el productor CDC del proyecto para probar cambios en MongoDB Atlas.",
            },
            "$push": {"cast": "Actor D"},
        },
    )
    print(
        f"[PRODUCER] UPDATE -> _id={movie_id} matched={resultado.matched_count} modified={resultado.modified_count}"
    )
    return movie_id


def emitir_eliminacion(db, movie_id: ObjectId | str | None = None, titulo: str | None = None) -> ObjectId:
    """Elimina un documento para generar un evento delete."""
    if movie_id is None:
        if titulo:
            doc = obtener_pelicula_por_titulo(db, titulo)
        else:
            doc = db.movies.find_one({"source": "cdc_producer"}, sort=[("_id", -1)])
        if not doc:
            raise RuntimeError("No existe una película de prueba para eliminar. Ejecuta primero --op insert.")
        movie_id = doc["_id"]
    else:
        movie_id = normalizar_objectid(movie_id)

    resultado = db.movies.delete_one({"_id": movie_id})
    print(f"[PRODUCER] DELETE -> _id={movie_id} deleted={resultado.deleted_count}")
    return movie_id


def ejecutar_secuencia(db, iterations: int = 1, delay_segundos: int = 3) -> None:
    """Emite una secuencia completa de insert, update y delete para cada iteracion."""
    for i in range(1, iterations + 1):
        titulo = f"CDC Demo Movie {i}"
        print(f"\n[PRODUCER] Iteracion {i}/{iterations}")
        movie_id = emitir_insercion(db, titulo=titulo)
        time.sleep(delay_segundos)
        emitir_actualizacion(db, movie_id=movie_id)
        time.sleep(delay_segundos)
        emitir_eliminacion(db, movie_id=movie_id)
        if i < iterations:
            time.sleep(delay_segundos)


def main() -> None:
    parser = argparse.ArgumentParser(description="Productor CDC para sample_mflix.movies")
    parser.add_argument("--op", choices=["insert", "update", "delete", "all"], default="all")
    parser.add_argument("--movie-id", default=None, help="ObjectId de la pelicula a actualizar/eliminar")
    parser.add_argument("--titulo", default="CDC Demo Movie", help="Titulo para los documentos generados")
    parser.add_argument("--iterations", type=int, default=1, help="Cantidad de secuencias a emitir")
    parser.add_argument("--delay", type=int, default=3, help="Segundos de pausa entre eventos")
    args = parser.parse_args()

    cliente, db = conectar_mongodb()
    try:
        if args.op == "insert":
            emitir_insercion(db, titulo=args.titulo)
        elif args.op == "update":
            emitir_actualizacion(db, movie_id=args.movie_id, titulo=args.titulo)
        elif args.op == "delete":
            emitir_eliminacion(db, movie_id=args.movie_id, titulo=args.titulo)
        else:
            ejecutar_secuencia(db, iterations=args.iterations, delay_segundos=args.delay)
    finally:
        cliente.close()
        print("[PRODUCER] Conexion cerrada.")


if __name__ == "__main__":
    main()
