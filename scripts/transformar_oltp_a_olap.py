"""ETL desde movies/comments hacia peliculas_analiticas."""

import sys, time
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb


def transformar_datos():
    cliente, db = conectar_mongodb()
    inicio = time.perf_counter()
    try:
        origen = db.movies.count_documents({})
        print("\n[ETL] Iniciando transformacion OLTP a OLAP...")
        print(f"[ETL] Origen: {origen} documentos de movies y sus comments relacionados.")
        print("[ETL] Calculando dimensiones de pelicula, tiempo, pais, generos y metricas IMDb/Tomatoes...")
        pipeline = [
            {"$lookup": {"from": "comments", "localField": "_id", "foreignField": "movie_id", "pipeline": [{"$group": {"_id": None, "total": {"$sum": 1}, "ultimo": {"$max": "$date"}}}], "as": "comentarios"}},
            {"$set": {"comentario": {"$first": "$comentarios"}}},
            {"$project": {"_id": 0, "movie_id": "$_id", "titulo": "$title", "anio": "$year", "fecha_estreno": "$released", "tipo": "$type", "clasificacion": "$rated", "generos": {"$ifNull": ["$genres", []]}, "paises": {"$ifNull": ["$countries", []]}, "idiomas": {"$ifNull": ["$languages", []]}, "duracion_minutos": "$runtime", "calificacion_imdb": {"$ifNull": ["$imdb.rating", 0]}, "votos_imdb": {"$ifNull": ["$imdb.votes", 0]}, "calificacion_audiencia": {"$ifNull": ["$tomatoes.viewer.rating", 0]}, "resenas_audiencia": {"$ifNull": ["$tomatoes.viewer.numReviews", 0]}, "premios_ganados": {"$ifNull": ["$awards.wins", 0]}, "nominaciones": {"$ifNull": ["$awards.nominations", 0]}, "total_comentarios": {"$ifNull": ["$comentario.total", 0]}, "ultimo_comentario": "$comentario.ultimo", "fecha_actualizacion": "$$NOW"}},
            {"$merge": {"into": "peliculas_analiticas", "on": "movie_id", "whenMatched": "replace", "whenNotMatched": "insert"}},
        ]
        list(db.movies.aggregate(pipeline, allowDiskUse=True))
        total = db.peliculas_analiticas.count_documents({})
        print("\n[ETL] Transformacion completada correctamente.")
        print(f"  Peliculas procesadas: {origen}")
        print(f"  Registros en peliculas_analiticas: {total}")
        print(f"  Tiempo total: {time.perf_counter() - inicio:.2f} segundos")
        print("[ETL] La coleccion OLAP esta lista para consultas analiticas.")
    finally:
        cliente.close()
        print("[ETL] Conexion cerrada.")


if __name__ == "__main__":
    transformar_datos()
