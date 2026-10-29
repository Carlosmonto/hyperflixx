"""Benchmark de agregacion por genero: OLTP (movies) vs OLAP."""

import sys, time
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb

ANIO_INICIAL = 2000
ANIO_FINAL = 2020


def medir(nombre, consulta):
    inicio = time.perf_counter()
    filas = list(consulta())
    milisegundos = (time.perf_counter() - inicio) * 1000
    print(f"  {nombre}: {milisegundos:.2f} ms | {len(filas)} generos obtenidos.")
    return milisegundos


def ejecutar_benchmark():
    cliente, db = conectar_mongodb()
    try:
        print("\n[BENCHMARK] Iniciando comparacion OLTP vs OLAP...")
        print(f"[BENCHMARK] Consulta: peliculas por genero, rating IMDb y comentarios ({ANIO_INICIAL}-{ANIO_FINAL}).")
        print(f"[BENCHMARK] Movies disponibles: {db.movies.count_documents({})}")
        print(f"[BENCHMARK] Registros OLAP disponibles: {db.peliculas_analiticas.count_documents({})}")

        oltp = [
            {"$match": {"year": {"$gte": ANIO_INICIAL, "$lte": ANIO_FINAL}}},
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres", "peliculas": {"$sum": 1}, "rating_promedio": {"$avg": {"$ifNull": ["$imdb.rating", 0]}}, "comentarios": {"$sum": {"$ifNull": ["$num_mflix_comments", 0]}}}},
            {"$sort": {"rating_promedio": -1}},
        ]
        olap = [
            {"$match": {"anio": {"$gte": ANIO_INICIAL, "$lte": ANIO_FINAL}}},
            {"$unwind": "$generos"},
            {"$group": {"_id": "$generos", "peliculas": {"$sum": 1}, "rating_promedio": {"$avg": "$calificacion_imdb"}, "comentarios": {"$sum": "$total_comentarios"}}},
            {"$sort": {"rating_promedio": -1}},
        ]
        print("[BENCHMARK] Ejecutando consulta sobre OLTP (movies)...")
        tiempo_oltp = medir("OLTP", lambda: db.movies.aggregate(oltp, allowDiskUse=True))
        print("[BENCHMARK] Ejecutando consulta sobre OLAP (peliculas_analiticas)...")
        tiempo_olap = medir("OLAP", lambda: db.peliculas_analiticas.aggregate(olap, allowDiskUse=True))
        print("\n[BENCHMARK] Resultado final:")
        if tiempo_olap:
            print(f"  Relacion de tiempo OLTP/OLAP: {tiempo_oltp / tiempo_olap:.2f}x")
        else:
            print("  No se pudo calcular la relacion de tiempos.")
        print("[BENCHMARK] Ejecucion finalizada.")
    finally:
        cliente.close()
        print("[BENCHMARK] Conexion cerrada.")


if __name__ == "__main__":
    ejecutar_benchmark()
