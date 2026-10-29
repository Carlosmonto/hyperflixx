"""Prepara los indices OLTP y la coleccion OLAP de sample_mflix."""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb


def crear_estructura_mflix():
    cliente, db = conectar_mongodb()
    try:
        print("\n[ESTRUCTURA] Iniciando configuracion de sample_mflix...")
        existentes = set(db.list_collection_names())
        requeridas = {"movies", "comments", "users", "theaters"}
        print(f"[ESTRUCTURA] Colecciones encontradas: {', '.join(sorted(existentes))}")
        faltantes = requeridas - existentes
        if faltantes:
            print(f"[ESTRUCTURA] Aviso: faltan colecciones del dump: {', '.join(sorted(faltantes))}")

        print("\n[ESTRUCTURA] Creando/verificando indices de movies...")
        db.movies.create_index([("year", 1), ("imdb.rating", -1)], name="year_rating")
        db.movies.create_index([("genres", 1)], name="genres")
        print("  OK: year + imdb.rating y genres.")

        print("[ESTRUCTURA] Creando/verificando indices de comments...")
        db.comments.create_index([("movie_id", 1), ("date", -1)], name="movie_date")
        db.comments.create_index([("email", 1)], name="comment_email")
        print("  OK: movie_id + date y email.")

        print("[ESTRUCTURA] Creando/verificando indices de users y theaters...")
        db.users.create_index([("email", 1)], unique=True, name="email_1")
        db.theaters.create_index([("location.geo", "2dsphere")], name="geo index")
        print("  OK: email unico y geolocalizacion 2dsphere.")

        print("[ESTRUCTURA] Creando/verificando la coleccion OLAP...")
        if "peliculas_analiticas" not in existentes:
            db.create_collection("peliculas_analiticas")
            print("  Coleccion 'peliculas_analiticas' creada.")
        else:
            print("  Coleccion 'peliculas_analiticas' ya existe; se conservara.")
        db.peliculas_analiticas.create_index([("movie_id", 1)], unique=True, name="movie_id_unique")
        db.peliculas_analiticas.create_index([("anio", 1), ("calificacion_imdb", -1)], name="year_rating")
        db.peliculas_analiticas.create_index([("generos", 1)], name="genres")

        print("\n[ESTRUCTURA] Proceso finalizado.")
        for nombre in ["movies", "comments", "users", "theaters", "peliculas_analiticas"]:
            print(f"  - {nombre}: {db[nombre].count_documents({})} documentos")
        print("[ESTRUCTURA] La base esta lista para carga, ETL y benchmarking.")
    finally:
        cliente.close()
        print("[ESTRUCTURA] Conexion cerrada.")


if __name__ == "__main__":
    crear_estructura_mflix()
