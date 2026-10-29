"""Agrega peliculas y comentarios con el esquema de sample_mflix."""

import os, random, sys, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from faker import Faker
from pymongo import UpdateOne
sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb

fake = Faker("en_US")
NUM_PELICULAS = int(os.getenv("MFLIX_NUM_PELICULAS", "5000"))
COMENTARIOS_MIN = int(os.getenv("MFLIX_COMENTARIOS_MIN", "1"))
COMENTARIOS_MAX = int(os.getenv("MFLIX_COMENTARIOS_MAX", "5"))
BATCH_SIZE = int(os.getenv("MFLIX_BATCH_SIZE", "1000"))
GENEROS = ["Action", "Comedy", "Drama", "Documentary", "Horror", "Romance", "Sci-Fi"]


def lotes(datos):
    for inicio in range(0, len(datos), BATCH_SIZE):
        yield datos[inicio : inicio + BATCH_SIZE]


def generar_datos():
    if NUM_PELICULAS < 1 or COMENTARIOS_MIN < 0 or COMENTARIOS_MAX < COMENTARIOS_MIN:
        raise ValueError("Revise MFLIX_NUM_PELICULAS y los limites de comentarios.")
    cliente, db = conectar_mongodb()
    inicio = time.perf_counter()
    try:
        print("\n[CARGA] Iniciando generacion masiva para sample_mflix...")
        print(f"[CARGA] Peliculas: {NUM_PELICULAS}; comentarios por pelicula: {COMENTARIOS_MIN}-{COMENTARIOS_MAX}; lote: {BATCH_SIZE}.")
        antes_movies, antes_comments = db.movies.count_documents({}), db.comments.count_documents({})
        ahora, peliculas = datetime.now(timezone.utc), []

        print("[CARGA] Generando documentos de movies...")
        for _ in range(NUM_PELICULAS):
            anio, rating = random.randint(1950, ahora.year), round(random.uniform(1, 10), 1)
            peliculas.append({"title": fake.sentence(4).rstrip("."), "plot": fake.paragraph(), "fullplot": fake.paragraph(6), "genres": random.sample(GENEROS, random.randint(1, 3)), "runtime": random.randint(70, 180), "cast": [fake.name() for _ in range(3)], "directors": [fake.name()], "languages": ["English"], "countries": ["USA"], "released": datetime(anio, random.randint(1, 12), random.randint(1, 28), tzinfo=timezone.utc), "year": anio, "rated": random.choice(["G", "PG", "PG-13", "R"]), "type": "movie", "imdb": {"rating": rating, "votes": random.randint(10, 500000)}, "tomatoes": {"viewer": {"rating": round(rating / 2, 1), "numReviews": random.randint(0, 50000)}}, "awards": {"wins": random.randint(0, 20), "nominations": random.randint(0, 30)}, "num_mflix_comments": 0})
        for numero, lote in enumerate(lotes(peliculas), 1):
            db.movies.insert_many(lote)
            print(f"  Movies: lote {numero} insertado ({min(numero * BATCH_SIZE, NUM_PELICULAS)}/{NUM_PELICULAS}).")

        print("[CARGA] Generando comentarios relacionados con las nuevas peliculas...")
        comentarios, conteos = [], {}
        for pelicula in peliculas:
            total = random.randint(COMENTARIOS_MIN, COMENTARIOS_MAX)
            conteos[pelicula["_id"]] = total
            for _ in range(total):
                comentarios.append({"name": fake.name(), "email": fake.email(), "movie_id": pelicula["_id"], "text": fake.paragraph(), "date": ahora - timedelta(days=random.randint(0, 3650))})
        for numero, lote in enumerate(lotes(comentarios), 1):
            db.comments.insert_many(lote)
            print(f"  Comments: lote {numero} insertado ({min(numero * BATCH_SIZE, len(comentarios))}/{len(comentarios)}).")
        if conteos:
            db.movies.bulk_write([UpdateOne({"_id": movie_id}, {"$set": {"num_mflix_comments": total}}) for movie_id, total in conteos.items()])

        print("\n[CARGA] Generacion masiva completada.")
        print(f"  Peliculas agregadas: {db.movies.count_documents({}) - antes_movies}")
        print(f"  Comentarios agregados: {db.comments.count_documents({}) - antes_comments}")
        print(f"  Tiempo total: {time.perf_counter() - inicio:.2f} segundos")
        print("[CARGA] Ejecute transformar_oltp_a_olap.py para actualizar la capa analitica.")
    finally:
        cliente.close()
        print("[CARGA] Conexion cerrada.")


if __name__ == "__main__":
    generar_datos()
