"""Consultas analiticas (BI) sobre el modelo estrella de sample_mflix.

Patron usado en todas las consultas: primero se agrega la tabla de hechos
(fact_comentarios) y despues se hace $lookup a la dimension. Asi el $lookup
se ejecuta sobre pocas filas agrupadas y no sobre cada comentario.

Ejecucion:
    python scripts/consultas_data_warehouse.py
"""

import sys, time
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb


def unir(dimension, llave, campos):
    """$lookup + $unwind hacia una dimension trayendo solo los campos necesarios."""
    return [
        {"$lookup": {"from": dimension, "localField": "_id", "foreignField": llave,
                     "pipeline": [{"$project": {"_id": 0, **{c: 1 for c in campos}}}], "as": "dim"}},
        {"$unwind": "$dim"},
    ]


CONSULTAS = {
    "1. Comentarios por anio y trimestre (ultimos 8 periodos)": ("fact_comentarios", [
        {"$group": {"_id": "$fecha_id", "comentarios": {"$sum": "$cantidad"}}},
        *unir("dim_tiempo", "fecha_id", ["anio", "trimestre"]),
        {"$group": {"_id": {"anio": "$dim.anio", "trimestre": "$dim.trimestre"},
                    "comentarios": {"$sum": "$comentarios"}}},
        {"$sort": {"_id.anio": -1, "_id.trimestre": -1}},
        {"$limit": 8},
    ]),
    "2. Entre semana vs fin de semana": ("fact_comentarios", [
        {"$group": {"_id": "$fecha_id", "comentarios": {"$sum": "$cantidad"}}},
        *unir("dim_tiempo", "fecha_id", ["es_fin_de_semana"]),
        {"$group": {"_id": {"$cond": ["$dim.es_fin_de_semana", "Fin de semana", "Entre semana"]},
                    "comentarios": {"$sum": "$comentarios"}, "dias": {"$sum": 1}}},
        {"$set": {"promedio_por_dia": {"$round": [{"$divide": ["$comentarios", "$dias"]}, 2]}}},
        {"$sort": {"comentarios": -1}},
    ]),
    "3. Top 10 generos por comentarios": ("fact_comentarios", [
        {"$group": {"_id": "$pelicula_id", "comentarios": {"$sum": "$cantidad"}}},
        *unir("dim_pelicula", "pelicula_id", ["generos"]),
        {"$unwind": "$dim.generos"},
        {"$group": {"_id": "$dim.generos", "comentarios": {"$sum": "$comentarios"},
                    "peliculas": {"$sum": 1}}},
        {"$sort": {"comentarios": -1}},
        {"$limit": 10},
    ]),
    "4. Top 10 peliculas mas comentadas": ("fact_comentarios", [
        {"$group": {"_id": "$pelicula_id", "comentarios": {"$sum": "$cantidad"}}},
        {"$sort": {"comentarios": -1}},
        {"$limit": 10},
        *unir("dim_pelicula", "pelicula_id", ["titulo", "anio", "calificacion_imdb"]),
        {"$project": {"_id": 0, "titulo": "$dim.titulo", "anio": "$dim.anio",
                      "imdb": "$dim.calificacion_imdb", "comentarios": 1}},
    ]),
    "5. Comentarios promedio por rango de calificacion IMDb": ("fact_comentarios", [
        {"$group": {"_id": "$pelicula_id", "comentarios": {"$sum": "$cantidad"}}},
        *unir("dim_pelicula", "pelicula_id", ["rango_calificacion"]),
        {"$group": {"_id": "$dim.rango_calificacion", "peliculas": {"$sum": 1},
                    "promedio_comentarios": {"$avg": "$comentarios"}}},
        {"$set": {"promedio_comentarios": {"$round": ["$promedio_comentarios", 2]}}},
        {"$sort": {"promedio_comentarios": -1}},
    ]),
    "6. Usuarios por segmento de actividad": ("dim_usuario", [
        {"$group": {"_id": "$segmento", "usuarios": {"$sum": 1},
                    "comentarios": {"$sum": "$total_comentarios"},
                    "registrados": {"$sum": {"$cond": ["$es_usuario_registrado", 1, 0]}}}},
        {"$sort": {"comentarios": -1}},
    ]),
}


def ejecutar_consultas():
    cliente, db = conectar_mongodb()
    try:
        if db.fact_comentarios.estimated_document_count() == 0:
            raise RuntimeError("fact_comentarios esta vacia. Ejecute primero "
                               "scripts/construir_data_warehouse.py")
        print("\n[BI] Consultas analiticas sobre el Data Warehouse")
        for titulo, (coleccion, pipeline) in CONSULTAS.items():
            inicio = time.perf_counter()
            filas = list(db[coleccion].aggregate(pipeline, allowDiskUse=True))
            print(f"\n{titulo}  ({time.perf_counter() - inicio:.2f} s)")
            for fila in filas:
                print(f"  {fila}")
    finally:
        cliente.close()
        print("\n[BI] Conexion cerrada.")


if __name__ == "__main__":
    ejecutar_consultas()
