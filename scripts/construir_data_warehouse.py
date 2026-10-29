"""Fase 3: Data Warehouse (modelo estrella) sobre sample_mflix.

Adapta el esquema del documento data_warehouse (ventas) al dominio del proyecto:

    ventas_analiticas  ->  peliculas_analiticas + comments  (fuente)
    dim_tiempo         ->  dim_tiempo      (calendario de los comentarios)
    dim_cliente        ->  dim_usuario     (quien comenta)
    dim_producto       ->  dim_pelicula    (catalogo de peliculas)
    fact_ventas        ->  fact_comentarios (un documento por comentario)

Ademas crea dos tablas agregadas (cubos precalculados) para que las consultas
pesadas del benchmark respondan en < 500 ms:
    agg_comentarios_diarios  un documento por dia con el total de comentarios
    agg_genero_decada        un documento por combinacion genero + decada

Ejecucion:
    python scripts/construir_data_warehouse.py               # carga incremental ($merge)
    python scripts/construir_data_warehouse.py --reconstruir # borra y reconstruye todo

Requisito: ejecutar antes scripts/transformar_oltp_a_olap.py (crea peliculas_analiticas).
"""

import os, sys, time
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb

# Zona horaria para fechas. Debe ser la misma en dim_tiempo y fact_comentarios
# para que fecha_id coincida entre ambas colecciones.
ZONA = os.getenv("DW_ZONA_HORARIA", "UTC")
COLECCIONES_DW = ["dim_tiempo", "dim_usuario", "dim_pelicula", "fact_comentarios",
                  "agg_comentarios_diarios", "agg_genero_decada", "agg_genero_top",
                  "agg_pais_rating", "agg_genero_premios"]
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
# $dayOfWeek devuelve 1 = domingo ... 7 = sabado
DIAS = ["Domingo", "Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado"]


def fecha_id(campo):
    return {"$dateToString": {"format": "%Y-%m-%d", "date": campo, "timezone": ZONA}}


def merge(coleccion, llave):
    """Carga idempotente: reemplaza si existe, inserta si no (como un UPSERT)."""
    return {"$merge": {"into": coleccion, "on": llave, "whenMatched": "replace", "whenNotMatched": "insert"}}


def preparar_colecciones(db, reconstruir):
    if reconstruir:
        print("[DW] --reconstruir: eliminando colecciones del Data Warehouse...")
        for nombre in COLECCIONES_DW:
            db[nombre].drop()
    # $merge exige un indice unico sobre el campo 'on'.
    db.dim_tiempo.create_index("fecha_id", unique=True, name="fecha_id_unique")
    db.dim_tiempo.create_index([("anio", 1), ("trimestre", 1)], name="anio_trimestre")
    db.dim_usuario.create_index("usuario_id", unique=True, name="usuario_id_unique")
    db.dim_pelicula.create_index("pelicula_id", unique=True, name="pelicula_id_unique")
    db.dim_pelicula.create_index("generos", name="generos")
    db.fact_comentarios.create_index("comentario_id", unique=True, name="comentario_id_unique")
    db.fact_comentarios.create_index("fecha_id", name="fecha_id")
    db.fact_comentarios.create_index("usuario_id", name="usuario_id")
    db.fact_comentarios.create_index("pelicula_id", name="pelicula_id")
    # Tablas agregadas. Los indices "cubiertos" contienen todos los campos que usa
    # la consulta, asi MongoDB responde leyendo solo el indice (sin abrir documentos).
    db.agg_comentarios_diarios.create_index("fecha_id", unique=True, name="fecha_id_unique")
    db.agg_comentarios_diarios.create_index([("dia_semana", 1), ("nombre_dia", 1), ("comentarios", 1)],
                                            name="idx_dia_semana_cubierto")
    db.agg_comentarios_diarios.create_index([("fecha_id", 1), ("anio", 1), ("trimestre", 1), ("comentarios", 1)],
                                            name="idx_fecha_trimestre_cubierto")
    db.agg_genero_decada.create_index("agg_id", unique=True, name="agg_id_unique")
    db.agg_genero_decada.create_index([("genero", 1), ("decada", 1)], name="idx_genero_decada")
    # Este índice único también cubre las consultas por género. No crear un
    # segundo índice simple: versiones anteriores dejaron idx_genero_top con
    # otra definición y MongoDB rechaza reutilizar el nombre con claves distintas.
    db.agg_genero_top.create_index("genero", unique=True, name="genero_unique")
    db.agg_pais_rating.create_index("pais", unique=True, name="pais_unique")
    db.agg_pais_rating.create_index([("rating_promedio", -1)], name="idx_pais_rating")
    db.agg_genero_premios.create_index("genero", unique=True, name="genero_unique")
    db.agg_genero_premios.create_index([("tasa_premiadas_pct", -1)], name="idx_genero_premios")


def construir_dim_tiempo(db):
    """Una fila por dia calendario en el que hubo comentarios."""
    fecha = "$_id"
    pipeline = [
        {"$match": {"date": {"$type": "date"}}},
        {"$group": {"_id": {"$dateTrunc": {"date": "$date", "unit": "day", "timezone": ZONA}}}},
        {"$project": {
            "_id": 0,
            "fecha_id": fecha_id(fecha),
            "fecha": fecha,
            "anio": {"$year": {"date": fecha, "timezone": ZONA}},
            "mes": {"$month": {"date": fecha, "timezone": ZONA}},
            "dia": {"$dayOfMonth": {"date": fecha, "timezone": ZONA}},
            # Correccion del documento: el fin de semana se calcula con $dayOfWeek,
            # no con el dia del mes.
            "dia_semana": {"$dayOfWeek": {"date": fecha, "timezone": ZONA}},
        }},
        {"$set": {
            "trimestre": {"$toInt": {"$ceil": {"$divide": ["$mes", 3]}}},
            "semestre": {"$cond": [{"$lte": ["$mes", 6]}, 1, 2]},
            "nombre_mes": {"$arrayElemAt": [MESES, {"$subtract": ["$mes", 1]}]},
            "nombre_dia": {"$arrayElemAt": [DIAS, {"$subtract": ["$dia_semana", 1]}]},
            "es_fin_de_semana": {"$in": ["$dia_semana", [1, 7]]},
            "fecha_actualizacion": "$$NOW",
        }},
        merge("dim_tiempo", "fecha_id"),
    ]
    db.comments.aggregate(pipeline, allowDiskUse=True)


def construir_dim_usuario(db):
    """Perfil unico de cada persona que comenta (llave natural: email)."""
    pipeline = [
        {"$match": {"email": {"$type": "string"}}},
        # Ordenar antes de agrupar hace que $first sea determinista:
        # se conserva el nombre del comentario mas reciente.
        {"$sort": {"date": -1}},
        {"$group": {
            "_id": "$email",
            "nombre": {"$first": "$name"},
            "total_comentarios": {"$sum": 1},
            "peliculas": {"$addToSet": "$movie_id"},
            "primer_comentario": {"$min": "$date"},
            "ultimo_comentario": {"$max": "$date"},
            "longitud_promedio": {"$avg": {"$strLenCP": {"$ifNull": ["$text", ""]}}},
        }},
        # Usa el indice unico users.email creado en crear_estructura.py
        {"$lookup": {"from": "users", "localField": "_id", "foreignField": "email",
                     "pipeline": [{"$project": {"_id": 1}}], "as": "registro"}},
        {"$project": {
            "_id": 0,
            "usuario_id": "$_id",
            "email": "$_id",
            "nombre": 1,
            "total_comentarios": 1,
            "peliculas_comentadas": {"$size": "$peliculas"},
            "primer_comentario": 1,
            "ultimo_comentario": 1,
            "longitud_promedio": {"$round": ["$longitud_promedio", 2]},
            "es_usuario_registrado": {"$gt": [{"$size": "$registro"}, 0]},
            "segmento": {"$switch": {"branches": [
                {"case": {"$gte": ["$total_comentarios", 100]}, "then": "Muy activo"},
                {"case": {"$gte": ["$total_comentarios", 10]}, "then": "Activo"},
            ], "default": "Ocasional"}},
            "fecha_actualizacion": "$$NOW",
        }},
        merge("dim_usuario", "usuario_id"),
    ]
    db.comments.aggregate(pipeline, allowDiskUse=True)


def construir_dim_pelicula(db):
    """Catalogo de peliculas a partir de la capa OLAP ya enriquecida."""
    def numero(campo, tipo, defecto):
        return {"$convert": {"input": campo, "to": tipo, "onError": defecto, "onNull": defecto}}

    pipeline = [
        {"$project": {
            "_id": 0,
            "pelicula_id": "$movie_id",
            "titulo": 1,
            # sample_mflix tiene anios con texto sucio (p. ej. "2014e"); quedan en null.
            "anio": numero("$anio", "int", None),
            "tipo": 1,
            "clasificacion": {"$ifNull": ["$clasificacion", "Sin clasificar"]},
            "generos": 1,
            "genero_principal": {"$ifNull": [{"$first": "$generos"}, "Sin genero"]},
            "pais_principal": {"$ifNull": [{"$first": "$paises"}, "Sin pais"]},
            "idioma_principal": {"$ifNull": [{"$first": "$idiomas"}, "Sin idioma"]},
            "duracion_minutos": numero("$duracion_minutos", "int", None),
            "calificacion_imdb": {"$round": [numero("$calificacion_imdb", "double", 0), 2]},
            "votos_imdb": numero("$votos_imdb", "int", 0),
            "calificacion_audiencia": numero("$calificacion_audiencia", "double", 0),
            "premios_ganados": numero("$premios_ganados", "int", 0),
            "nominaciones": numero("$nominaciones", "int", 0),
        }},
        {"$set": {
            "decada": {"$cond": [{"$eq": ["$anio", None]}, None,
                                 {"$toInt": {"$multiply": [{"$floor": {"$divide": ["$anio", 10]}}, 10]}}]},
            "rango_calificacion": {"$switch": {"branches": [
                {"case": {"$eq": ["$calificacion_imdb", 0]}, "then": "Sin calificacion"},
                {"case": {"$gte": ["$calificacion_imdb", 8]}, "then": "Excelente (8-10)"},
                {"case": {"$gte": ["$calificacion_imdb", 6]}, "then": "Buena (6-8)"},
                {"case": {"$gte": ["$calificacion_imdb", 4]}, "then": "Regular (4-6)"},
            ], "default": "Baja (<4)"}},
            "fecha_actualizacion": "$$NOW",
        }},
        merge("dim_pelicula", "pelicula_id"),
    ]
    db.peliculas_analiticas.aggregate(pipeline, allowDiskUse=True)


def construir_fact_comentarios(db):
    """Tabla de hechos: solo llaves hacia las dimensiones + medidas numericas."""
    pipeline = [
        {"$match": {"date": {"$type": "date"}, "movie_id": {"$exists": True}}},
        {"$project": {
            "_id": 0,
            "comentario_id": "$_id",
            "fecha_id": fecha_id("$date"),
            "usuario_id": "$email",
            "pelicula_id": "$movie_id",
            "fecha_hora": "$date",
            # $literal evita que el 1 se interprete como "incluir campo".
            "cantidad": {"$literal": 1},
            "longitud_texto": {"$strLenCP": {"$ifNull": ["$text", ""]}},
            "fecha_actualizacion": "$$NOW",
        }},
        merge("fact_comentarios", "comentario_id"),
    ]
    db.comments.aggregate(pipeline, allowDiskUse=True)


def construir_agg_comentarios_diarios(db):
    """Agregado diario: el $lookup a dim_tiempo se hace una sola vez aqui, en el ETL,
    y no en cada consulta analitica."""
    pipeline = [
        {"$group": {"_id": "$fecha_id", "comentarios": {"$sum": "$cantidad"},
                    "longitud_total": {"$sum": "$longitud_texto"}}},
        {"$lookup": {"from": "dim_tiempo", "localField": "_id", "foreignField": "fecha_id",
                     "pipeline": [{"$project": {"_id": 0, "anio": 1, "mes": 1, "trimestre": 1, "dia_semana": 1,
                                                "nombre_dia": 1, "es_fin_de_semana": 1}}], "as": "t"}},
        {"$unwind": "$t"},
        {"$project": {
            "_id": 0,
            "fecha_id": "$_id",
            "comentarios": 1,
            "longitud_total": 1,
            "anio": "$t.anio",
            "mes": "$t.mes",
            "trimestre": "$t.trimestre",
            "dia_semana": "$t.dia_semana",
            "nombre_dia": "$t.nombre_dia",
            "es_fin_de_semana": "$t.es_fin_de_semana",
            "fecha_actualizacion": "$$NOW",
        }},
        merge("agg_comentarios_diarios", "fecha_id"),
    ]
    db.fact_comentarios.aggregate(pipeline, allowDiskUse=True)


def construir_agg_genero_decada(db):
    """Cubo genero x decada precalculado desde dim_pelicula."""
    pipeline = [
        {"$match": {"decada": {"$type": "number"}}},
        {"$unwind": "$generos"},
        {"$group": {
            "_id": {"genero": "$generos", "decada": "$decada"},
            "peliculas": {"$sum": 1},
            # Las peliculas sin calificacion (0) no cuentan en el promedio.
            "rating_promedio": {"$avg": {"$cond": [{"$gt": ["$calificacion_imdb", 0]}, "$calificacion_imdb", None]}},
        }},
        {"$project": {
            "_id": 0,
            "agg_id": {"$concat": ["$_id.genero", "|", {"$toString": "$_id.decada"}]},
            "genero": "$_id.genero",
            "decada": "$_id.decada",
            "peliculas": 1,
            "rating_promedio": {"$round": ["$rating_promedio", 2]},
            "fecha_actualizacion": "$$NOW",
        }},
        merge("agg_genero_decada", "agg_id"),
    ]
    db.dim_pelicula.aggregate(pipeline, allowDiskUse=True)


def construir_agg_genero_top(db):
    pipeline = [
        {"$match": {"tipo": "movie", "votos_imdb": {"$gte": 10000},
                    "calificacion_imdb": {"$type": "number"}}},
        {"$unwind": "$generos"},
        {"$sort": {"generos": 1, "calificacion_imdb": -1, "titulo": 1}},
        {"$group": {"_id": "$generos", "top": {"$push": {
            "titulo": "$titulo", "imdb": "$calificacion_imdb"}}}},
        {"$project": {"_id": 0, "genero": "$_id",
                      "top_peliculas": {"$slice": ["$top", 5]},
                      "fecha_actualizacion": "$$NOW"}},
        merge("agg_genero_top", "genero"),
    ]
    db.peliculas_analiticas.aggregate(pipeline, allowDiskUse=True)


def construir_agg_pais_rating(db):
    pipeline = [
        {"$match": {"tipo": "movie"}},
        {"$unwind": "$paises"},
        {"$group": {"_id": "$paises", "peliculas": {"$sum": 1},
                    "rating_promedio": {"$avg": "$calificacion_imdb"}}},
        {"$match": {"peliculas": {"$gte": 50}}},
        {"$project": {"_id": 0, "pais": "$_id", "peliculas": 1,
                      "rating_promedio": {"$round": ["$rating_promedio", 2]},
                      "fecha_actualizacion": "$$NOW"}},
        merge("agg_pais_rating", "pais"),
    ]
    db.peliculas_analiticas.aggregate(pipeline, allowDiskUse=True)


def construir_agg_genero_premios(db):
    pipeline = [
        {"$match": {"tipo": "movie"}},
        {"$unwind": "$generos"},
        {"$group": {"_id": "$generos", "total": {"$sum": 1},
                    "premiadas": {"$sum": {"$cond": [
                        {"$gt": ["$premios_ganados", 0]}, 1, 0]}}}},
        {"$project": {"_id": 0, "genero": "$_id", "total": 1, "premiadas": 1,
                      "tasa_premiadas_pct": {"$round": [{"$multiply": [
                          {"$divide": ["$premiadas", "$total"]}, 100]}, 2]},
                      "fecha_actualizacion": "$$NOW"}},
        merge("agg_genero_premios", "genero"),
    ]
    db.peliculas_analiticas.aggregate(pipeline, allowDiskUse=True)


def huerfanos(db, llave, dimension, llave_dim):
    """Hechos cuya llave no existe en la dimension (integridad referencial)."""
    resultado = list(db.fact_comentarios.aggregate([
        {"$lookup": {"from": dimension, "localField": llave, "foreignField": llave_dim,
                     "pipeline": [{"$project": {"_id": 1}}], "as": "d"}},
        {"$match": {"d": {"$size": 0}}},
        {"$count": "total"},
    ], allowDiskUse=True))
    return resultado[0]["total"] if resultado else 0


def construir_data_warehouse():
    reconstruir = "--reconstruir" in sys.argv
    cliente, db = conectar_mongodb()
    inicio = time.perf_counter()
    try:
        print("\n[DW] Iniciando construccion del Data Warehouse (modelo estrella)...")
        print(f"[DW] Zona horaria de fechas: {ZONA}")
        if db.peliculas_analiticas.estimated_document_count() == 0:
            raise RuntimeError("peliculas_analiticas esta vacia. Ejecute primero "
                               "scripts/transformar_oltp_a_olap.py")
        preparar_colecciones(db, reconstruir)

        pasos = [("dim_tiempo", construir_dim_tiempo),
                 ("dim_usuario", construir_dim_usuario),
                 ("dim_pelicula", construir_dim_pelicula),
                 ("fact_comentarios", construir_fact_comentarios),
                 ("agg_comentarios_diarios", construir_agg_comentarios_diarios),
                 ("agg_genero_decada", construir_agg_genero_decada),
                 ("agg_genero_top", construir_agg_genero_top),
                 ("agg_pais_rating", construir_agg_pais_rating),
                 ("agg_genero_premios", construir_agg_genero_premios)]
        for nombre, funcion in pasos:
            t = time.perf_counter()
            print(f"[DW] Construyendo {nombre}...")
            funcion(db)
            print(f"  OK: {db[nombre].count_documents({})} documentos ({time.perf_counter() - t:.2f} s)")

        print("\n[DW] Validando integridad referencial de fact_comentarios...")
        for llave, dim in [("fecha_id", "dim_tiempo"), ("usuario_id", "dim_usuario"),
                           ("pelicula_id", "dim_pelicula")]:
            total = huerfanos(db, llave, dim, llave)
            estado = "OK" if total == 0 else "AVISO"
            print(f"  {estado}: {total} hechos sin coincidencia en {dim}")

        print("\n==============================")
        print(" DATA WAREHOUSE CONFIGURADO")
        print("==============================")
        for nombre in COLECCIONES_DW:
            print(f"  - {nombre}: {db[nombre].count_documents({})} documentos")
        print(f"  Tiempo total: {time.perf_counter() - inicio:.2f} segundos")
        print("[DW] Ejecute scripts/consultas_data_warehouse.py para las consultas BI.")
    finally:
        cliente.close()
        print("[DW] Conexion cerrada.")


if __name__ == "__main__":
    construir_data_warehouse()
