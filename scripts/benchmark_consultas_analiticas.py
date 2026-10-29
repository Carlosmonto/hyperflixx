"""Benchmarking de 10 consultas analiticas con indexacion compuesta selectiva.

Adapta el documento "dashboard" (ventas_analiticas) al proyecto sample_mflix:
    - 7 consultas sobre el modelo desnormalizado peliculas_analiticas
    - 3 consultas sobre el modelo estrella y sus tablas agregadas
      (agg_comentarios_diarios, agg_genero_decada, dim_usuario)

Diferencias frente al documento:
    - El plan (IXSCAN / COLLSCAN) se verifica con explain(), no se supone.
    - Cada consulta se ejecuta 1 vez de calentamiento + N veces; se reporta la mediana.
    - Se mide la latencia de red (ping) para separar red de procesamiento.
    - Los resultados se guardan en reportes/ para que el dashboard los use.
    - Todas las filas llegan en un solo viaje de red (batchSize), sin getMore.

Ejecucion:
    python scripts/benchmark_consultas_analiticas.py
Variables opcionales (.env): BENCH_SLA_MS=499, BENCH_REPETICIONES=3
"""

import json, os, statistics, sys, time
from datetime import datetime
from pathlib import Path
from pymongo.errors import OperationFailure
RAIZ = Path(__file__).resolve().parents[1]
sys.path.append(str(RAIZ))
from conexion import conectar_mongodb

SLA_MS = float(os.getenv("BENCH_SLA_MS", "499"))
REPETICIONES = max(1, int(os.getenv("BENCH_REPETICIONES", "3")))
CARPETA_REPORTES = RAIZ / "reportes"
ARCHIVO_ULTIMO = CARPETA_REPORTES / "benchmark_ultimo.json"

# Indexacion compuesta selectiva: cada indice existe para una consulta concreta.
INDICES = {
    "peliculas_analiticas": [
        ([("anio", 1), ("calificacion_imdb", -1)], "year_rating"),          # C1 (ya lo crea crear_estructura.py)
        ([("clasificacion", 1), ("anio", 1)], "idx_clasificacion_anio"),     # C9
        ([("tipo", 1), ("votos_imdb", -1)], "idx_tipo_votos"),               # C2
        ([("total_comentarios", -1)], "idx_total_comentarios"),              # C10
    ],
    "dim_usuario": [([("total_comentarios", -1)], "idx_total_comentarios")],  # C7
    # Los siguientes ya los crea construir_data_warehouse.py; aqui solo se verifican.
    "agg_genero_decada": [([("genero", 1), ("decada", 1)], "idx_genero_decada")],                     # C3
    "agg_genero_top": [([("genero", 1)], "genero_unique")],                                              # C2
    "agg_pais_rating": [([("rating_promedio", -1)], "idx_pais_rating")],                                 # C4
    "agg_genero_premios": [([("tasa_premiadas_pct", -1)], "idx_genero_premios")],                       # C6
    "agg_comentarios_diarios": [
        ([("dia_semana", 1), ("nombre_dia", 1), ("comentarios", 1)], "idx_dia_semana_cubierto"),     # C5
        ([("fecha_id", 1), ("anio", 1), ("trimestre", 1), ("comentarios", 1)], "idx_fecha_trimestre_cubierto"),  # C8
    ],
}
# Tamano de lote: por defecto MongoDB envia 101 documentos en el primer lote y pide
# el resto con getMore (otro viaje de red). Con 1000 cualquier consulta llega completa.
TAMANO_LOTE = 1000


def num(campo, tipo="int"):
    """Convierte valores sucios de sample_mflix ("" o texto) a numero."""
    return {"$convert": {"input": campo, "to": tipo, "onError": 0, "onNull": 0}}


CONSULTAS = [
    ("Serie temporal: estrenos y rating IMDb por anio (2000-2015)", "peliculas_analiticas", [
        {"$match": {"anio": {"$gte": 2000, "$lte": 2015}}},
        {"$group": {"_id": "$anio", "peliculas": {"$sum": 1}, "rating_promedio": {"$avg": "$calificacion_imdb"}}},
        {"$set": {"rating_promedio": {"$round": ["$rating_promedio", 2]}}},
        {"$sort": {"_id": 1}},
    ]),
    ("Top 5 peliculas mejor calificadas por genero (>= 10k votos)", "agg_genero_top", [
        {"$sort": {"genero": 1}},
    ]),
    ("Matriz genero x decada (cubo precalculado, salida grande)", "agg_genero_decada", [
        {"$sort": {"genero": 1, "decada": 1}},
        {"$project": {"_id": 0, "genero": 1, "decada": 1, "peliculas": 1, "rating_promedio": 1}},
    ]),
    ("Ranking de paises por rating IMDb (min. 50 peliculas)", "agg_pais_rating", [
        {"$sort": {"rating_promedio": -1}},
        {"$limit": 10},
    ]),
    ("Comentarios por dia de la semana (agregado diario)", "agg_comentarios_diarios", [
        {"$match": {"dia_semana": {"$gte": 1}}},  # activa el indice cubierto
        {"$group": {"_id": {"dia": "$dia_semana", "nombre": "$nombre_dia"}, "comentarios": {"$sum": "$comentarios"}}},
        {"$sort": {"_id.dia": 1}},
    ]),
    ("Tasa de peliculas premiadas por genero", "agg_genero_premios", [
        {"$sort": {"tasa_premiadas_pct": -1}},
    ]),
    ("Top 10 usuarios mas activos (VIP)", "dim_usuario", [
        {"$sort": {"total_comentarios": -1}},
        {"$limit": 10},
        {"$project": {"_id": 0, "nombre": 1, "email": 1, "total_comentarios": 1, "peliculas_comentadas": 1, "segmento": 1}},
    ]),
    ("Comentarios trimestrales desde 2015 (agregado diario)", "agg_comentarios_diarios", [
        {"$match": {"fecha_id": {"$gte": "2015-01-01"}}},
        {"$group": {"_id": {"anio": "$anio", "trimestre": "$trimestre"}, "comentarios": {"$sum": "$comentarios"}}},
        {"$project": {"_id": 0, "anio": "$_id.anio", "trimestre": {"$concat": ["Q", {"$toString": "$_id.trimestre"}]},
                      "comentarios": 1}},
        {"$sort": {"anio": 1, "trimestre": 1}},
    ]),
    ("IMDb vs audiencia: R y PG-13 desde 1990 (Top 15)", "peliculas_analiticas", [
        {"$match": {"clasificacion": {"$in": ["R", "PG-13"]}, "anio": {"$gte": 1990}}},
        {"$group": {"_id": {"clasificacion": "$clasificacion", "anio": "$anio"}, "peliculas": {"$sum": 1},
                    "imdb_0a10": {"$avg": "$calificacion_imdb"}, "audiencia_0a5": {"$avg": "$calificacion_audiencia"}}},
        {"$set": {"imdb_0a10": {"$round": ["$imdb_0a10", 2]}, "audiencia_0a5": {"$round": ["$audiencia_0a5", 2]}}},
        {"$sort": {"peliculas": -1}},
        {"$limit": 15},
    ]),
    ("Top 10 peliculas con mayor engagement (comentarios)", "peliculas_analiticas", [
        {"$sort": {"total_comentarios": -1}},
        {"$limit": 10},
        {"$project": {"_id": 0, "titulo": 1, "anio": 1, "calificacion_imdb": 1, "total_comentarios": 1}},
    ]),
]


def crear_indices(db, existentes):
    for coleccion, indices in INDICES.items():
        if coleccion not in existentes:
            print(f"  AVISO: {coleccion} no existe; se omiten sus indices.")
            continue
        for llaves, nombre in indices:
            try:
                db[coleccion].create_index(llaves, name=nombre)
            except OperationFailure as error:
                # Codigo 85/86: ya existe el mismo indice con otro nombre u opciones.
                if error.code in (85, 86):
                    print(f"  INFO: {coleccion}.{nombre} ya existe con otro nombre; se reutiliza.")
                else:
                    raise
    print("  OK: indices compuestos verificados/creados.")


def plan_de_ejecucion(db, coleccion, pipeline):
    """Lee el plan ganador con explain() y lo clasifica como IXSCAN, COLLSCAN o MIXTO."""
    try:
        resultado = db.command({"explain": {"aggregate": coleccion, "pipeline": pipeline, "cursor": {},
                                            "allowDiskUse": True}, "verbosity": "queryPlanner"})
    except OperationFailure:
        return "DESCONOCIDO", []
    etapas, indices = set(), set()

    def recorrer(nodo):
        if isinstance(nodo, dict):
            for llave, valor in nodo.items():
                if llave == "rejectedPlans":
                    continue
                if llave == "stage" and isinstance(valor, str):
                    etapas.add(valor)
                elif llave == "indexName" and isinstance(valor, str):
                    indices.add(valor)
                recorrer(valor)
        elif isinstance(nodo, list):
            for elemento in nodo:
                recorrer(elemento)

    recorrer(resultado)
    usa_indice = any(("IXSCAN" in e) or e in ("DISTINCT_SCAN", "COUNT_SCAN") for e in etapas)
    usa_collscan = "COLLSCAN" in etapas
    if usa_indice and usa_collscan:
        return "MIXTO", sorted(indices)
    if usa_indice:
        return "IXSCAN", sorted(indices)
    if usa_collscan:
        return "COLLSCAN", []
    return "DESCONOCIDO", sorted(indices)


def medir_red(cliente, veces=5):
    tiempos = []
    for _ in range(veces):
        inicio = time.perf_counter()
        cliente.admin.command("ping")
        tiempos.append((time.perf_counter() - inicio) * 1000)
    return statistics.median(tiempos)


def medir_consulta(db, coleccion, pipeline):
    opciones = {"allowDiskUse": True, "batchSize": TAMANO_LOTE}
    list(db[coleccion].aggregate(pipeline, **opciones))  # calentamiento (cache de Atlas)
    tiempos, filas = [], []
    for _ in range(REPETICIONES):
        inicio = time.perf_counter()
        filas = list(db[coleccion].aggregate(pipeline, **opciones))
        tiempos.append((time.perf_counter() - inicio) * 1000)
    return tiempos, filas


def ejecutar_benchmark(guardar=True):
    cliente, db = conectar_mongodb()
    try:
        print("\n[BENCHMARK] Iniciando benchmarking de 10 consultas analiticas...")
        print(f"[BENCHMARK] SLA: <= {SLA_MS:.0f} ms | repeticiones por consulta: {REPETICIONES} (+1 calentamiento)")
        existentes = set(db.list_collection_names())

        print("\n[BENCHMARK] Fase 1: indexacion compuesta selectiva...")
        crear_indices(db, existentes)

        red_ms = medir_red(cliente)
        print(f"\n[BENCHMARK] Latencia de red (ping, mediana): {red_ms:.2f} ms")
        print("[BENCHMARK] Fase 2: ejecutando consultas y midiendo latencia...")
        resultados = []
        for numero, (titulo, coleccion, pipeline) in enumerate(CONSULTAS, 1):
            print("-" * 80)
            print(f"[{numero}/{len(CONSULTAS)}] {titulo}")
            if coleccion not in existentes or db[coleccion].estimated_document_count() == 0:
                print(f"  OMITIDA: {coleccion} esta vacia. Ejecute construir_data_warehouse.py")
                resultados.append({"numero": numero, "titulo": titulo, "coleccion": coleccion, "estado": "OMITIDA"})
                continue
            tiempos, filas = medir_consulta(db, coleccion, pipeline)
            plan, indices = plan_de_ejecucion(db, coleccion, pipeline)
            mediana = statistics.median(tiempos)
            muestra = str(filas[0])[:150] if filas else "(sin filas)"
            dentro = mediana <= SLA_MS
            print(f"  Latencia mediana: {mediana:.2f} ms (min {min(tiempos):.2f} / max {max(tiempos):.2f}) | "
                  f"Filas: {len(filas)} | Plan: {plan}{' ' + ', '.join(indices) if indices else ''} | "
                  f"{'OK' if dentro else 'FUERA DE SLA'}")
            print(f"  Muestra: {muestra}...")
            resultados.append({"numero": numero, "titulo": titulo, "coleccion": coleccion, "estado": "OK",
                               "latencia_ms": round(mediana, 2), "latencia_min_ms": round(min(tiempos), 2),
                               "latencia_max_ms": round(max(tiempos), 2), "filas": len(filas),
                               "plan": plan, "indices": indices, "dentro_sla": dentro})

        ejecutadas = [r for r in resultados if r["estado"] == "OK"]
        latencias = [r["latencia_ms"] for r in ejecutadas]
        resumen = {
            "fecha": datetime.now().isoformat(timespec="seconds"),
            "sla_ms": SLA_MS, "repeticiones": REPETICIONES, "latencia_red_ms": round(red_ms, 2),
            "ejecutadas": len(ejecutadas), "omitidas": len(resultados) - len(ejecutadas),
            "latencia_promedio_ms": round(statistics.mean(latencias), 2) if latencias else None,
            "latencia_mediana_ms": round(statistics.median(latencias), 2) if latencias else None,
            "latencia_maxima_ms": round(max(latencias), 2) if latencias else None,
            "dentro_sla": sum(r["dentro_sla"] for r in ejecutadas),
            "con_indice": sum(r["plan"] in ("IXSCAN", "MIXTO") for r in ejecutadas),
            "registros_analiticos": db.peliculas_analiticas.estimated_document_count()
                                    + db.fact_comentarios.estimated_document_count(),
            "consultas": resultados,
        }
        imprimir_resumen(resumen)
        if guardar:
            guardar_resultados(resumen)
        return resumen
    finally:
        cliente.close()
        print("[BENCHMARK] Conexion cerrada.")


def imprimir_resumen(r):
    print("\n" + "=" * 80)
    print(f" RESUMEN DE BENCHMARKING ({r['ejecutadas']} CONSULTAS EJECUTADAS, {r['omitidas']} OMITIDAS)")
    print("=" * 80)
    if not r["ejecutadas"]:
        print(" No se ejecuto ninguna consulta.")
        return
    print(f" Latencia promedio: {r['latencia_promedio_ms']} ms | mediana: {r['latencia_mediana_ms']} ms | "
          f"maxima: {r['latencia_maxima_ms']} ms")
    print(f" Latencia de red (piso minimo por consulta): {r['latencia_red_ms']} ms")
    print(f" Dentro del SLA (<= {r['sla_ms']:.0f} ms): {r['dentro_sla']}/{r['ejecutadas']}")
    print(f" Consultas que usan indice (segun explain): {r['con_indice']}/{r['ejecutadas']}")
    print(f" Registros analiticos (peliculas_analiticas + fact_comentarios): {r['registros_analiticos']:,}")
    print("=" * 80)
    lentas = [c for c in r["consultas"] if c["estado"] == "OK" and not c["dentro_sla"]]
    if not lentas:
        print(" CONCLUSION: todas las consultas cumplen el SLA con los indices y allowDiskUse actuales.")
    for c in lentas:
        pista = ("salida grande: agregar $limit o $project" if c["filas"] > 500
                 else "sin indice: evaluar un indice para el $match/$sort" if c["plan"] == "COLLSCAN"
                 else "revisar etapas $unwind/$group y el tier del cluster")
        print(f" FUERA DE SLA: C{c['numero']} {c['titulo']} ({c['latencia_ms']} ms) -> {pista}")
    print("=" * 80)


def guardar_resultados(resumen):
    CARPETA_REPORTES.mkdir(exist_ok=True)
    marca = datetime.now().strftime("%Y%m%d_%H%M%S")
    texto = json.dumps(resumen, ensure_ascii=False, indent=2, default=str)
    (CARPETA_REPORTES / f"benchmark_{marca}.json").write_text(texto, encoding="utf-8")
    ARCHIVO_ULTIMO.write_text(texto, encoding="utf-8")
    print(f"[BENCHMARK] Resultados guardados en reportes/benchmark_{marca}.json")


if __name__ == "__main__":
    ejecutar_benchmark()
