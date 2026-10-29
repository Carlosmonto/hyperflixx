"""Job PySpark: RAW del Data Lake -> datos analiticos para Data Warehouse.

El trabajo localiza automaticamente el ultimo archivo RAW de movies, aplica
limpieza, tipado, deduplicacion, explosion de generos y agregacion analitica.

Ejecutar desde la raiz del proyecto:
    python scripts/spark_jobs.py
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
RAIZ_RAW = RAIZ_PROYECTO / "data_lake" / "raw" / "movies"
RAIZ_CURATED = RAIZ_PROYECTO / "data_lake" / "curated" / "spark"
HADOOP_HOME_LOCAL = RAIZ_PROYECTO / "tools" / "hadoop"


def configurar_hadoop_windows() -> None:
    """Configura winutils localmente para que Spark pueda escribir en Windows."""
    if os.name != "nt":
        return
    bin_hadoop = HADOOP_HOME_LOCAL / "bin"
    winutils = bin_hadoop / "winutils.exe"
    if not winutils.exists():
        raise FileNotFoundError(
            f"Falta {winutils}. Consulta tools/hadoop/README.md para instalarlo."
        )
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME_LOCAL)
    os.environ["hadoop.home.dir"] = str(HADOOP_HOME_LOCAL)
    os.environ["PATH"] = f"{bin_hadoop}{os.pathsep}{os.environ['PATH']}"


def obtener_raw_mas_reciente() -> Path:
    """Devuelve movies.jsonl de la particion de carga mas reciente."""
    archivos = list(RAIZ_RAW.glob("fecha_carga=*/movies.jsonl"))
    if not archivos:
        raise FileNotFoundError(
            "No hay archivos RAW. Ejecuta primero: python scripts/pipeline_data_lake.py"
        )
    return max(archivos, key=lambda archivo: archivo.parent.name)


def ejecutar_job() -> None:
    configurar_hadoop_windows()
    origen = obtener_raw_mas_reciente()
    fecha_carga = origen.parent.name.removeprefix("fecha_carga=")
    destino = RAIZ_CURATED / "peliculas_por_genero" / f"fecha_carga={fecha_carga}"

    spark = (
        SparkSession.builder.appName("DataLakeMoviesSparkJob")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.json(str(origen))

        # Transformacion 1: seleccionar campos, renombrar y convertir tipos.
        peliculas = raw.select(
            F.col("_id.$oid").alias("movie_id"),
            F.trim(F.coalesce(F.col("title"), F.lit("Sin titulo"))).alias("titulo"),
            # En sample_mflix hay valores como "1994–1998"; se conserva el
            # primer año de cuatro digitos y los valores invalidos quedan nulos.
            F.expr(
                "try_cast(regexp_extract(CAST(year AS STRING), '^([0-9]{4})', 1) AS INT)"
            ).alias("anio"),
            F.col("imdb.rating").cast("double").alias("calificacion_imdb"),
            F.col("imdb.votes").cast("long").alias("votos_imdb"),
            F.coalesce(F.col("genres"), F.array()).alias("generos"),
            F.coalesce(F.col("countries"), F.array()).alias("paises"),
        )

        # Transformacion 2: eliminar registros sin identificador y duplicados.
        peliculas_limpias = peliculas.filter(F.col("movie_id").isNotNull()).dropDuplicates(["movie_id"])

        # Transformacion 3: una pelicula con varios generos pasa a una fila por genero.
        peliculas_por_genero = peliculas_limpias.withColumn("genero", F.explode_outer("generos"))
        peliculas_por_genero = peliculas_por_genero.withColumn(
            "genero", F.coalesce(F.col("genero"), F.lit("Sin genero"))
        )

        # Transformacion 4: agregacion que genera una tabla de hechos para BI/DWH.
        warehouse = (
            peliculas_por_genero.groupBy("anio", "genero")
            .agg(
                F.countDistinct("movie_id").alias("total_peliculas"),
                F.round(F.avg("calificacion_imdb"), 2).alias("promedio_calificacion_imdb"),
                F.sum(F.coalesce("votos_imdb", F.lit(0))).alias("total_votos_imdb"),
            )
            .withColumn("fecha_carga", F.lit(fecha_carga))
            .orderBy("anio", "genero")
        )

        warehouse.write.mode("overwrite").parquet(str(destino))
        print(f"[SPARK] RAW leido: {origen.relative_to(RAIZ_PROYECTO)}")
        print(f"[SPARK] Filas para Data Warehouse: {warehouse.count()}")
        print(f"[SPARK] Parquet generado: {destino.relative_to(RAIZ_PROYECTO)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        ejecutar_job()
    except Exception as error:
        print(f"[SPARK] Error: {error}")
        sys.exit(1)
