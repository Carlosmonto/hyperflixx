"""Carga el Data Warehouse derivado de MongoDB en Google BigQuery.

Ejecutar despues de transformar_oltp_a_olap.py y construir_data_warehouse.py.
Cada tabla se carga primero en staging; solo al completar la lectura de MongoDB
se reemplaza la tabla publicada, evitando dejar una carga parcial visible.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

from bson import ObjectId
from google.cloud import bigquery

sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb


ESQUEMAS = {
    "dim_tiempo": [
        ("fecha_id", "STRING"), ("fecha", "DATE"), ("anio", "INT64"),
        ("mes", "INT64"), ("dia", "INT64"), ("dia_semana", "INT64"),
        ("trimestre", "INT64"), ("semestre", "INT64"), ("nombre_mes", "STRING"),
        ("nombre_dia", "STRING"), ("es_fin_de_semana", "BOOL"),
        ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "dim_usuario": [
        ("usuario_id", "STRING"), ("email", "STRING"), ("nombre", "STRING"),
        ("total_comentarios", "INT64"), ("peliculas_comentadas", "INT64"),
        ("primer_comentario", "TIMESTAMP"), ("ultimo_comentario", "TIMESTAMP"),
        ("longitud_promedio", "FLOAT64"), ("es_usuario_registrado", "BOOL"),
        ("segmento", "STRING"), ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "dim_pelicula": [
        ("pelicula_id", "STRING"), ("titulo", "STRING"), ("anio", "INT64"),
        ("tipo", "STRING"), ("clasificacion", "STRING"),
        ("generos", "STRING", "REPEATED"), ("genero_principal", "STRING"),
        ("pais_principal", "STRING"), ("idioma_principal", "STRING"),
        ("duracion_minutos", "INT64"), ("calificacion_imdb", "FLOAT64"),
        ("votos_imdb", "INT64"), ("calificacion_audiencia", "FLOAT64"),
        ("premios_ganados", "INT64"), ("nominaciones", "INT64"),
        ("decada", "INT64"), ("rango_calificacion", "STRING"),
        ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "fact_comentarios": [
        ("comentario_id", "STRING"), ("fecha_id", "STRING"),
        ("usuario_id", "STRING"), ("pelicula_id", "STRING"),
        ("fecha_hora", "TIMESTAMP"), ("cantidad", "INT64"),
        ("longitud_texto", "INT64"), ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "agg_comentarios_diarios": [
        ("fecha_id", "STRING"), ("comentarios", "INT64"),
        ("longitud_total", "INT64"), ("anio", "INT64"), ("mes", "INT64"),
        ("trimestre", "INT64"), ("dia_semana", "INT64"),
        ("nombre_dia", "STRING"), ("es_fin_de_semana", "BOOL"),
        ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "agg_genero_decada": [
        ("agg_id", "STRING"), ("genero", "STRING"), ("decada", "INT64"),
        ("peliculas", "INT64"), ("rating_promedio", "FLOAT64"),
        ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "agg_genero_top": [
        ("genero", "STRING"),
        ("top_peliculas", "RECORD", "REPEATED", [
            ("titulo", "STRING"), ("imdb", "FLOAT64"),
        ]),
        ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "agg_pais_rating": [
        ("pais", "STRING"), ("peliculas", "INT64"),
        ("rating_promedio", "FLOAT64"), ("fecha_actualizacion", "TIMESTAMP"),
    ],
    "agg_genero_premios": [
        ("genero", "STRING"), ("total", "INT64"), ("premiadas", "INT64"),
        ("tasa_premiadas_pct", "FLOAT64"), ("fecha_actualizacion", "TIMESTAMP"),
    ],
}


def convertir_esquema(campos: list[tuple]) -> list[bigquery.SchemaField]:
    resultado = []
    for campo in campos:
        nombre, tipo, *opciones = campo
        modo = "NULLABLE"
        subcampos = None
        if opciones and isinstance(opciones[0], str):
            modo = opciones[0]
            if len(opciones) > 1:
                subcampos = convertir_esquema(opciones[1])
        resultado.append(bigquery.SchemaField(nombre, tipo, mode=modo, fields=subcampos))
    return resultado


def serializar_valor(valor):
    """Deja los BSON en tipos JSON compatibles con los esquemas explícitos."""
    if isinstance(valor, ObjectId):
        return str(valor)
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    if isinstance(valor, dict):
        resultado = {}
        for llave, contenido in valor.items():
            if llave == "_id":
                continue
            if llave == "fecha" and isinstance(contenido, (datetime, date)):
                resultado[str(llave)] = contenido.date().isoformat() if isinstance(contenido, datetime) else contenido.isoformat()
            else:
                resultado[str(llave)] = serializar_valor(contenido)
        return resultado
    if isinstance(valor, list):
        return [serializar_valor(item) for item in valor]
    if isinstance(valor, bytes):
        return valor.decode("utf-8", errors="replace")
    return valor


def cargar_lote(cliente, referencia, filas, esquema, disposicion):
    config = bigquery.LoadJobConfig(
        schema=esquema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=disposicion,
    )
    cliente.load_table_from_json(filas, referencia, job_config=config).result()


def exportar_coleccion(db, cliente, dataset_ref, nombre, tamano_lote):
    esquema = convertir_esquema(ESQUEMAS[nombre])
    staging = dataset_ref.table(f"_stg_{nombre}_{uuid.uuid4().hex[:12]}")
    destino = dataset_ref.table(nombre)
    lote = []
    total = 0
    disposicion = bigquery.WriteDisposition.WRITE_TRUNCATE
    try:
        for documento in db[nombre].find({}).batch_size(tamano_lote):
            lote.append(serializar_valor(documento))
            if len(lote) == tamano_lote:
                cargar_lote(cliente, staging, lote, esquema, disposicion)
                total += len(lote)
                print(f"  [{nombre}] {total:,} filas leídas")
                lote = []
                disposicion = bigquery.WriteDisposition.WRITE_APPEND

        if lote:
            cargar_lote(cliente, staging, lote, esquema, disposicion)
            total += len(lote)
        elif total == 0:
            # Asegura que una colección vacía también publique tabla y esquema.
            cliente.create_table(bigquery.Table(staging, schema=esquema), exists_ok=True)

        cliente.copy_table(
            staging, destino,
            job_config=bigquery.CopyJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
        ).result()
        print(f"  [{nombre}] {total:,} filas publicadas")
        return total
    finally:
        cliente.delete_table(staging, not_found_ok=True)


def ejecutar_exportacion() -> None:
    proyecto = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BIGQUERY_DATASET", "mongo_data_warehouse")
    tamano_lote = int(os.getenv("BIGQUERY_BATCH_ROWS", "5000"))
    if not proyecto:
        raise RuntimeError("Falta GCP_PROJECT_ID en el entorno o archivo .env.")
    if tamano_lote < 1:
        raise ValueError("BIGQUERY_BATCH_ROWS debe ser mayor que cero.")

    cliente_bq = bigquery.Client(project=proyecto)
    dataset_ref = bigquery.DatasetReference(
        proyecto, dataset
    )
    dataset_obj = bigquery.Dataset(dataset_ref)
    dataset_obj.location = os.getenv("BIGQUERY_LOCATION", "US")
    cliente_bq.create_dataset(dataset_obj, exists_ok=True)
    cliente_mongo, db = conectar_mongodb(
        read_preference=os.getenv("DWH_READ_PREFERENCE", "secondaryPreferred"),
        validar=False,
    )
    try:
        print(f"[BQ] Proyecto: {proyecto} | dataset: {dataset} | region: {os.getenv('BIGQUERY_LOCATION', 'US')}")
        for nombre in ESQUEMAS:
            exportar_coleccion(db, cliente_bq, dataset_ref, nombre, tamano_lote)
        print("[BQ] Exportación Atlas -> BigQuery finalizada.")
    finally:
        cliente_mongo.close()
        cliente_bq.close()


if __name__ == "__main__":
    ejecutar_exportacion()
