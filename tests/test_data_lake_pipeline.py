from datetime import datetime, timezone

import pandas as pd

from scripts.pipeline_data_lake import (
    construir_curated,
    clave_particion,
    transformar_processed,
)


def test_clave_particion_usa_la_convencion_del_data_lake():
    assert clave_particion("raw", "2026-10-28", "movies.jsonl") == (
        "raw/movies/fecha_carga=2026-10-28/movies.jsonl"
    )


def test_transformar_processed_normaliza_tipos_y_valores_faltantes():
    registros = [
        {
            "_id": "movie-1",
            "title": "  Example  ",
            "year": "2024",
            "released": datetime(2024, 1, 2, tzinfo=timezone.utc),
            "imdb": {"rating": "8.5", "votes": "120"},
            "genres": ["Drama"],
        },
        {
            "_id": "movie-1",
            "title": "Duplicado",
            "year": "2024",
        },
    ]

    resultado = transformar_processed(registros)

    assert len(resultado) == 1
    assert resultado.loc[0, "movie_id"] == "movie-1"
    assert resultado.loc[0, "titulo"] == "Example"
    assert resultado.loc[0, "anio"] == 2024
    assert resultado.loc[0, "votos_imdb"] == 120
    assert resultado.loc[0, "paises"] == []


def test_construir_curated_calcula_dimensiones_y_popularidad():
    processed = pd.DataFrame(
        [
            {
                "movie_id": "movie-1",
                "titulo": "Example",
                "anio": 2024,
                "fecha_estreno": pd.Timestamp("2024-01-02", tz="UTC"),
                "tipo": "movie",
                "clasificacion": "PG",
                "duracion_minutos": 100,
                "generos": ["Drama", "Mystery"],
                "paises": ["USA"],
                "calificacion_imdb": 8.5,
                "votos_imdb": 120,
                "calificacion_audiencia": 4.0,
                "resenas_audiencia": 20,
                "premios_ganados": 2,
                "nominaciones": 3,
            }
        ]
    )

    resultado = construir_curated(processed, "2026-10-28")

    assert resultado.loc[0, "genero_principal"] == "Drama"
    assert resultado.loc[0, "pais_principal"] == "USA"
    assert resultado.loc[0, "decada"] == 2020
    assert resultado.loc[0, "popularidad_imdb"] == 1020
    assert resultado.loc[0, "fecha_carga"] == "2026-10-28"
