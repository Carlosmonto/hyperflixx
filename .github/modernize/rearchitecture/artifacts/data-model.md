# Modelo de datos existente

## Fuentes OLTP en `sample_mflix`

- `movies`: identificador, título, año, géneros, países, idiomas, duración, ratings IMDb,
  votos, tomates, premios y metadatos de película.
- `comments`: `movie_id`, `email`, `name`, `text` y `date`; cada comentario referencia
  una película.
- `users`: usuarios registrados, relacionados por `email`.
- `theaters`: cines y `location.geo`, indexado geoespacial.
- `sessions` y `embedded_movies`: colecciones del dump original, no son fuentes de los
  pipelines principales.

## Modelo OLAP y warehouse en MongoDB

- `peliculas_analiticas`: una fila derivada por película; combina `movies` con conteos y
  fecha del último comentario.
- `dim_tiempo`: una fila por fecha con comentarios, llave `fecha_id`.
- `dim_usuario`: una fila por email comentarista, con actividad y pertenencia a `users`.
- `dim_pelicula`: dimensión de catálogo derivada de `peliculas_analiticas`.
- `fact_comentarios`: hechos de comentarios, con `comentario_id`, `fecha_id`,
  `usuario_id` y `pelicula_id`.
- `agg_comentarios_diarios`, `agg_genero_decada`, `agg_genero_top`, `agg_pais_rating` y
  `agg_genero_premios`: cubos agregados para consultas y benchmarks.

## Persistencia externa

S3 usa particiones `zona/movies/fecha_carga=YYYY-MM-DD/` con JSONL en RAW y Parquet en
PROCESSED/CURATED. BigQuery replica las nueve colecciones del warehouse con esquemas
explícitos y tablas staging temporales.

## Llaves e integridad

MongoDB conserva `_id`/ObjectId en OLTP. Las dimensiones y hechos usan llaves string
normalizadas; índices únicos implementan las llaves de `$merge`. `fact_comentarios`
mantiene referencias lógicas a las tres dimensiones, validadas por el script del
warehouse, no por una FK nativa.
