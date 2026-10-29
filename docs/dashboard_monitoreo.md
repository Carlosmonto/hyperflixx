# Benchmarking y dashboard de monitoreo (sample_mflix)

Adaptación del documento de referencia (basado en `ventas_analiticas`) al proyecto.

## Orden de ejecución

1. `pip install -r requirements.txt` (agrega `matplotlib`)
2. Construir las capas: `transformar_oltp_a_olap.py` y `construir_data_warehouse.py`
3. `python scripts/benchmark_consultas_analiticas.py`
4. `python scripts/dashboard_monitoreo.py` (o con `--benchmark` para medir de nuevo)

Los resultados quedan en `reportes/`: `benchmark_*.json` y `dashboard_monitoreo_*.png`.

## Las 10 consultas

| #  | Consulta (documento → proyecto)                                   | Colección              | Índice                     |
|----|-------------------------------------------------------------------|------------------------|----------------------------|
| 1  | Ventas mensuales → estrenos y rating por año                      | peliculas_analiticas   | year_rating                |
| 2  | Top productos por categoría → top 5 películas por género          | peliculas_analiticas   | idx_tipo_votos             |
| 3  | Segmento × ciudad (salida grande) → matriz género × década        | agg_genero_decada      | idx_genero_decada          |
| 4  | Ticket por método de pago → ranking de países por rating          | peliculas_analiticas   | (baja selectividad)        |
| 5  | Ventas por día de la semana → comentarios por día de la semana    | agg_comentarios_diarios| idx_dia_semana_cubierto    |
| 6  | Tasa de cancelación → tasa de películas premiadas por género      | peliculas_analiticas   | (baja selectividad)        |
| 7  | Clientes VIP → usuarios más activos                               | dim_usuario            | idx_total_comentarios      |
| 8  | Ventas trimestrales → comentarios trimestrales desde 2015         | agg_comentarios_diarios| idx_fecha_trimestre_cubierto |
| 9  | Ticket por ciudad y segmento → IMDb vs audiencia (R, PG-13)       | peliculas_analiticas   | idx_clasificacion_anio     |
| 10 | Productos con mayor rotación → películas con más comentarios      | peliculas_analiticas   | idx_total_comentarios      |

## Cómo se llevaron todas las consultas a < 500 ms

Primera medición real (22-09-2026): 3 consultas fuera del SLA.

| Consulta | Antes | Causa | Solución |
|---|---|---|---|
| C5 Día de la semana | 2457–2538 ms (COLLSCAN) | Recorría todos los comentarios y hacía un `$lookup` a `dim_tiempo` por cada día distinto | Tabla agregada `agg_comentarios_diarios` + índice cubierto |
| C8 Trimestral | 664–753 ms | Mismo `$lookup` por día, sobre los comentarios desde 2015 | Misma tabla agregada + índice cubierto |
| C3 Género × década | 394–576 ms | `$unwind` sobre todo el catálogo y 244 filas en 2 viajes de red | Cubo `agg_genero_decada` + `batchSize` |

**Tabla agregada (cubo precalculado):** el trabajo pesado (`$group` + `$lookup`) se hace una sola vez
en el ETL (`construir_data_warehouse.py`) y la consulta solo lee el resultado. Es la técnica estándar
de los Data Warehouse cuando el SLA es estricto. Costo: hay que volver a ejecutar el ETL cuando
entran datos nuevos.

**Índice cubierto:** contiene todos los campos que usa la consulta, así MongoDB responde leyendo
solo el índice, sin abrir los documentos.

**batchSize:** MongoDB envía por defecto 101 documentos en el primer lote y pide el resto con otro
viaje de red (`getMore`). Con ping de ~275 ms, 244 filas costaban dos viajes solo de red.

**Límite que no depende del código:** ninguna consulta puede tardar menos que el ping a Atlas
(medido entre 96 y 276 ms). Si la red está lenta, consultas simples como C7 y C10 se acercan al SLA.

## Mejoras respecto al documento

- **Plan de ejecución verificado:** el documento afirma que hay "Index Scan" sin comprobarlo.
  Aquí cada consulta pasa por `explain()` y se reporta IXSCAN, COLLSCAN o MIXTO.
- **Medición más estable:** una ejecución de calentamiento y luego la mediana de N repeticiones,
  en lugar de una sola medición.
- **Latencia de red separada:** se mide el `ping` a Atlas. Esa cifra es el piso de cualquier
  consulta; si la latencia está cerca del ping, el cuello de botella es la red, no el índice.
- **Consultas 4 y 6 sin índice útil a propósito:** filtran `tipo: "movie"`, que cubre casi toda
  la colección. Sirven para mostrar que un índice sobre un campo poco selectivo no ayuda.
- **Dashboard sin gráficos engañosos:** el pastel de costos y la barra "normalizada" del documento
  no se podían leer (un valor ocupaba el 99%). Se reemplazaron por almacenamiento real por colección
  (datos vs índices, desde `$collStats`) y por el uso del límite del plan de Atlas.
- **Mes en curso marcado como parcial:** en el documento el último mes parece una caída de ventas;
  en realidad el mes no había terminado.
- **Costo configurable, no inventado:** `ATLAS_PLAN`, `ATLAS_COSTO_MENSUAL` y `ATLAS_LIMITE_MB`
  en `.env` (por defecto M0 gratuito, 512 MB).
- **Sin emojis en el PNG:** matplotlib los mostraba como cuadros vacíos en el resumen del documento.
