# Taller 3 – Parte II: Análisis de Tendencias Internacionales de YouTube

**Herramientas de Big Data** · Maestría en Analítica Aplicada · Universidad de la Sabana
Profesor: Hugo Franco, Ph.D.

**Estudiantes:** Gabriela Bejarano, Ana María Cordero y Juan Pablo Parrado

Pipeline ETL orquestado con **Prefect** sobre el dataset de Kaggle
[`datasnaek/youtube-new`](https://www.kaggle.com/datasets/datasnaek/youtube-new):

```
Kaggle ──► pandas (transformación) ──► PostgreSQL (OLTP) ──► DuckDB (OLAP) ──► agregados + KMeans
```

El informe con los resultados y las respuestas P1–P4 está en [`ANALISIS.pdf`](ANALISIS.pdf).

## Estructura

| Archivo | Contenido |
|---|---|
| `pipeline.py` | Las 11 tareas (`@task`) y el único flow `youtube_pipeline` |
| `config.py` | Rutas, nombre de la tabla y DSN de PostgreSQL (credenciales por variables de entorno) |
| `decorators.py` | `timing_decorator` y `report_size_decorator` |
| `analysis_outputs/*.csv` | Resultados de las tres agregaciones de `aggregate_insights` |
| `ANALISIS.pdf` | Informe según la plantilla del curso |

## Tareas del flow

| # | Tarea | Qué hace |
|---|---|---|
| 3.1.1 | `extract_data` | Descarga el dataset de Kaggle a `data/` y registra su tamaño en disco (retries=3) |
| 3.1.2 | `enumerate_country_files` | Mapea país → `*videos.csv` (exige 10 países; omite JSON) |
| 3.2.1 | `unify_countries` | Concatena los 10 CSV agregando la columna `country` |
| 3.2.2 | `top100_global` | Una fila por `video_id` (máximo `views`) y top 100 global |
| 3.2.3 | `standardize_dates` | Parsea `trending_date` (`AA.DD.MM`) y `publish_time` (ISO UTC) con `apply`+`lambda` y con `iterrows`, y verifica con `assert` que coinciden |
| 3.2.4 | `compute_days_to_trending` | Días entre publicación y tendencia; falla si hay valores negativos |
| 3.2.5 | `clean_records` | Descarta filas con faltantes en columnas numéricas |
| 3.3.1 | `load_data` | `DROP/CREATE` idempotente + `execute_values` en PostgreSQL (retries=3) |
| 3.4.1 | `duckdb_fetch` | `ATTACH ... (TYPE POSTGRES, READ_ONLY)` desde DuckDB y verificación del conteo |
| 3.4.2 | `aggregate_insights` | Videos por país, mediana de días por país, engagement por categoría → CSV |
| 3.4.3 | `cluster_videos` | KMeans K=2..8 sobre `days_to_trending` y `engagement_ratio`, K por silhouette |

Cada tarea usa `try/except/finally`, registra con `get_run_logger()` y vuelve a lanzar las
excepciones, de modo que cualquier fallo hace fallar el flow.

## Requisitos

- Python 3.12
- Contenedor PostgreSQL de la Sesión 2 escuchando en `localhost:5433`
- Credenciales de Kaggle en `~/.kaggle/kaggle.json`

```bash
pip install prefect duckdb pandas psycopg2-binary scikit-learn kaggle matplotlib
```

La conexión se puede cambiar con las variables de entorno `PG_HOST`, `PG_PORT`, `PG_DB`,
`PG_USER` y `PG_PASSWORD` (los valores por defecto están en `config.py`).

## Ejecución

```bash
prefect server start      # en otra terminal; UI en http://127.0.0.1:4200
python pipeline.py
```

El flow **“YouTube — Top-100 Multi-País”** termina con las 11 tareas en estado `Completed`.

## Resultados principales

- 375 942 filas crudas → 184 286 videos únicos → top 100 (sin filas descartadas por faltantes).
- `days_to_trending`: mínimo 6, mediana 31, máximo 38 días.
- Gran Bretaña aporta 85 de los 100 videos y tiene la mediana de días más alta (36).
- KMeans: K = 3 (silhouette 0,561).

`data/` y `kaggle.json` no se incluyen en el repositorio.
