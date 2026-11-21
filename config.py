"""Configuración central del pipeline (única fuente de verdad).

Las credenciales se leen de variables de entorno; los valores por defecto
corresponden al contenedor PostgreSQL local de la Sesión 2 (puerto 5433).
Las tareas nunca escriben credenciales: solo importan estas constantes.
"""
import os
from pathlib import Path

# --- Kaggle ---
TARGET_DATASET = "datasnaek/youtube-new"
DOWNLOAD_DIRECTORY = Path("data")
EXPECTED_COUNTRIES = 10

# --- PostgreSQL (OLTP) ---
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "bigdatatools1")
PG_USER = os.getenv("PG_USER", "psqluser")
PG_PASSWORD = os.getenv("PG_PASSWORD", "psqlpass")

TABLE_NAME = "youtube_top100_global"

# DSN libpq: un solo string, usado tanto por psycopg2 como por el ATTACH de DuckDB
PG_DSN = (
    f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
    f"user={PG_USER} password={PG_PASSWORD}"
)

# --- Transformación ---
TOP_N = 100
REQUIRED_COLUMNS = [
    "views", "likes", "dislikes", "comment_count",
    "days_to_trending", "category_id",
]
LOAD_COLUMNS = [
    "video_id", "country", "title", "category_id", "views", "likes",
    "dislikes", "comment_count", "days_to_trending",
    "trending_date_parsed", "publish_time_parsed",
]

# --- Analítica ---
OUTPUT_DIRECTORY = Path("analysis_outputs")
K_VALUES = list(range(2, 9))  # K = 2..8
RANDOM_STATE = 42
