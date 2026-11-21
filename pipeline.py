"""Taller 3 - Parte II: Análisis de Tendencias Internacionales de YouTube.

Pipeline ETL orquestado con Prefect:
  Extracción (Kaggle) -> Transformación (pandas) -> Carga (PostgreSQL, OLTP)
  -> Analítica (DuckDB sobre PostgreSQL, OLAP) -> Clustering (KMeans).

Ejecutar:  python pipeline.py
"""
import os
os.environ.setdefault("OMP_NUM_THREADS", "1")  # evita problemas de KMeans con Prefect

from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from config import (
    DOWNLOAD_DIRECTORY, EXPECTED_COUNTRIES, K_VALUES, LOAD_COLUMNS,
    OUTPUT_DIRECTORY, PG_DSN, RANDOM_STATE, REQUIRED_COLUMNS, TABLE_NAME,
    TARGET_DATASET, TOP_N,
)
from decorators import report_size_decorator, timing_decorator


# =============================================================================
# 3.1 EXTRACCIÓN
# =============================================================================
@task(name="extract_data", retries=3, retry_delay_seconds=15, cache_policy=NO_CACHE)
@timing_decorator
@report_size_decorator
def extract_data(download_path: Path = DOWNLOAD_DIRECTORY,
                 dataset_id: str = TARGET_DATASET) -> Path:
    """Descarga el dataset de Kaggle y lo descomprime en data/."""
    logger = get_run_logger()
    path = Path(download_path)
    try:
        # Import diferido: KaggleApi lee las credenciales de ~/.kaggle/kaggle.json
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading '{dataset_id}' into '{path.resolve()}'...")
        api.dataset_download_files(dataset_id, path=str(path), unzip=True, quiet=True)

        video_files = sorted(path.glob("*videos.csv"))
        if not video_files:
            raise FileNotFoundError(f"No *videos.csv files found in '{path}'.")
        logger.info(f"Download completed: {len(list(path.iterdir()))} files in '{path}'.")
        return path
    except Exception as e:
        logger.error(f"extract_data failed: {e}")
        raise
    finally:
        logger.info("extract_data task finished.")


@task(name="enumerate_country_files", cache_policy=NO_CACHE)
@timing_decorator
def enumerate_country_files(raw_path: Path) -> dict:
    """Mapea código de país -> CSV de videos (sin JSON ni *_comments.csv)."""
    logger = get_run_logger()
    try:
        files = {}
        for csv in sorted(Path(raw_path).glob("*.csv")):
            name = csv.name
            if name.endswith("_comments.csv") or not name.endswith("videos.csv"):
                continue
            files[name[:2].upper()] = csv
        logger.info(f"Country files found: {len(files)} -> {sorted(files)}")
        if len(files) != EXPECTED_COUNTRIES:
            raise ValueError(f"Expected {EXPECTED_COUNTRIES} country files, found {len(files)}.")
        return files
    except Exception as e:
        logger.error(f"enumerate_country_files failed: {e}")
        raise
    finally:
        logger.info("enumerate_country_files task finished.")


# =============================================================================
# 3.2 TRANSFORMACIÓN
# =============================================================================
def _read_country_csv(path: Path) -> pd.DataFrame:
    """Algunos CSV (p. ej. MX, RU) traen bytes no UTF-8 sueltos: se reemplazan
    en lugar de re-decodificar todo el archivo como latin-1 (que daña los acentos)."""
    return pd.read_csv(path, encoding="utf-8", encoding_errors="replace")


@task(name="unify_countries", cache_policy=NO_CACHE)
@timing_decorator
def unify_countries(files: dict) -> pd.DataFrame:
    """Lee cada CSV, agrega la columna country y concatena en un solo DataFrame."""
    logger = get_run_logger()
    try:
        frames = []
        for country, csv in files.items():
            df = _read_country_csv(csv)
            df["country"] = country
            logger.info(f"  {country}: {df.shape[0]:,} rows")
            frames.append(df)
        df_all = pd.concat(frames, ignore_index=True)
        logger.info(f"Unified shape: {df_all.shape[0]:,} rows x {df_all.shape[1]} columns")
        return df_all
    except Exception as e:
        logger.error(f"unify_countries failed: {e}")
        raise
    finally:
        logger.info("unify_countries task finished.")


@task(name="top100_global", cache_policy=NO_CACHE)
@timing_decorator
def top100_global(df_all: pd.DataFrame, top_n: int = TOP_N) -> pd.DataFrame:
    """Una fila por video_id (la de máximo views) y los top_n con más vistas."""
    logger = get_run_logger()
    try:
        # "#NAME?" es un video_id corrompido por Excel: agrupa muchos videos distintos,
        # así que no es una clave válida para de-duplicar.
        invalid = df_all["video_id"].isna() | (df_all["video_id"] == "#NAME?")
        logger.info(f"Discarding {int(invalid.sum()):,} rows with invalid video_id ('#NAME?')")
        df_valid = df_all[~invalid]
        df_max = (
            df_valid.sort_values("views", ascending=False, kind="mergesort")
                  .drop_duplicates(subset="video_id", keep="first")
        )
        logger.info(f"Unique videos after de-duplication: {df_max.shape[0]:,} "
                    f"(removed {df_valid.shape[0] - df_max.shape[0]:,} duplicate rows)")
        df_top = df_max.head(top_n).reset_index(drop=True)
        dist = df_top["country"].value_counts()
        logger.info(f"Top-{top_n} shape: {df_top.shape}")
        logger.info(f"Country distribution in top {top_n}:\n{dist.to_string()}")
        return df_top
    except Exception as e:
        logger.error(f"top100_global failed: {e}")
        raise
    finally:
        logger.info("top100_global task finished.")


@task(name="standardize_dates", cache_policy=NO_CACHE)
@timing_decorator
def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parseo de trending_date (AA.DD.MM) y publish_time (ISO 8601 UTC) de dos formas."""
    logger = get_run_logger()
    try:
        df = df.copy()
        # Forma 1: apply + lambda (vectorizada, la de producción)
        df["trending_date_parsed"] = df["trending_date"].apply(
            lambda s: pd.to_datetime(s, format="%y.%d.%m")
        )
        df["publish_time_parsed"] = df["publish_time"].apply(
            lambda s: pd.to_datetime(s, utc=True)
        )
        # Forma 2: iterrows explícito (verificación cruzada fila por fila)
        df["publish_time_iter"] = pd.Series(pd.NaT, index=df.index,
                                            dtype=df["publish_time_parsed"].dtype)
        for idx, row in df.iterrows():
            df.at[idx, "publish_time_iter"] = pd.to_datetime(row["publish_time"], utc=True)

        try:
            assert (df["publish_time_parsed"] == df["publish_time_iter"]).all(), \
                "apply/lambda and iterrows parsing of publish_time differ"
        except AssertionError as e:
            logger.error(f"Date parsing cross-check FAILED: {e}")
            raise
        logger.info("Cross-check OK: apply+lambda == iterrows for publish_time "
                    f"({len(df)} rows).")

        sample = df.loc[0, ["trending_date", "trending_date_parsed"]]
        logger.info(f"Sample trending_date: '{sample['trending_date']}' -> "
                    f"{sample['trending_date_parsed'].date()}")
        return df
    except Exception as e:
        logger.error(f"standardize_dates failed: {e}")
        raise
    finally:
        logger.info("standardize_dates task finished.")


@task(name="compute_days_to_trending", cache_policy=NO_CACHE)
@timing_decorator
def compute_days_to_trending(df: pd.DataFrame) -> pd.DataFrame:
    """Días entre la publicación y la fecha de tendencia (0 = mismo día)."""
    logger = get_run_logger()
    try:
        df = df.copy()
        df["days_to_trending"] = (
            df["trending_date_parsed"].dt.normalize()
            - df["publish_time_parsed"].dt.tz_localize(None).dt.normalize()
        ).dt.days
        d = df["days_to_trending"]
        logger.info(f"days_to_trending -> min={d.min()}, median={d.median()}, max={d.max()}")
        if d.min() < 0:
            logger.warning(f"Negative days_to_trending found (min={d.min()}): parsing error.")
            raise ValueError("Negative days_to_trending: trending_date/publish_time mis-parsed.")
        return df
    except Exception as e:
        logger.error(f"compute_days_to_trending failed: {e}")
        raise
    finally:
        logger.info("compute_days_to_trending task finished.")


@task(name="clean_records", cache_policy=NO_CACHE)
@timing_decorator
def clean_records(df: pd.DataFrame, required: list = REQUIRED_COLUMNS) -> pd.DataFrame:
    """Descarta filas con faltantes en las columnas numéricas requeridas."""
    logger = get_run_logger()
    try:
        missing = df[required].isna().sum()
        for col, n in missing.items():
            if n:
                logger.info(f"  {n} row(s) missing '{col}'")
        df_clean = df.dropna(subset=required).reset_index(drop=True)
        dropped = len(df) - len(df_clean)
        logger.info(f"Dropped {dropped} row(s) with missing values in {required}; "
                    f"clean shape: {df_clean.shape}")
        return df_clean
    except Exception as e:
        logger.error(f"clean_records failed: {e}")
        raise
    finally:
        logger.info("clean_records task finished.")


# =============================================================================
# 3.3 CARGA (OLTP -> PostgreSQL)
# =============================================================================
@task(name="load_data", retries=3, retry_delay_seconds=15, cache_policy=NO_CACHE)
@timing_decorator
def load_data(df: pd.DataFrame, table_name: str = TABLE_NAME, dsn: str = PG_DSN) -> int:
    """DROP/CREATE idempotente + inserción masiva con execute_values."""
    logger = get_run_logger()
    conn = cur = None
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                video_id             TEXT PRIMARY KEY,
                country              CHAR(2)  NOT NULL,
                title                TEXT,
                category_id          INTEGER  NOT NULL,
                views                BIGINT   NOT NULL,
                likes                BIGINT   NOT NULL,
                dislikes             BIGINT   NOT NULL,
                comment_count        BIGINT   NOT NULL,
                days_to_trending     INTEGER  NOT NULL,
                trending_date_parsed DATE     NOT NULL,
                publish_time_parsed  TIMESTAMPTZ NOT NULL
            );
        """)
        rows = [
            (
                r.video_id, r.country, r.title, int(r.category_id), int(r.views),
                int(r.likes), int(r.dislikes), int(r.comment_count),
                int(r.days_to_trending), r.trending_date_parsed.date(),
                r.publish_time_parsed.to_pydatetime(),
            )
            for r in df[LOAD_COLUMNS].itertuples(index=False)
        ]
        execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(LOAD_COLUMNS)}) VALUES %s",
            rows,
        )
        conn.commit()
        logger.info(f"Inserted {len(rows)} rows into {table_name}")
        return len(rows)
    except Exception as e:
        if conn is not None:
            conn.rollback()
        logger.error(f"load_data failed (is the PostgreSQL container on port 5433 up?): {e}")
        raise
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        logger.info("load_data task finished.")


# =============================================================================
# 3.4 ANALÍTICA (OLAP -> DuckDB sobre PostgreSQL)
# =============================================================================
@task(name="duckdb_fetch", cache_policy=NO_CACHE)
@timing_decorator
def duckdb_fetch(table_name: str = TABLE_NAME, expected_rows: int | None = None,
                 dsn: str = PG_DSN) -> pd.DataFrame:
    """ATTACH READ_ONLY a PostgreSQL desde DuckDB y SELECT hacia pandas."""
    logger = get_run_logger()
    con = None
    try:
        con = duckdb.connect()
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES, READ_ONLY);")
        df = con.execute(f"""
            SELECT video_id, country, title, category_id, views, likes, dislikes,
                   comment_count, days_to_trending,
                   trending_date_parsed, publish_time_parsed
            FROM pg.public.{table_name}
        """).df()
        logger.info(f"DuckDB fetched shape: {df.shape}")
        if expected_rows is not None:
            assert len(df) == expected_rows, \
                f"Row count mismatch: fetched {len(df)} vs inserted {expected_rows}"
            logger.info(f"Row count verified against load_data: {expected_rows}")
        return df
    except Exception as e:
        logger.error(f"duckdb_fetch failed: {e}")
        raise
    finally:
        if con is not None:
            con.close()
        logger.info("duckdb_fetch task finished (DuckDB connection closed).")


def _engagement(df: pd.DataFrame) -> pd.Series:
    return (df["likes"] + df["dislikes"]) / (df["views"] + 1)


@task(name="aggregate_insights", cache_policy=NO_CACHE)
@timing_decorator
def aggregate_insights(df: pd.DataFrame, out_dir: Path = OUTPUT_DIRECTORY) -> dict:
    """Tres GROUP BY: videos por país, mediana de días por país, engagement por categoría."""
    logger = get_run_logger()
    try:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        df = df.copy()
        df["engagement_ratio"] = _engagement(df)

        videos_by_country = (
            df.groupby("country").size().rename("video_count")
              .sort_values(ascending=False).reset_index()
        )
        median_days_by_country = (
            df.groupby("country")["days_to_trending"].median()
              .rename("median_days_to_trending").sort_values(ascending=False).reset_index()
        )
        engagement_by_category = (
            df.groupby("category_id")
              .agg(video_count=("video_id", "size"),
                   median_engagement_ratio=("engagement_ratio", "median"))
              .sort_values("median_engagement_ratio", ascending=False).reset_index()
        )
        results = {
            "videos_by_country": videos_by_country,
            "median_days_to_trending_by_country": median_days_by_country,
            "engagement_ratio_by_category": engagement_by_category,
        }
        for name, table in results.items():
            path = out_dir / f"{name}.csv"
            table.to_csv(path, index=False)
            logger.info(f"{name} -> {path}\n{table.to_string(index=False)}")
        return results
    except Exception as e:
        logger.error(f"aggregate_insights failed: {e}")
        raise
    finally:
        logger.info("aggregate_insights task finished.")


@task(name="cluster_videos", cache_policy=NO_CACHE)
@timing_decorator
def cluster_videos(df: pd.DataFrame, k_values: list = K_VALUES,
                   out_dir: Path = OUTPUT_DIRECTORY) -> dict:
    """KMeans sobre (days_to_trending, engagement_ratio), K elegido por silhouette."""
    logger = get_run_logger()
    try:
        df = df.copy()
        if "engagement_ratio" not in df.columns:
            df["engagement_ratio"] = _engagement(df)
        features = ["days_to_trending", "engagement_ratio"]
        X = df[features].astype(float)
        # Las dos variables están en escalas muy distintas (días vs. razón ~0.0x):
        # se estandarizan para que ambas pesen en la distancia euclidiana.
        X_scaled = StandardScaler().fit_transform(X)

        scores = {}
        for k in k_values:
            labels = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10).fit_predict(X_scaled)
            scores[k] = silhouette_score(X_scaled, labels)
        logger.info(f"Silhouette sweep: { {k: round(v, 4) for k, v in scores.items()} }")

        best_k = max(scores, key=scores.get)
        logger.info(f"Best K = {best_k} (silhouette = {scores[best_k]:.4f})")

        model = KMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=10).fit(X_scaled)
        df["cluster"] = model.labels_
        centroids = df.groupby("cluster")[features].mean()
        centroids["n_videos"] = df.groupby("cluster").size()
        logger.info(f"Cluster centroid means (original units):\n{centroids.to_string()}")

        # Figura para el informe
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
        ax1.plot(list(scores), list(scores.values()), marker="o")
        ax1.axvline(best_k, ls="--", c="gray")
        ax1.set(xlabel="K", ylabel="Silhouette", title="Silhouette por K")
        ax1.grid(alpha=.3)
        sc = ax2.scatter(df["days_to_trending"], df["engagement_ratio"], c=df["cluster"],
                         cmap="viridis", alpha=.85)
        ax2.set(xlabel="days_to_trending", ylabel="engagement_ratio",
                title=f"KMeans K={best_k}")
        ax2.legend(*sc.legend_elements(), title="Cluster")
        ax2.grid(alpha=.3)
        fig.tight_layout()
        fig.savefig(out_dir / "clusters.png", dpi=150)
        plt.close(fig)

        return {"best_k": best_k, "silhouette": scores[best_k], "scores": scores,
                "centroids": centroids}
    except Exception as e:
        logger.error(f"cluster_videos failed: {e}")
        raise
    finally:
        logger.info("cluster_videos task finished.")


# =============================================================================
# 3.5 FLOW
# =============================================================================
@flow(name="YouTube — Top-100 Multi-País", log_prints=True)
def youtube_pipeline():
    raw_path = extract_data()
    files    = enumerate_country_files(raw_path)
    df_all   = unify_countries(files)
    df_top   = top100_global(df_all)
    df_top   = standardize_dates(df_top)
    df_top   = compute_days_to_trending(df_top)
    df_top   = clean_records(df_top)
    n_loaded = load_data(df_top, TABLE_NAME)

    df_analysis = duckdb_fetch(TABLE_NAME, expected_rows=n_loaded)
    aggregate_insights(df_analysis)
    cluster_videos(df_analysis)


if __name__ == "__main__":
    youtube_pipeline()
