"""Decoradores personalizados (Taller 3 Parte I).

Se apilan DEBAJO de @task, de modo que Prefect envuelve la función ya decorada
y get_run_logger() está disponible dentro del contexto de la tarea.
"""
import time
from functools import wraps
from pathlib import Path
from typing import Callable

from prefect import get_run_logger


def timing_decorator(func: Callable) -> Callable:
    """Mide y registra el tiempo de ejecución de la tarea (incluso si falla)."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_run_logger()
        start_time = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.time() - start_time
            logger.info(f"Task '{func.__name__}' completed in {elapsed:.2f} seconds")
    return wrapper


def _disk_size(path: Path) -> int:
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _human(n_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n_bytes < 1024:
            return f"{n_bytes:.2f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.2f} TB"


def report_size_decorator(func: Callable) -> Callable:
    """Registra el tamaño en disco de la ruta (archivo o directorio) que retorna la tarea."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_run_logger()
        result = func(*args, **kwargs)
        size = _disk_size(result)
        logger.info(f"Task '{func.__name__}' output '{result}' size on disk: "
                    f"{_human(size)} ({size:,} bytes)")
        return result
    return wrapper
