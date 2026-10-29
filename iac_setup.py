"""Prepara el entorno local reproducible del proyecto Mongo Data Lake.

Este script no crea recursos remotos de Atlas. Terraform se encarga de Atlas;
este orquestador prepara Python, el Data Lake local y valida la conexion.
Ejecutar desde cualquier directorio con: python iac_setup.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
VENV_DIR = PROJECT_ROOT / ".venv"
ENV_FILE = PROJECT_ROOT / ".env"
REQUIREMENTS_FILE = PROJECT_ROOT / "requirements.txt"

DATA_LAKE_DIRECTORIES = (
    "raw/movies",
    "raw/streaming/movies",
    "processed/movies",
    "curated/movies",
    "curated/spark/peliculas_por_genero",
    "streaming",
)


def python_version_is_supported() -> bool:
    """Comprueba el minimo de Python requerido por los pipelines actuales."""
    return sys.version_info >= (3, 10)


def venv_python() -> Path:
    """Devuelve el ejecutable Python del entorno virtual en cualquier plataforma."""
    executable = "python.exe" if os.name == "nt" else "python"
    return VENV_DIR / ("Scripts" if os.name == "nt" else "bin") / executable


def run(command: list[str], description: str) -> None:
    """Ejecuta un comando mostrando solo el contexto, no secretos."""
    print(f"[IAC] {description}...")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def create_virtual_environment() -> Path:
    interpreter = venv_python()
    if interpreter.exists():
        print(f"[IAC] Entorno virtual existente: {VENV_DIR.relative_to(PROJECT_ROOT)}")
        return interpreter

    run([sys.executable, "-m", "venv", str(VENV_DIR)], "Creando entorno virtual")
    return interpreter


def install_dependencies(interpreter: Path) -> None:
    if not REQUIREMENTS_FILE.exists():
        raise FileNotFoundError(f"No existe {REQUIREMENTS_FILE.name} en la raiz del proyecto.")
    run(
        [str(interpreter), "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)],
        "Instalando dependencias",
    )


def create_data_lake_directories() -> None:
    print("[IAC] Creando estructura local del Data Lake...")
    for relative_path in DATA_LAKE_DIRECTORIES:
        (PROJECT_ROOT / "data_lake" / relative_path).mkdir(parents=True, exist_ok=True)
    print(f"[IAC] Zonas preparadas: {len(DATA_LAKE_DIRECTORIES)}")


def read_env_values() -> dict[str, str]:
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            "Falta .env. Crea el archivo y configura MONGO_URI antes del health check."
        )

    values: dict[str, str] = {}
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def validate_environment_file() -> None:
    values = read_env_values()
    uri = values.get("MONGO_URI", "")
    markers = ("<PASSWORD>", "<CLUSTER_HOST>", "CAMBIA_ESTO", "TU_")
    if not uri or not uri.startswith("mongodb+srv://"):
        raise RuntimeError("MONGO_URI debe ser una URI mongodb+srv:// de MongoDB Atlas.")
    if any(marker in uri for marker in markers):
        raise RuntimeError("MONGO_URI todavia contiene marcadores de ejemplo.")
    print("[IAC] .env encontrado y MONGO_URI configurada.")


def run_health_check(interpreter: Path) -> None:
    health_check = (
        "from conexion import conectar_mongodb; "
        "cliente, _ = conectar_mongodb(); cliente.close()"
    )
    run([str(interpreter), "-c", health_check], "Ejecutando health check de Atlas")


def main() -> int:
    try:
        if not python_version_is_supported():
            version = ".".join(str(part) for part in sys.version_info[:3])
            raise RuntimeError(f"Python {version} detectado; se requiere Python 3.10 o superior.")

        print(f"[IAC] Python {sys.version_info.major}.{sys.version_info.minor} compatible.")
        interpreter = create_virtual_environment()
        install_dependencies(interpreter)
        create_data_lake_directories()
        validate_environment_file()
        run_health_check(interpreter)
        print("[IAC] Entorno preparado correctamente.")
        print("[IAC] Para provisionar Atlas como IaC: cd terraform; terraform plan")
        return 0
    except (FileNotFoundError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"[IAC][ERROR] {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())