"""Ingesta CDC: MongoDB Change Streams -> zona RAW del Data Lake.

Deja el proceso ejecutándose mientras se insertan, actualizan o eliminan
documentos en la colección movies. Se detiene de forma segura con Ctrl+C.

Ejecutar desde la raíz del proyecto:
    python scripts/ingesta_streaming.py
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

from bson import json_util
from pymongo.errors import PyMongoError

sys.path.append(str(Path(__file__).resolve().parents[1]))
from conexion import conectar_mongodb


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
RAIZ_RAW = RAIZ_PROYECTO / "data_lake" / "raw" / "streaming" / "movies"
ARCHIVO_TOKEN = RAIZ_RAW / "resume_token.json"


def cargar_token():
    """Recupera el ultimo token para continuar tras reiniciar el consumidor."""
    if not ARCHIVO_TOKEN.exists():
        return None
    return json_util.loads(ARCHIVO_TOKEN.read_text(encoding="utf-8"))


def guardar_token(token) -> None:
    ARCHIVO_TOKEN.parent.mkdir(parents=True, exist_ok=True)
    ARCHIVO_TOKEN.write_text(json_util.dumps(token), encoding="utf-8")


def ruta_eventos() -> Path:
    fecha = datetime.now(timezone.utc).date().isoformat()
    destino = RAIZ_RAW / f"fecha_carga={fecha}"
    destino.mkdir(parents=True, exist_ok=True)
    return destino / "eventos.jsonl"


def ejecutar_ingesta() -> None:
    """Escucha los cambios de movies y persiste cada evento CDC en formato JSONL."""
    cliente, db = conectar_mongodb()
    archivo_eventos = ruta_eventos()
    token = cargar_token()
    opciones = {"full_document": "updateLookup"}
    if token:
        opciones["resume_after"] = token

    try:
        print("[STREAMING] Escuchando cambios en sample_mflix.movies...")
        print(f"[STREAMING] Eventos RAW: {archivo_eventos.relative_to(RAIZ_PROYECTO)}")
        print("[STREAMING] Presiona Ctrl+C para detener la ingesta.")
        with db.movies.watch(**opciones) as stream, archivo_eventos.open("a", encoding="utf-8") as salida:
            for cambio in stream:
                evento = {
                    "fecha_ingesta_utc": datetime.now(timezone.utc),
                    "evento_cdc": cambio,
                }
                salida.write(json_util.dumps(evento))
                salida.write("\n")
                salida.flush()
                guardar_token(cambio["_id"])
                print(f"[STREAMING] Evento recibido: {cambio['operationType']}")
    except KeyboardInterrupt:
        print("\n[STREAMING] Ingesta detenida por el usuario.")
    except PyMongoError as error:
        raise RuntimeError(f"Error en MongoDB Change Streams: {error}") from error
    finally:
        cliente.close()
        print("[STREAMING] Conexion cerrada.")


if __name__ == "__main__":
    ejecutar_ingesta()
