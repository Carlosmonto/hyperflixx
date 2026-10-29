"""Conexion centralizada a la base de datos sample_mflix."""

import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
NOMBRE_BD = os.getenv("MONGO_DB", "sample_mflix")


def conectar_mongodb(read_preference: str | None = None, validar: bool = True):
    """Valida Atlas y devuelve el cliente junto con sample_mflix."""
    if not MONGO_URI:
        raise RuntimeError("Falta MONGO_URI en el archivo .env")
    try:
        print("\n[CONEXION] Intentando conectar a MongoDB Atlas...")
        opciones = {
            "tlsCAFile": certifi.where(),
            "connectTimeoutMS": 20000,
            "serverSelectionTimeoutMS": 30000,
        }
        if read_preference:
            opciones["readPreference"] = read_preference
        cliente = MongoClient(
            MONGO_URI,
            **opciones,
        )
        if validar:
            cliente.admin.command("ping")
            print(f"[CONEXION] Conexion exitosa. Base seleccionada: {NOMBRE_BD}")
        else:
            print(f"[CONEXION] Cliente creado. Validacion diferida para lectura: {NOMBRE_BD}")
        return cliente, cliente[NOMBRE_BD]
    except ConnectionFailure as error:
        raise RuntimeError(f"No se pudo contactar Atlas: {error}") from error
    except ConfigurationError as error:
        raise RuntimeError(f"URI de MongoDB invalida: {error}") from error
    except OperationFailure as error:
        raise RuntimeError(f"Autenticacion o permisos rechazados: {error}") from error


if __name__ == "__main__":
    cliente, _ = conectar_mongodb()
    cliente.close()
    print("[CONEXION] Conexion cerrada.")
