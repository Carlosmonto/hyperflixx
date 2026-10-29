"""Almacenamiento Amazon S3 para el Data Lake batch."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()


class DataLakeStorage:
    """Expone operaciones S3 comunes para el pipeline batch del Data Lake."""

    def __init__(self, uri: str | None = None) -> None:
        self.backend = "s3"
        self.uri = uri or os.getenv("DATA_LAKE_URI", "")
        parsed = urlparse(self.uri)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(
                "DATA_LAKE_URI es obligatorio y debe tener el formato s3://bucket/prefijo."
            )
        self.bucket = parsed.netloc
        self.prefix = parsed.path.strip("/")
        self.client = self._s3_client()

    def _s3_client(self):
        import boto3
        from botocore.exceptions import ProfileNotFound

        profile = os.getenv("AWS_PROFILE") or None
        try:
            session = boto3.Session(profile_name=profile)
        except ProfileNotFound as error:
            raise RuntimeError(
                f"El perfil AWS '{profile}' no existe. Ejecuta 'aws configure --profile {profile}'."
            ) from error
        if session.get_credentials() is None:
            raise RuntimeError(
                "No hay credenciales AWS. Configura 'aws configure' o define AWS_PROFILE "
                "antes de ejecutar el pipeline."
            )
        return session.client("s3", region_name=os.getenv("AWS_REGION") or None)

    def _key(self, relative_key: str) -> str:
        relative_key = relative_key.strip("/")
        return "/".join(part for part in (self.prefix, relative_key) if part)

    def uri_for(self, relative_key: str) -> str:
        return f"s3://{self.bucket}/{self._key(relative_key)}"

    def write_lines(self, relative_key: str, lines) -> None:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as temporal:
            temporal_path = Path(temporal.name)
            for line in lines:
                temporal.write(line)
                temporal.write("\n")
        try:
            self.client.upload_file(str(temporal_path), self.bucket, self._key(relative_key))
        finally:
            temporal_path.unlink(missing_ok=True)

    def iter_lines(self, relative_key: str):
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(relative_key))
        for line in response["Body"].iter_lines():
            if line:
                yield line.decode("utf-8") + "\n"

    def write_parquet(self, dataframe, relative_key: str) -> None:
        from io import BytesIO

        buffer = BytesIO()
        dataframe.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
        buffer.seek(0)
        self.client.upload_fileobj(buffer, self.bucket, self._key(relative_key))

    def upload_file(self, local_path: str, relative_key: str) -> None:
        """Publica un archivo local grande sin cargarlo completo en memoria."""
        self.client.upload_file(local_path, self.bucket, self._key(relative_key))