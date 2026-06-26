"""
Servicio de almacenamiento de archivos.

LocalStorage (dev): guarda en /app/media/ (volumen Docker mapeado a ./media/ local).
S3Storage (prod): sube a DigitalOcean Spaces u otro bucket S3-compatible.

Selección controlada por settings.storage_backend ("local" | "s3").
Las funciones públicas download_and_save y save_uploaded_file mantienen sus firmas
originales; los callers no necesitan cambios.
"""
import logging
import re
import uuid
from abc import ABC, abstractmethod
from pathlib import Path

import httpx
from fastapi import UploadFile

from app.config import get_settings

logger = logging.getLogger(__name__)


class StorageBackend(ABC):
    @abstractmethod
    async def save_bytes(self, data: bytes, key: str) -> str:
        """Persiste data bajo key y retorna su URL pública."""

    async def save_upload(self, file: UploadFile, key: str) -> str:
        content = await file.read()
        return await self.save_bytes(content, key)


class LocalStorage(StorageBackend):
    BASE_DIR = Path("/app/media")

    async def save_bytes(self, data: bytes, key: str) -> str:
        dest = self.BASE_DIR / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        logger.info("storage[local]: guardado en %s (%d bytes)", dest, len(data))
        return f"/media/{key}"


class S3Storage(StorageBackend):
    async def save_bytes(self, data: bytes, key: str) -> str:
        import aioboto3
        s = get_settings()
        session = aioboto3.Session(
            aws_access_key_id=s.s3_access_key,
            aws_secret_access_key=s.s3_secret_key,
        )
        async with session.client("s3", endpoint_url=s.s3_endpoint_url) as s3:
            await s3.put_object(
                Bucket=s.s3_bucket_name,
                Key=key,
                Body=data,
                ACL="public-read",
            )
        url = f"{s.s3_public_base_url.rstrip('/')}/{key}"
        logger.info("storage[s3]: subido a %s", url)
        return url


_storage_instance: StorageBackend | None = None


def get_storage() -> StorageBackend:
    """Singleton por proceso — se crea al primer uso."""
    global _storage_instance
    if _storage_instance is None:
        backend = get_settings().storage_backend
        _storage_instance = S3Storage() if backend == "s3" else LocalStorage()
    return _storage_instance


def _normalize_waha_url(url: str) -> str:
    """
    WAHA incluye su propia URL en los webhooks usando el hostname con el que
    arrancó (puede ser localhost:3000). Dentro de Docker el servicio se llama
    'waha', así que reemplazamos el origen con settings.waha_url.
    """
    return re.sub(r'^https?://[^/]+', get_settings().waha_url.rstrip('/'), url)


async def download_and_save(media_url: str) -> str:
    """
    Descarga el archivo desde WAHA y lo persiste en el storage configurado.

    Retorna URL relativa (/media/...) con LocalStorage,
    URL absoluta (https://...) con S3Storage.
    """
    internal_url = _normalize_waha_url(media_url)
    logger.info("storage: descargando comprobante desde %s", internal_url)

    s = get_settings()
    headers = {}
    if s.waha_api_key:
        headers["X-Api-Key"] = s.waha_api_key

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(internal_url, headers=headers, follow_redirects=True)
        response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if "jpeg" in content_type or "jpg" in content_type:
        ext = ".jpg"
    elif "png" in content_type:
        ext = ".png"
    elif "pdf" in content_type:
        ext = ".pdf"
    else:
        suffix = Path(media_url.split("?")[0]).suffix
        ext = suffix if suffix else ".bin"

    key = f"comprobantes/{uuid.uuid4().hex}{ext}"
    return await get_storage().save_bytes(response.content, key)


async def save_uploaded_file(file: UploadFile, subfolder: str) -> str:
    """
    Guarda un archivo subido vía multipart/form-data.

    Retorna URL relativa o absoluta según el storage configurado.
    """
    ext = Path(file.filename).suffix if file.filename else ".jpg"
    if not ext:
        ext = ".jpg"
    key = f"imagenes/{subfolder}/{uuid.uuid4().hex}{ext}"
    return await get_storage().save_upload(file, key)
