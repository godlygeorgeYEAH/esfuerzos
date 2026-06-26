from sqlalchemy.orm import Session
from app.models.cliente import Cliente


def get_or_create_cliente(
    db: Session,
    negocio_id: int,
    telefono: str,
    nombre: str | None = None,
) -> Cliente:
    """
    Retorna el Cliente existente para (negocio_id, telefono) o lo crea si no existe.

    - Normaliza el teléfono a E.164 sin '+' antes de buscar/insertar.
    - Si el cliente ya existe y no tiene nombre, actualiza con el nombre recibido.
    - Idempotente: múltiples llamadas con los mismos datos no generan duplicados.
    """
    telefono = telefono.lstrip("+").strip()

    cliente = db.query(Cliente).filter(
        Cliente.negocio_id == negocio_id,
        Cliente.telefono == telefono,
    ).first()

    if not cliente:
        cliente = Cliente(negocio_id=negocio_id, telefono=telefono, nombre=nombre)
        db.add(cliente)
        db.flush()
    elif nombre and not cliente.nombre:
        cliente.nombre = nombre

    return cliente
