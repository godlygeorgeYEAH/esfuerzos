"""
Flow Seeder — Crea el FlowTemplate de intake de crisis Reúne v1.

Flujo principal:
  bienvenida → guia_familiar → pedir_foto → notas_adicionales → reporte_guardado
  bienvenida → guia_rescatista  (placeholder)
  bienvenida → guia_hospital    (placeholder)

Navegación: exclusivamente por next_node_map.
  - Claves exactas (ej. "1", "listo", "reporte") para keywords.
  - "default" para cualquier texto libre.
  - Sin expected_responses — el texto del usuario se almacena en contexto.
"""
import json
import logging
from sqlalchemy.orm import Session

from app.models.bot import FlowTemplate, FlowNode

logger = logging.getLogger(__name__)

DEFAULT_NODES = [
    # ------------------------------------------------------------------
    # 1. Bienvenida — identifica el tipo de usuario
    # ------------------------------------------------------------------
    {
        "node_key": "bienvenida",
        "node_type": "greeting",
        "order_position": 1,
        "message_template": (
            "Hola, soy *Reúne* 🤝\n\n"
            "Estoy aquí para ayudarte a conectar personas tras el sismo.\n\n"
            "¿Cuál es tu perfil?\n\n"
            "*1* — Soy familiar de un desaparecido\n"
            "*2* — Soy rescatista\n"
            "*3* — Soy hospital o refugio"
        ),
        "expected_responses": None,
        "next_node_map": json.dumps({
            "1": "guia_familiar",
            "2": "guia_rescatista",
            "3": "guia_hospital",
        }),
    },

    # ------------------------------------------------------------------
    # 2. Guía familiar — instrucciones + pide datos en un solo mensaje
    # ------------------------------------------------------------------
    {
        "node_key": "guia_familiar",
        "node_type": "intake_guide",
        "order_position": 2,
        "message_template": (
            "Vamos a registrar el reporte en 3 pasos.\n\n"
            "Para el primer paso, envíame *un solo mensaje* con esta información:\n\n"
            "› El nombre completo de a quien estás reportando\n\n"
            "› El género de la persona que estás reportando\n\n"
            "› La edad de la persona que estás reportando\n\n"
            "› Última ubicación conocida\n\n"
            "_Ejemplo: María García, femenino, 34, Cumaná centro_"
        ),
        "expected_responses": None,
        "next_node_map": json.dumps({"default": "pedir_foto"}),
    },

    # ------------------------------------------------------------------
    # 3. Pedir foto — espera fotos; avanza con "listo"
    # ------------------------------------------------------------------
    {
        "node_key": "pedir_foto",
        "node_type": "intake_photo",
        "order_position": 3,
        "message_template": (
            "✅ Datos recibidos.\n\n"
            "Ahora envía *una o varias fotos* de la persona.\n"
            "Cuando termines, escribe *listo*."
        ),
        "expected_responses": None,
        "next_node_map": json.dumps({"default": "pedir_foto"}),
    },

    # ------------------------------------------------------------------
    # 4. Notas adicionales — señas, ropa; "reporte" inicia uno nuevo
    # ------------------------------------------------------------------
    {
        "node_key": "notas_adicionales",
        "node_type": "intake_notes",
        "order_position": 4,
        "message_template": (
            "📸 Imágenes recibidas.\n\n"
            "¿Tienes señas, ropa u otros detalles? Escríbelos ahora.\n\n"
            "O escribe *reporte* para registrar un nuevo caso."
        ),
        "expected_responses": None,
        "next_node_map": json.dumps({
            "reporte": "guia_familiar",
            "default": "reporte_guardado",
        }),
    },

    # ------------------------------------------------------------------
    # 5. Reporte guardado — confirmación final
    # ------------------------------------------------------------------
    {
        "node_key": "reporte_guardado",
        "node_type": "intake_saved",
        "order_position": 5,
        "message_template": (
            "✅ *Reporte registrado.*\n\n"
            "Nuestro equipo lo revisará. No confirmaremos coincidencias "
            "sin verificación humana previa.\n\n"
            "Escribe *reporte* en cualquier momento para registrar otro caso."
        ),
        "expected_responses": None,
        "next_node_map": json.dumps({
            "reporte": "guia_familiar",
            "1": "guia_familiar",
            "2": "guia_rescatista",
            "3": "guia_hospital",
            "default": "bienvenida",
        }),
    },

    # ------------------------------------------------------------------
    # 6. Guía rescatista — placeholder
    # ------------------------------------------------------------------
    {
        "node_key": "guia_rescatista",
        "node_type": "placeholder",
        "order_position": 6,
        "message_template": (
            "Gracias por tu apoyo 🙏\n\n"
            "El flujo para rescatistas estará disponible muy pronto.\n"
            "Un coordinador se pondrá en contacto contigo."
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 7. Guía hospital — placeholder
    # ------------------------------------------------------------------
    {
        "node_key": "guia_hospital",
        "node_type": "placeholder",
        "order_position": 7,
        "message_template": (
            "Gracias por contactarnos 🏥\n\n"
            "El flujo para hospitales y refugios estará disponible muy pronto.\n"
            "Un coordinador se pondrá en contacto contigo."
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 8. Fallback — no entendió; retoma con 1/2/3
    # ------------------------------------------------------------------
    {
        "node_key": "fallback",
        "node_type": "fallback",
        "order_position": 8,
        "message_template": (
            "No entendí tu mensaje.\n\n"
            "Escribe el número de tu perfil:\n"
            "*1* — Familiar de un desaparecido\n"
            "*2* — Rescatista\n"
            "*3* — Hospital o refugio"
        ),
        "expected_responses": None,
        "next_node_map": json.dumps({
            "1": "guia_familiar",
            "2": "guia_rescatista",
            "3": "guia_hospital",
        }),
    },
]


def seed_default_flow(db: Session) -> FlowTemplate:
    """
    Garantiza que el FlowTemplate de crisis exista con todos sus nodos.
    Crea o actualiza en cada arranque. Idempotente.
    """
    template = db.query(FlowTemplate).filter(FlowTemplate.is_system_default == True).first()

    if not template:
        template = FlowTemplate(
            name="Flujo de Intake de Crisis — Reúne v1",
            description="Identifica al usuario y recopila datos del desaparecido o encontrado.",
            is_system_default=True,
        )
        db.add(template)
        db.flush()
        logger.info("FlowSeeder: FlowTemplate de crisis creado (id=%d)", template.id)

    created_count = 0
    for node_data in DEFAULT_NODES:
        existing = db.query(FlowNode).filter(
            FlowNode.flow_template_id == template.id,
            FlowNode.node_key == node_data["node_key"],
        ).first()

        if not existing:
            db.add(FlowNode(
                flow_template_id=template.id,
                node_type=node_data["node_type"],
                node_key=node_data["node_key"],
                order_position=node_data["order_position"],
                message_template=node_data.get("message_template"),
                expected_responses=node_data.get("expected_responses"),
                next_node_map=node_data.get("next_node_map"),
            ))
            created_count += 1
        else:
            existing.message_template = node_data.get("message_template")
            existing.expected_responses = node_data.get("expected_responses")
            existing.next_node_map = node_data.get("next_node_map")

    if created_count > 0:
        logger.info("FlowSeeder: %d nodo(s) creado(s) en template id=%d", created_count, template.id)

    db.commit()
    return template
