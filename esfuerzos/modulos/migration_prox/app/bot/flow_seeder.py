"""
Flow Seeder - Crea el FlowTemplate del sistema por defecto con todos los FlowNodes de Phase 2.

Se llama al startup de la aplicación para garantizar que el flujo exista.
Idempotente: seguro llamarlo en cada arranque.

Flujo Phase 2:
  bienvenida → ver_menu → pedido_recibido (instrucciones completas de pago)
             → esperar_comprobante → comprobante_recibido
             → [dashboard] → orden_confirmada | orden_rechazada → esperar_comprobante
  bienvenida → info_negocio (FAQ: horarios, delivery, pagos)
  cualquier nodo → fallback
"""
import json
import logging
from sqlalchemy.orm import Session

from app.models.bot import FlowTemplate, FlowNode

logger = logging.getLogger(__name__)

DEFAULT_NODES = [
    # ------------------------------------------------------------------
    # 1. Bienvenida — punto de entrada
    # ------------------------------------------------------------------
    {
        "node_key": "bienvenida",
        "node_type": "greeting",
        "order_position": 1,
        "message_template": (
            "¡Hola! 👋 Soy el asistente de *{business_name}*.\n\n"
            "¿En qué puedo ayudarte?\n"
            "• Ver menú y hacer un pedido → escribe *menú*\n"
            "• Información del negocio → escribe *info*"
        ),
        "expected_responses": json.dumps(["menu", "info", "pedido", "quiero pedir", "hola", "buenas"]),
        "next_node_map": json.dumps({
            "menu|pedido|quiero pedir": "ver_menu",
            "info": "info_negocio",
        }),
    },

    # ------------------------------------------------------------------
    # 2. Ver menú — envía link de la webapp
    # ------------------------------------------------------------------
    {
        "node_key": "ver_menu",
        "node_type": "menu",
        "order_position": 2,
        "message_template": (
            "Aquí está nuestro menú 🍽️\n"
            "{webapp_link}\n\n"
            "Elige tus platos, arma tu carrito y confírmalo desde la app.\n"
            "Cuando termines, envíame el número de orden para continuar."
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 3. Pedido recibido — resumen de la orden (opciones de pago van en mensajes separados)
    # ------------------------------------------------------------------
    {
        "node_key": "pedido_recibido",
        "node_type": "order_received",
        "order_position": 3,
        "message_template": (
            "¡Recibí tu pedido *#{orden_numero}*! 🎉\n\n"
            "📋 *Tu pedido:*\n{items_list}\n\n"
            "🧾 Subtotal: ${subtotal}\n"
            "{delivery_line}"
            "💰 *Total: ${total}*\n\n"
            "Puedes pagar con cualquiera de estos métodos 👇"
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 4. Esperar comprobante — nodo de espera, se avanza por media
    # ------------------------------------------------------------------
    {
        "node_key": "esperar_comprobante",
        "node_type": "awaiting_proof",
        "order_position": 4,
        "message_template": (
            "Estoy esperando tu comprobante de pago 📎\n"
            "Envíame una captura de pantalla o foto del recibo."
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 5. Comprobante recibido — acuse de recibo, dashboard verifica
    # ------------------------------------------------------------------
    {
        "node_key": "comprobante_recibido",
        "node_type": "proof_received",
        "order_position": 5,
        "message_template": (
            "✅ ¡Comprobante recibido!\n\n"
            "Estamos verificando tu pago. En unos minutos te confirmamos.\n"
            "Gracias por tu pedido en *{business_name}* 🙏"
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 6. Orden confirmada — enviado proactivamente por el dashboard
    # ------------------------------------------------------------------
    {
        "node_key": "orden_confirmada",
        "node_type": "order_confirmed",
        "order_position": 6,
        "message_template": (
            "✅ *¡Pago confirmado!*\n\n"
            "Tu pedido *#{orden_numero}* está en preparación 🍽️\n\n"
            "{aviso_siguiente_paso}"
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 7. Orden en camino — enviado proactivamente por el dashboard
    # ------------------------------------------------------------------
    {
        "node_key": "orden_en_camino",
        "node_type": "order_on_the_way",
        "order_position": 7,
        "message_template": (
            "🛵 *¡Tu pedido #{orden_numero} está en camino!*\n\n"
            "{cliente_referencia_msg}"
            "Estará contigo en breve. ¡Gracias por elegir *{business_name}*!"
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 8. Orden lista para retiro — enviado proactivamente por el dashboard
    # ------------------------------------------------------------------
    {
        "node_key": "orden_lista_retiro",
        "node_type": "order_ready_pickup",
        "order_position": 8,
        "message_template": (
            "✅ *¡Tu pedido #{orden_numero} está listo para retirar!*\n\n"
            "📍 *Dirección:* {direccion_negocio}\n\n"
            "Te esperamos. ¡Gracias por elegir *{business_name}*!"
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 9. Orden rechazada — enviado proactivamente por el dashboard
    # ------------------------------------------------------------------
    {
        "node_key": "orden_rechazada",
        "node_type": "order_rejected",
        "order_position": 9,
        "message_template": (
            "⚠️ No pudimos verificar tu comprobante de pago.\n\n"
            "Puede ser que la imagen no sea legible o los datos no coincidan.\n"
            "Por favor envíame un nuevo comprobante para continuar."
        ),
        "expected_responses": json.dumps(["ok", "entiendo", "voy", "te mando", "aqui va"]),
        "next_node_map": json.dumps({"default": "esperar_comprobante"}),
    },

    # ------------------------------------------------------------------
    # 10. Info negocio — FAQ: horarios, delivery, pagos
    # ------------------------------------------------------------------
    {
        "node_key": "info_negocio",
        "node_type": "faq",
        "order_position": 10,
        "message_template": (
            "ℹ️ Información de *{business_name}*\n\n"
            "🕐 *Horario*: {working_hours}\n"
            "📍 *Cobertura*: {area_cobertura}\n"
            "💳 *Métodos de pago*: {payment_methods}\n\n"
            "¿Quieres ver el menú? Escribe *menú* 😊"
        ),
        "expected_responses": json.dumps(["menu", "pedido", "si", "quiero pedir"]),
        "next_node_map": json.dumps({"menu|pedido|si|quiero pedir": "ver_menu"}),
    },

    # ------------------------------------------------------------------
    # 11. Fallback — no entendió el mensaje
    # ------------------------------------------------------------------
    {
        "node_key": "fallback",
        "node_type": "fallback",
        "order_position": 11,
        "message_template": (
            "No entendí bien tu mensaje 😅\n"
            "Puedes escribir *menú* para ver nuestros platos,\n"
            "o *info* para horarios y delivery."
        ),
        "expected_responses": None,
        "next_node_map": None,
    },

    # ------------------------------------------------------------------
    # 12. Escalado a humano — problema técnico o solicitud humana
    # ------------------------------------------------------------------
    {
        "node_key": "escalado_humano",
        "node_type": "human_escalation",
        "order_position": 12,
        "message_template": (
            "Entiendo que estás teniendo problemas con el pago. 🙏\n\n"
            "Voy a conectarte con uno de nuestros agentes para que te ayuden directamente.\n"
            "Por favor espera un momento."
        ),
        "expected_responses": None,
        "next_node_map": None,
    },
]


def seed_default_flow(db: Session) -> FlowTemplate:
    """
    Garantiza que el FlowTemplate del sistema exista con todos los nodos requeridos.
    Crea el template y sus nodos si no existen. Si ya existen, no hace nada.
    """
    template = db.query(FlowTemplate).filter(FlowTemplate.is_system_default == True).first()

    if not template:
        template = FlowTemplate(
            name="Flujo Gastronómico Phase 2",
            description="Flujo conversacional completo: consulta → pedido → pago → comprobante.",
            is_system_default=True,
        )
        db.add(template)
        db.flush()
        logger.info(f"FlowSeeder: FlowTemplate por defecto creado (id={template.id})")

    created_count = 0
    for node_data in DEFAULT_NODES:
        existing = db.query(FlowNode).filter(
            FlowNode.flow_template_id == template.id,
            FlowNode.node_key == node_data["node_key"],
        ).first()

        if not existing:
            node = FlowNode(
                flow_template_id=template.id,
                node_type=node_data["node_type"],
                node_key=node_data["node_key"],
                order_position=node_data["order_position"],
                message_template=node_data.get("message_template"),
                expected_responses=node_data.get("expected_responses"),
                next_node_map=node_data.get("next_node_map"),
            )
            db.add(node)
            created_count += 1
        else:
            # Sincronizar todos los campos configurables del nodo
            existing.message_template = node_data.get("message_template")
            existing.expected_responses = node_data.get("expected_responses")
            existing.next_node_map = node_data.get("next_node_map")

    if created_count > 0:
        logger.info(f"FlowSeeder: {created_count} nodo(s) creado(s) en template id={template.id}")

    db.commit()
    return template
