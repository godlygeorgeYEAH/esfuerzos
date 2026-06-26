Plan de acción — Capa WhatsApp (WAHA)
Qué construimos
El PRD requiere dos flujos de intake por WhatsApp y una notificación de salida. Con WAHA en lugar de Cloud API, el equivalente es:

Requisito PRD	Equivalente WAHA
Verificación hub.challenge	No aplica — WAHA pushea a nuestro webhook sin handshake
WhatsApp Flows (formulario estructurado)	Conversación guiada por nodos (ProX FlowEngine)
Descarga de media por Graph API	URL de media directa del payload WAHA
Plantilla Utility aprobada	Mensaje de texto libre (WAHA no tiene restricción de 24h)
reporter_wa_hash	Hash SHA-256 del from antes de persistir
Paso 1 — Limpieza del módulo ProX
Eliminar los archivos y bloques ya identificados. Lo que desaparece:

Archivos completos:

app/models/cliente.py, conductor.py, orden.py, notificacion.py
app/core/clientes.py, conductores.py, notificaciones.py, phone.py
app/bot/abc_layer.py
app/services/storage.py
Bloques dentro de archivos:

flow_engine.py → import Articulo + nodo service_list + método _build_payment_method_messages + todos los branches de commerce en _generate_response
webhook.py → bloque de conductor (líneas 106-121) + get_or_create_cliente (123-125)
orchestrator.py → _handle_comprobante, _handle_texto_en_espera_comprobante, _persist_flow_entities, Paso 12 (ABC), bloque crear_notificacion
models/negocio.py → campos de pago, delivery y GPS
config.py → vars de storage S3, media, webapp
Paso 2 — Modelo de datos de intake (SQLAlchemy, sin vectores)
Nuevo archivo app/models/reporte.py. Solo los campos que el bot recolecta vía WhatsApp — sin embeddings, sin pgvector (eso es territorio Supabase, fuera del alcance de esta sesión):

reports: id, kind (missing|found), full_name, age,
         last_seen_location, distinguishing_marks,
         clothing, reporter_wa_hash, consent, source, created_at
photos:  id, report_id, media_url, created_at
Paso 3 — Dos flujos conversacionales (flow_seeder reescrito)
Reemplaza completamente el seeder de food delivery con los dos flujos del PRD §7.1:

Flujo A — Desaparecido (familia reporta):

bienvenida → tipo_reporte
tipo_reporte → nombre_desaparecido
nombre_desaparecido → edad_desaparecido
edad_desaparecido → ubicacion_desaparecido
ubicacion_desaparecido → señas_desaparecido
señas_desaparecido → ropa_desaparecido
ropa_desaparecido → foto_desaparecido
foto_desaparecido → confirmacion_reporte → reporte_guardado
Flujo B — Encontrado (rescatista, hospital, refugio):

bienvenida → tipo_reporte
tipo_reporte → nombre_encontrado
nombre_encontrado → edad_encontrado
edad_encontrado → ubicacion_encontrado
ubicacion_encontrado → señas_encontrado
señas_encontrado → ropa_encontrado
ropa_encontrado → foto_encontrado
foto_encontrado → confirmacion_encontrado → encontrado_guardado
El nodo tipo_reporte bifurca el flujo según si el usuario responde "desaparecido" o "rescatista". El Orchestrator guía nodo a nodo, acumulando campos en el contexto de la conversación.

Paso 4 — Gestión de sesión y fotos múltiples
El PRD §7.1 indica que las fotos llegan como mensajes separados y deben agruparse por sesión. En WAHA los mensajes llegan en burst — implementar en el Orchestrator una ventana de agrupación:

Cuando el nodo actual es foto_* y llega un media_url, guardar en photos sin avanzar el nodo
Avanzar solo cuando llegue texto ("listo", "es todo") o tras un TTL configurado en contexto
Máximo configurable de fotos por reporte
Paso 5 — Intake handler
Nuevo app/core/intake.py. Al llegar a reporte_guardado o encontrado_guardado:

Leer todos los campos acumulados del contexto de conversación
Hashear el client_phone (SHA-256) → reporter_wa_hash
Escribir el registro en reports
Vincular las fotos en photos
Limpiar el contexto de conversación
Paso 6 — Notificación de salida
El PRD §7.5: cuando un humano confirma un match, el sistema envía un mensaje a la familia. El trigger vendrá de la consola (fuera de alcance ahora), pero el emisor vive aquí.

Agregar en app/services/waha.py la función send_match_notification(wa_hash, nombre_reportado) que envía:

"Tenemos una posible coincidencia con tu reporte de {nombre}. Un voluntario la verificará y te contactará pronto."

El wa_hash se usa para recuperar el waha_chat_id de la conversación — el número nunca se expone fuera del sistema.

Paso 7 — BotConfig y Negocio seeders
Sin BotConfig en DB el bot responde en silencio. Crear un script scripts/seed_crisis_bot.py que inserta:

Un registro Negocio (nombre: "Reúne", waha_session: "default")
Un BotConfig con is_bot_active=True, sin horario restrictivo (24/7, emergencia)
El NegocioFlow apuntando al template de crisis
Orden de ejecución
Paso 1 — Limpieza (desbloquea que la app arranque)
Paso 2 — app/models/reporte.py
Paso 7 — Seeder (sin esto el bot es mudo)
Paso 3 — flow_seeder.py reescrito
Paso 4 — Lógica de fotos múltiples en Orchestrator
Paso 5 — app/core/intake.py
Paso 6 — send_match_notification en waha.py
