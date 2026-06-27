---
# Guía para contribuidores - Reune VE

Bienvenido al repo. Este documento está escrito para ti, el segundo contribuidor, que llegó con ProX ya construido y funcional. El objetivo es claro: integrar tu módulo al pipeline principal sin romper lo que ya funciona en ninguno de los dos lados.

Antes de tocar cualquier cosa, lee esto completo. Hay decisiones arquitectónicas en el API principal que afectan directamente cómo tienes que conectar tu módulo.

---

## Estructura del repo

```
reune-ve/
|-- api/                         # API principal (Reune: matching facial, scrapers, WAHA)
|   |-- main.py                  # Punto de entrada del API principal (FastAPI)
|   |-- config.py                # Settings via pydantic-settings, todas las vars de entorno
|   |-- face_pipeline.py         # Pipeline de matching facial con InsightFace
|   |-- scraper_orchestrator.py  # Orquestador de scrapers con APScheduler
|   |-- base44_webhook_router.py # Router Base44 Superagent
|   |-- base44_poller.py         # Polling loop para Base44
|   |-- bot/                     # Intake de WhatsApp (maquina de estados determinista, usa httpx solo para WAHA)
|   |   |-- flows.py
|   |   |-- webhook_router.py
|   |-- scrapers/                # Fuentes externas (SOS Venezuela, Reconexion, etc.)
|
|-- app/                         # Segunda app FastAPI (vision / reportes de daños)
|   |-- main.py                  # Punto de entrada: app.main:app (el que corre el Dockerfile)
|   |-- vision.py
|   |-- routes/
|       |-- transcribe.py
|       |-- ...
|
|-- migrations/                  # Migraciones Supabase (001-004 activas)
|-- Dockerfile                   # Construye y lanza app.main:app (ver nota de drift abajo)
|-- docker-compose.yml           # Contenedor crisis-ve, puerto 8081, mem_limit 256m
|-- requirements.txt
|
|-- esfuerzos/                   # Por crear: módulos de contribuidores, fuera del core
    |-- modulos/
        |-- migration_prox/      # Tu módulo (ProX, adaptado de foob_v2)
            |-- ...
```

> **Nota de drift importante:** el `Dockerfile` y `docker-compose.yml` actuales despliegan `app.main:app`, que es la app de visión/daños en `app/`. No existe todavía un artefacto de despliegue (Dockerfile / compose) que levante el sistema Reune en `api/`. Esta guía describe el estado objetivo; el mantenedor del core tiene que crear ese artefacto antes de que la integración descrita aquí sea operable en producción.

El contenedor activo en el VPS es `crisis-ve` (puerto 8081 en el host, 8081 interno). Tu módulo vive bajo `esfuerzos/` y se despliega como un contenedor separado en la misma red Docker. No modifica el core directamente: se integra via HTTP interno y variables de entorno compartidas.

---

## Integración requerida

Hay cinco puntos de integración que tienes que resolver antes de que tu módulo pueda correr en producción junto al API principal. Los detallo uno por uno.

### 1. Migrar DeepSeek a Groq

El servicio de visión (`app/`) usa Groq con `meta-llama/llama-4-scout-17b-16e-instruct` via el SDK oficial de Groq (ver `app/vision.py`, `app/routes/transcribe.py`). Tu módulo usa DeepSeek (`deepseek-chat` via `api.deepseek.com`). Para compartir una sola API key de LLM y evitar gestionar dos providers distintos en el VPS, tienes que migrar a Groq.

El cambio es mínimo porque Groq expone un endpoint OpenAI-compatible que acepta la misma forma de petición que DeepSeek.

**Variables de entorno a cambiar en tu `.env`:**

```
# Antes (DeepSeek)
LLM_API_KEY=sk-...deepseek...
LLM_BASE_URL=https://api.deepseek.com
LLM_MODEL=deepseek-chat

# Después (Groq)
LLM_API_KEY=gsk_...groq...
LLM_BASE_URL=https://api.groq.com/openai/v1
LLM_MODEL=meta-llama/llama-4-scout-17b-16e-instruct
```

**Opción con httpx (cliente genérico OpenAI-compatible, sirve si prefieres no agregar el SDK de Groq):**

```python
import httpx

async def call_llm(messages: list[dict], api_key: str, base_url: str, model: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": messages,
                "temperature": 0.3,
                "max_tokens": 400,
                "response_format": {"type": "json_object"},
            },
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
```

Si tu módulo actualmente instancia un cliente OpenAI SDK apuntando a `api.deepseek.com`, cambia solo `base_url`, `api_key` y `model`. El resto del código no cambia.

### 2. Compartir la tabla `reports`

El API principal tiene una tabla `reports` en Supabase definida en `002_match_functions.sql` (crea la tabla y las funciones de matching) y `002_unique_constraint.sql` (agrega `source_url` y el constraint UNIQUE). **No crees una tabla propia de reportes en tu módulo.** Todos los casos de personas desaparecidas/encontradas deben vivir en `reports` para que el motor de matching facial y semántico funcione sobre ellos.

**Mapeo de columnas: tu esquema a `reports`:**

| Tu modelo (ProX / foob_v2) | Columna en `reports` | Notas |
|---|---|---|
| Nombre completo del caso | `full_name` | Texto libre |
| Tipo de reporte (desaparecido / encontrado) | `kind` | Valor: `"missing"` o `"found"` |
| Fuente del reporte | `source` | Usa un valor fijo, p. ej. `"prox_waha"` |
| URL del caso original (si aplica) | `source_url` | Par (`source`, `source_url`) es UNIQUE: permite upsert idempotente |
| Edad | `age` | Entero o null |
| Última ubicación conocida | `last_seen_location` | Texto libre |
| Embedding semántico del texto | `text_embedding` | Vector 768-dim; el API principal lo genera; ver nota abajo |
| Cluster de deduplicación | `dedup_group_id` | Null en creación; el sistema lo asigna |

**Nota sobre `text_embedding`:** el API principal carga `paraphrase-multilingual-mpnet-base-v2` en `app.state.text_model` y genera embeddings de 768 dimensiones. Tu módulo no tiene acceso directo a ese estado. Tienes dos opciones: (1) llamar al endpoint HTTP interno del API principal para crear el reporte y dejar que el principal genere el embedding, o (2) cargar el mismo modelo en tu contenedor, lo que aumenta RAM considerablemente. La opción 1 es la correcta.

> **Nota sobre RAM:** el `docker-compose.yml` del core establece `mem_limit: 256m` (valor verificado). Sin embargo, InsightFace `buffalo_sc` más `paraphrase-multilingual-mpnet-base-v2` difícilmente caben en ese límite. Hay un conflicto no resuelto entre el artefacto de despliegue y los requisitos reales del sistema. Confirma con el mantenedor del core cuál es el límite real operativo antes de dimensionar tu propio contenedor.

Para upsert a `reports` via Supabase REST directamente (si prefieres no pasar por el API principal):

```python
import httpx

async def upsert_report(data: dict, supabase_url: str, service_key: str) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{supabase_url}/rest/v1/reports",
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=data,
        )
        response.raise_for_status()
        return response.json()[0]
```

Incluye `source` y `source_url` en `data` para que el upsert sea idempotente.

### 3. Trigger del face pipeline cuando llega una foto

Cuando tu bot recibe una imagen via WAHA, no tienes que reimplementar el pipeline facial. El API principal expone el pipeline internamente a través de `process_photo_for_report`. El camino correcto desde tu módulo es:

1. Guardar el reporte en `reports` (ver punto 2).
2. Insertar la foto en la tabla `photos` con el `report_id` obtenido.
3. Llamar al API principal por HTTP interno para que procese el matching.

El API principal no expone aún un endpoint público dedicado para esto. La solución temporal es hacer una llamada POST interna al endpoint de admin:

```python
import httpx

async def trigger_photo_processing(report_id: str, media_url: str, admin_key: str) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://crisis-ve:8081/admin/process_photo",  # endpoint a crear en el core
            headers={"Authorization": f"Bearer {admin_key}"},
            json={"report_id": report_id, "media_url": media_url},
            timeout=60.0,
        )
        response.raise_for_status()
        return response.json()
```

Abre un issue en el repo para que el mantenedor del core cree `/admin/process_photo`. Es un wrapper de una línea sobre `process_photo_for_report` ya existente. Mientras tanto, si quieres integrar en paralelo, puedes insertar la fila en `photos` directamente via Supabase REST y dejar que el scheduler del API principal la procese en el siguiente ciclo (no es tiempo real, pero es seguro).

### 4. Red Docker

El core no declara una sección `networks:` explícita en su `docker-compose.yml`; Compose crea automáticamente una red con nombre `<proyecto>_default`. El nombre exacto depende del nombre de proyecto que Compose usa en el droplet (por defecto, el nombre del directorio donde vive el compose). Verifica el nombre real antes de intentar unirte:

```bash
ssh root@134.122.54.197 "docker network ls | grep crisis"
```

Una vez que tengas el nombre exacto (por ejemplo `reune-ve_default`), configura tu `docker-compose.yml`:

```yaml
services:
  prox-api:
    container_name: prox-ve-api
    restart: unless-stopped
    ports:
      - "8082:8080"          # elige un puerto libre en el host
    env_file: .env
    networks:
      - reune_main

networks:
  reune_main:
    external: true
    name: reune-ve_default   # reemplaza con el nombre verificado arriba
```

Con esto, desde tu contenedor puedes hacer `http://crisis-ve:8081/...` y funciona.

Si el nombre de red no existe todavía (el core aún no está levantado en ese entorno), créala manualmente antes de levantar tu contenedor:

```bash
docker network create reune-ve_default
```

### 5. Coordinación WAHA

El API principal registra el webhook de WAHA en `POST /webhook/waha` (definido en `api/bot/webhook_router.py`) y es el dueño de esa ruta. Si tu módulo también intenta recibir eventos WAHA, hay dos opciones:

**Opción A (recomendada): sesión WAHA separada.**
WAHA soporta múltiples sesiones. El API principal usa la sesión definida en `WAHA_SESSION` (default: `"default"`). Tu módulo puede usar una sesión distinta, por ejemplo `"prox"`. Configura en tu `.env`:

```
WAHA_SESSION=prox
```

Y registra el webhook de esa sesión apuntando a tu contenedor:

```
WAHA_WEBHOOK_URL=http://prox-ve-api:8080/webhook/waha
```

Esto elimina el conflicto de rutas completamente.

**Opción B: fan-out desde el API principal.**
El API principal recibe todos los mensajes de WAHA en `/webhook/waha` y los despacha internamente. Podría hacer un HTTP POST a tu módulo para los mensajes que le correspondan (por número de sesión, por tipo de mensaje, etc.). Esta opción requiere un cambio en `api/bot/webhook_router.py` del core. Abre un issue si quieres coordinar esto.

No intentes montar tu propio handler en la misma sesión WAHA sin coordinación previa. Los webhooks de WAHA son por sesión, no por ruta: solo un destino por sesión.

---

## Cómo deployar tu módulo en el VPS

Asume que tienes acceso SSH al droplet `root@134.122.54.197` y que tu módulo tiene su propio `Dockerfile` y `docker-compose.yml` dentro de `esfuerzos/modulos/migration_prox/`.

```bash
# 1. Conectarte al VPS
ssh root@134.122.54.197

# 2. Ir al directorio del repo (asume que el repo está clonado en /root/reune-ve)
cd /root/reune-ve

# 3. Pull del código más reciente
git pull origin main

# 4. Ir a tu módulo
cd esfuerzos/modulos/migration_prox

# 5. Copiar .env al directorio si no está (nunca lo commitees al repo)
# cp /root/prox.env .env

# 6. Build y levantar tu contenedor
docker compose up -d --build

# 7. Verificar que levantó
docker ps | grep prox
docker logs prox-ve-api --tail 50

# 8. Verificar conectividad con el API principal desde tu contenedor
docker exec prox-ve-api curl -s http://crisis-ve:8081/health
```

Para actualizaciones posteriores:

```bash
cd /root/reune-ve && git pull origin main
cd esfuerzos/modulos/migration_prox
docker compose up -d --build
docker logs prox-ve-api --tail 20
```

Si tu contenedor necesita las variables de entorno del API principal (como `SUPABASE_URL` y `SUPABASE_SERVICE_ROLE_KEY`), crea un `.env` propio en tu directorio con esas variables. No compartas el `.env` del core directamente; mantén tu configuración separada para que los deployments sean independientes.

---

## Preguntas

Si algo de esta guía no está claro, o si el endpoint `/admin/process_photo` que necesitas no existe todavía, o si hay un conflicto de red o de sesión WAHA que no anticipamos aquí: abre un issue en el repo con el prefijo `[prox]` en el título.

Lo que ya sabemos que hay que resolver en conjunto:

- Crear un Dockerfile/compose que levante el sistema `api/` (Reune) en producción, ya que el artefacto actual solo corre `app/` (visión/daños).
- Crear `/admin/process_photo` en el core para que tu módulo pueda disparar el face pipeline.
- Decidir si usamos sesión WAHA separada (opción A) o fan-out (opción B) para el routing de mensajes.
- Reconciliar el `mem_limit: 256m` del compose con los requisitos reales de InsightFace + el modelo de embeddings.
- Confirmar que el tenant/`Negocio` de tu módulo se puede mapear a alguna entidad del esquema principal (o si queda completamente separado por ahora).

Cualquier cambio que toque `api/main.py`, `api/bot/webhook_router.py`, o las migraciones de Supabase del core lo revisamos antes de mergear. Los cambios dentro de `esfuerzos/` son tuyos para mergear directamente.
