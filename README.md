# Crisis VE

Crisis VE es una herramienta de respuesta a emergencias para sismos en Venezuela. Permite a ciudadanos reportar daños estructurales con fotos, registrar check-ins de "estoy bien", transcribir mensajes de voz a texto, y consultar un mapa de reportes por estado y nivel de riesgo.

El backend es una API FastAPI desplegada en Docker sobre un VPS DigitalOcean. El análisis visual lo realiza el modelo Groq `meta-llama/llama-4-scout-17b-16e-instruct` (vision capable) siguiendo el protocolo FEMA ATC-20; la transcripción de audio usa Groq Whisper (`whisper-large-v3`). El frontend es una app React (Cloudflare Pages).

## Arquitectura

```
  +----------------------------+
  |   Usuarios (frontend)      |
  |   React / Cloudflare Pages |
  +------------+---------------+
               |
  +------------v-----------------------------------------+
  |             crisis-ve                                |
  |   FastAPI + Groq API | Puerto 8081 | 256 MB RAM      |
  |                                                       |
  |  +--------------------------------------------------+ |
  |  | POST /analyze                                    | |
  |  | Sube foto -> Groq vision -> FEMA ATC-20            | |
  |  | Persiste en Supabase Storage + damage_reports    | |
  |  +--------------------------------------------------+ |
  |                                                       |
  |  +--------------------------------------------------+ |
  |  | POST /checkin  / GET /checkin/{token}            | |
  |  | GET /checkins                                    | |
  |  | Registro "estoy bien" con share link y texto WA  | |
  |  +--------------------------------------------------+ |
  |                                                       |
  |  +--------------------------------------------------+ |
  |  | GET /reports                                     | |
  |  | Listado paginado de reportes de daño             | |
  |  +--------------------------------------------------+ |
  |                                                       |
  |  +--------------------------------------------------+ |
  |  | POST /transcribe                                 | |
  |  | Audio -> Groq Whisper -> extracción nombre/estado/ciudad |
  |  +--------------------------------------------------+ |
  |                                                       |
  |  +--------------------------------------------------+ |
  |  | GET /admin/stats                                 | |
  |  | PATCH /admin/report/{id}                         | |
  |  | PATCH /admin/checkin/{id}                        | |
  |  | Bearer auth requerido                            | |
  |  +--------------------------------------------------+ |
  +------------+------------------------------------------+
               |
  +------------v-----------+
  |        Supabase        |
  |  Postgres + Storage    |
  |  (damage_reports,      |
  |   safe_checkins,       |
  |   damage-photos bucket)|
  +------------------------+
```

## Endpoints

| Endpoint | Método | Auth | Descripción |
|---|---|---|---|
| `GET /health` | GET | Ninguna | Verifica que la API está viva; devuelve `{"status":"ok","timestamp":"..."}` |
| `POST /analyze` | POST | Ninguna | Multipart: `photo` (JPEG/PNG/WebP, max 10 MB) + `location_text` + `state` + `lat?` + `lng?`. Sube la foto a Supabase Storage, corre análisis Groq vision, persiste el reporte y devuelve `id`, `photo_url`, `risk_level`, `fema_category`, `damage_type`, `recommendation`, `ai_analysis`, `share_url` |
| `POST /checkin` | POST | Ninguna | JSON: `full_name`, `state`, `city`, `message?`. Registra check-in "estoy bien"; devuelve `id`, `share_token`, `share_url`, `whatsapp_text` |
| `GET /checkin/{token}` | GET | Ninguna | Recupera un check-in por su `share_token` |
| `GET /checkins` | GET | Ninguna | Lista check-ins con filtros opcionales (`name`, `state`, `limit`, `offset`) |
| `GET /reports` | GET | Ninguna | Lista reportes de daño (excluye `false_report=true`); filtros opcionales `state`, `risk_level`, `limit`, `offset` |
| `GET /admin/stats` | GET | Bearer token | Estadísticas agregadas: total reportes, por nivel de riesgo, check-ins, últimas 24h, falsos reportes, verificados |
| `PATCH /admin/report/{id}` | PATCH | Bearer token | Actualiza `verified` y/o `false_report` de un reporte |
| `PATCH /admin/checkin/{id}` | PATCH | Bearer token | Actualiza `verified` de un check-in |
| `POST /transcribe` | POST | Ninguna | Multipart: `audio` (cualquier formato soportado por Whisper). Transcribe con Groq Whisper y extrae entidades; devuelve `transcript`, `name`, `state`, `city` |

## Variables de entorno

| Variable | Default | Requerida | Descripción |
|---|---|---|---|
| `SUPABASE_URL` | | Sí | URL del proyecto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | | Sí | Service role key para el backend (bypasa RLS) |
| `GROQ_API_KEY` | | Sí | API key de Groq; usada para análisis visual (`/analyze`) y transcripción de audio (`/transcribe`) |
| `ADMIN_SECRET_TOKEN` | | Sí | Token que protege los endpoints de administración bajo `/admin/`. Se envía como `Authorization: Bearer <token>` |
| `ALLOWED_ORIGINS` | `http://localhost:5173` | No | Orígenes CORS permitidos, separados por coma. Si no se configura, solo acepta `localhost:5173` |
| `BASE_URL` | `https://crisisve.org` | No | URL pública del sitio; se usa para construir los links de compartir en `whatsapp_text` |

## Correr localmente

Prerequisito: Docker y Docker Compose instalados.

```bash
# Clona el repo y entra al directorio
git clone https://github.com/GPezzuti/crisis-ve.git
cd crisis-ve

# Crea el archivo de entorno con las variables requeridas
cat > .env << 'EOF'
SUPABASE_URL=https://<tu-proyecto>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service-role-key>
GROQ_API_KEY=<groq-api-key>
ADMIN_SECRET_TOKEN=<token-secreto-largo>
# Opcionales
ALLOWED_ORIGINS=http://localhost:5173
BASE_URL=http://localhost:8081
EOF

# Levanta la API
docker-compose up crisis-ve

# Para correr en background
docker-compose up -d crisis-ve

# Ver logs en tiempo real
docker-compose logs -f crisis-ve

# Apagar
docker-compose down
```

> Nota: el archivo `.env.example` en el repositorio corresponde a otra aplicación y no contiene las variables requeridas por este proyecto. Usa el bloque de arriba como referencia.

## Deploy en VPS

El VPS corre en DigitalOcean, IP `134.122.54.197`. La API escucha en el puerto `8081`.

```bash
# Sube los cambios desde tu máquina local
git push origin main

# Conéctate al VPS
ssh root@134.122.54.197

# En el VPS: entra al directorio del proyecto y sincroniza
cd /root/crisis-ve
git pull origin main

# Reconstruye la imagen si cambiaste requirements.txt o el Dockerfile
docker-compose build crisis-ve

# Reinicia el contenedor
docker-compose up -d crisis-ve

# Verifica que levantó correctamente
docker ps
docker logs crisis-ve --tail 50

# Comprueba el health check
curl http://localhost:8081/health
```

## Seguridad

- Todas las llamadas al backend de Supabase usan `SUPABASE_SERVICE_ROLE_KEY`, nunca la anon key. Esto bypasa RLS intencionalmente para operaciones del sistema; asegúrate de que ningún endpoint público exponga datos crudos sin filtrar.
- Los endpoints bajo `/admin/` requieren el header `Authorization: Bearer <ADMIN_SECRET_TOKEN>`. Si la variable no está configurada la aplicación no arranca (es requerida en `config.py`).
- CORS configurable via `ALLOWED_ORIGINS`. En producción, agrégalo al `.env` con el dominio exacto del frontend para no dejar el wildcard.
- `GROQ_API_KEY` y otras credenciales solo viven en el archivo `.env` del VPS. Nunca se commitean al repositorio.

## Pendiente

- **Estado de sesión en `/transcribe`:** la extracción de entidades usa una sola llamada sin historial; si el usuario envía múltiples audios, cada uno se procesa en forma independiente. Considerar acumular contexto por sesión si el flujo del frontend lo requiere.
- **Autenticación en `/analyze` y `/checkin`:** los endpoints de creación son públicos. Si se necesita limitar el abuso, agregar rate limiting a nivel de nginx o SlowAPI, o requerir un token de usuario.
- **Retención de fotos:** el bucket `damage-photos` en Supabase Storage no tiene política de expiración automática configurada. Implementar una función Edge o cron para purgar fotos según antigüedad cuando el storage crezca.
- **ADMIN_SECRET_TOKEN en producción:** asegúrate de que tiene un valor largo y aleatorio. Sin él la app no arranca; con un valor débil los endpoints de admin quedan expuestos.
