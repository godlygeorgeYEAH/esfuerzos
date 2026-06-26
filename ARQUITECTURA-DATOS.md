 Arquitectura de Bases de Datos
Todas las DBs son SQLite con journal_mode=WAL (write-ahead logging) para soportar lecturas concurrentes mientras el scraper escribe. Cada una vive en un archivo .db en el directorio de trabajo.

1. sos_personas.db
Fuente: https://sosvenezuela2026.com/api/persons/list
Scraper: scraper/sos_api_scraper.py
Script: scripts/scrape_sos_api.py

Tabla: sos_persons
Columna	Tipo	Descripción
id	TEXT PK	ID único de la persona
status	TEXT	Estado: desaparecido, localizado, etc.
cedula_masked	TEXT	Cédula parcialmente oculta (ej: V-****567)
display_name	TEXT	Nombre completo
municipio	TEXT	Municipio de ubicación
parroquia	TEXT	Parroquia de ubicación
photo_url	TEXT	URL de la foto en el servidor origen
photo_path	TEXT	Path relativo de foto (metadato de la API)
photo_local	TEXT	Ruta local tras descargar la foto
source_date	TEXT	Fecha del reporte en la fuente
fecha_scraped	TEXT	Timestamp ISO cuando fue scrapeado
Índices: status, source_date, display_name

2. pnp_cedulas.db
Fuente: https://www.sistemaspnp.com/cedula/
Scraper: scraper/pnp_scraper.py
Script: scripts/scrape_pnp.py

Itera cédulas venezolanas desde V-10000 hasta V-33000000. Resuelve un CAPTCHA aritmético por request. Usa Playwright + playwright-stealth + rotación de proxies para evadir rate limiting.

Tabla: cedulas
Columna	Tipo	Descripción
cedula	INTEGER PK	Número de cédula
rif	TEXT	RIF asociado
primer_apellido	TEXT	Primer apellido
segundo_apellido	TEXT	Segundo apellido
nombres	TEXT	Nombres
estado	TEXT	Estado (entidad federal)
municipio	TEXT	Municipio
parroquia	TEXT	Parroquia
centro_electoral	TEXT	Centro de votación asignado
raw_html	TEXT	HTML crudo de la respuesta (debugging)
status	TEXT	found / not_found / proxy_error
scraped_at	TEXT	Timestamp ISO del scraping
Índices: primer_apellido, nombres, status

Solo registros con status = 'found' tienen datos de persona. Los demás son cédulas no asignadas o errores de proxy.

3. reconexion.db
Fuente: https://desaparecidos-terremoto-api.theempire.tech/api/personas
Scraper: scraper/reconexion_scraper.py
Script: scripts/scrape_reconexion.py

REST API paginada (pageSize=100). Directorio de desaparecidos del terremoto. No requiere autenticación. Fotos alojadas en Supabase.

Tabla: personas
Columna	Tipo	Descripción
id	TEXT PK	UUID de la persona
nombre	TEXT	Nombre completo
edad	INTEGER	Edad
ubicacion	TEXT	Última ubicación conocida
fecha	TEXT	Fecha del reporte
descripcion	TEXT	Descripción adicional
contacto	TEXT	Contacto del reportante
foto_url	TEXT	URL de la foto
foto_local	TEXT	Ruta local tras descargar la foto
estado	TEXT	Estado: desaparecido, localizado, etc.
localizado_por	TEXT	Nombre de quien lo localizó
localizado_contacto	TEXT	Contacto de quien lo localizó
localizado_relacion	TEXT	Relación con la persona localizada
localizado_nota	TEXT	Nota adicional de localización
reportada	INTEGER	Flag booleano (0/1)
reportes	INTEGER	Cantidad de reportes
created_at	INTEGER	Unix timestamp de creación en la API
updated_at	INTEGER	Unix timestamp de última actualización
scraped_at	TEXT	Timestamp ISO del scraping
Índices: nombre, estado, updated_at

4. venezreporta.db
Fuente: https://venezuelareporta.org/buscar
Scraper: scraper/venezreporta_scraper.py
Script: scripts/scrape_venezreporta.py

Sitio Next.js con SSR. Se obtiene el HTML renderizado directamente vía aiohttp y se parsea con BeautifulSoup. Se scrapean dos estados: buscando y encontrado.

Tabla: reportes
Columna	Tipo	Descripción
id	TEXT PK	UUID de la persona (del href /reporte/{uuid})
nombre	TEXT	Nombre completo
ubicacion	TEXT	Ubicación reportada
estado	TEXT	Se busca / Encontrado
foto_url	TEXT	URL de la foto (Supabase storage)
foto_local	TEXT	Ruta local tras descargar la foto
detail_url	TEXT	URL completa del reporte individual
scraped_at	TEXT	Timestamp ISO del scraping
Índices: nombre, estado

Notas generales
WAL mode — todas las DBs usan PRAGMA journal_mode=WAL para permitir que el scraper escriba y otras herramientas lean simultáneamente sin bloqueos.

Upsert — todos los scrapers usan INSERT ... ON CONFLICT DO UPDATE SET en cada sweep, capturando cambios de estado (ej: persona pasa de desaparecido a localizado).

Fotos — se descargan localmente en carpetas {fuente}_images/{id}/. El campo foto_local se llena tras la descarga exitosa. Máximo 10 MB por foto.

Exportación unificada — scripts/export_all.py consolida las 4 DBs en un CSV con columnas normalizadas: fuente, nombre, ubicacion, estado, contacto, cedula, edad, extra.