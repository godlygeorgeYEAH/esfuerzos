# Next Actions — Reune VE
**Updated:** 2026-07-01 (session 5 — voice feedback / modo Buscar)

---

## NEW — Modo "Buscar" (voice feedback 2026-07-01, sin deploy aún)

Feedback (2 audios) de alguien verificando fotos contra el sistema: el bot forzaba
el formulario completo (nombre/edad/cédula/ubicación) por cada foto antes de decir
si había coincidencia, y una foto ya matcheada la noche anterior volvía a pedir
datos en vez de mostrar el match. Causa raíz confirmada en `_handle_photo`: sin
`report_id` activo, pedía datos antes de intentar el match.

**Implementado en `waha_intake.py` (NO desplegado a VPS):**
- Saludo ahora muestra un menú: *1) Buscar* / *2) Registrar*.
- Modo **Buscar**: cada foto o nombre/cédula se responde al toque, sin abrir el
  formulario de intake. 100% de solo lectura — no crea filas en `reports`,
  `photos` ni `matches`, así que las consultas casuales no ensucian el dashboard
  admin ni los pipelines de dedup/consolidación. Mantiene el mismo filtro de
  privacidad F7 (nunca revela un reporte de WhatsApp de otra familia).
- Modo **Registrar**: el flujo de intake completo de siempre, sin cambios.
- Sesiones existentes (sin `mode` guardado) caen por defecto en el flujo viejo —
  compatible con conversaciones ya en curso al momento del deploy.

**Antes de desplegar:**
- [ ] Revisar el diff de `waha_intake.py` (`git diff`)
- [ ] `git commit` + push a `esfuerzos` main, luego en el VPS: `git pull && docker compose build reune-ve-api && docker compose up -d`
- [ ] Probar en el bot real:
  - Enviar "hola" → debe mostrar el menú 1/2
  - Elegir "1" → enviar una foto → debe responder match/no-match sin pedir nombre/edad/cédula
  - Enviar una segunda foto de otra persona inmediatamente → debe responder de forma independiente (no debe arrastrar datos de la foto anterior)
  - Elegir "2" (o cualquier otra respuesta) → debe comportarse exactamente como el flujo de registro de siempre
  - `select count(*) from reports where source='waha_whatsapp' and created_at > now() - interval '1 hour';` tras varias búsquedas → debe seguir igual (0 filas nuevas por las búsquedas, solo por registros reales)

---

## BLOCKERS (GP must do — bot non-functional without these)

### 1. Groq API key on VPS

Bot receives messages but can't respond — LLM_API_KEY missing on VPS.

```bash
ssh root@13.140.166.72
nano /root/reune/.env
# Add:
# LLM_API_KEY=<your-groq-key>   (free at console.groq.com)
# LLM_BASE_URL=https://api.groq.com/openai/v1
# LLM_MODEL=llama-3.3-70b-versatile

cd /root/reune
docker compose restart reune-ve-api
curl http://localhost:8080/health
```

### 2. WhatsApp number for WAHA

WAHA is running but stuck at SCAN_QR_CODE. Any spare SIM with WhatsApp works.

```
Go to: http://13.140.166.72:3000
Start session "default" -> scan QR with WhatsApp phone
Status: SCAN_QR_CODE -> WORKING
```

### 3. Supabase housekeeping

- Rename project display name: Supabase dashboard > Settings > General > Name: "Reune VE"
- Rotate DB password: Settings > Database > Reset database password (old one in git history)

---

## CODE (next sprint, no blockers)

### 4. Test the full bot flow end-to-end

Once QR is scanned + Groq key is set:
```
Send to bot number: "hola"
-> should get welcome message
Follow Familiar flow -> report a missing person
-> check Supabase: select * from reports where source='whatsapp' limit 5;
-> check matches table for any cross-matches
```

### 5. Photo intake

Handle image messages in waha_intake.py:
- WAHA delivers `hasMedia=true`, `mediaUrl=<url>`
- Download -> face_pipeline.process_photo_for_report()
- Already wired in waha_intake.py — verify it works end-to-end

### 6. New scrapers

Sources identified in swarm (commit a680f56) but not yet active:
- VenezuelaTeBusca (tilores dataset — 26k records, see Tilores pro-bono outreach)
- hospitalesenvenezuela.com (HOSPITALES_ANON_KEY)
- redayudavenezuela.com (REDAYUDA_ANON_KEY)

### 7. Match notification

When reviewer confirms a match:
- Send WhatsApp message to reporter
- "Tenemos una posible coincidencia con {name}. Un voluntario la verificara."
- Needs WhatsApp Business number + Utility template approval

### 8. Review console (Lovable)

Human-in-the-loop match review UI:
- Connects to Supabase (URL + anon key)
- Shows matches table (status=pending)
- Reviewer approves/dismisses -> triggers match notification
- PRD section 7.4 is the spec

---

## INFRASTRUCTURE

### 9. Push current main to VPS

After repo is clean, pull on VPS:
```bash
ssh root@13.140.166.72
cd /root/reune
git pull origin main
docker compose build reune-ve-api
docker compose up -d
```

### 10. VPS image cleanup (optional)

If /root/crisis_images/ is still on VPS and the Supabase data is purged, the dir can be removed:
```bash
ssh root@13.140.166.72
# First confirm Supabase has no source='crisis' rows
# Then: rm -rf /root/crisis_images/
# Also remove the volume mount from docker-compose.yml
```
