# Next Actions — Reune VE
**Updated:** 2026-06-27 (session 4)

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
