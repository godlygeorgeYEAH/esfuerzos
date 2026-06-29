#!/usr/bin/env bash
#
# pg_backup.sh — Respaldo del Postgres LOCAL del bot (FASE 0.4).
#
# Por qué: el named volume `pg_data` sobrevive reinicios y redeploys, pero NO un
# `docker compose down -v` ni el borrado del host. Este respaldo da durabilidad
# ante desastre con UNA escritura periódica (no por mensaje) — irrelevante para
# la cuota de Supabase, frente a escribir cada turno como hacía V2.
#
# Uso (desde el host, idealmente vía cron 1x/hora):
#   ./scripts/pg_backup.sh
#
# Cron sugerido (crontab -e):
#   0 * * * * cd /ruta/al/repo && ./scripts/pg_backup.sh >> scripts/pg_backups/backup.log 2>&1
#
# Retiene los últimos 48 dumps (~2 días si corre cada hora).
set -euo pipefail

CONTAINER="${PG_CONTAINER:-reune-pg}"
PG_USER="${POSTGRES_USER:-reune}"
PG_DB="${POSTGRES_DB:-reune}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/pg_backups"
RETAIN="${PG_BACKUP_RETAIN:-48}"

mkdir -p "$OUT_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/reune_${STAMP}.sql.gz"

echo "[pg_backup] $(date -u) → ${OUT_FILE}"
docker exec "$CONTAINER" pg_dump -U "$PG_USER" -d "$PG_DB" | gzip > "$OUT_FILE"

# Rotación: conservar solo los últimos $RETAIN
ls -1t "$OUT_DIR"/reune_*.sql.gz 2>/dev/null | tail -n +$((RETAIN + 1)) | xargs -r rm -f

echo "[pg_backup] OK — $(du -h "$OUT_FILE" | cut -f1)"
