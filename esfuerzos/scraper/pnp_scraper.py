"""
Scraper para https://www.sistemaspnp.com/cedula/ usando Playwright + stealth.

Con proxies: cada worker tiene su propio BrowserContext con una IP diferente.
Sin proxies: todos los workers comparten un contexto (mismo IP).

proxies.txt — una por línea, formatos soportados:
    http://1.2.3.4:8080
    http://user:pass@1.2.3.4:8080
    socks5://1.2.3.4:1080
"""
import asyncio
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright_stealth import stealth_async

from db.pnp_repository import cedula_exists, upsert_cedula

COOKIES_FILE    = "cookies_pnp.json"
PROXIES_FILE    = "proxies.txt"
PROXY_STATS_FILE = "proxy_stats.json"


class _NeedRotation(Exception):
    """Señal interna: este worker debe rotar a otro proxy."""


logger = logging.getLogger(__name__)

BASE_URL   = "https://www.sistemaspnp.com/cedula/"
RESULT_URL = "https://www.sistemaspnp.com/cedula/resultado.php"

START_CEDULA = 10_000
END_CEDULA   = 33_000_000

_WORKERS                = 3
_DELAY_BETWEEN          = 1.5
_RETRY_MAX              = 3
_RATE_LIMIT_PAUSE       = 60
_TIMEOUTS_BEFORE_ROTATE = 2

_NOT_FOUND_KW  = ["no encontrado", "no existe", "not found", "sin resultados", "cédula no"]
_RATE_LIMIT_KW = ["límite de consultas", "limite de consultas", "rate limit", "intente nuevamente"]

_SAME_SITE_MAP = {
    "strict": "Strict", "lax": "Lax", "none": "None",
    "no_restriction": "None", "unspecified": "Lax",
}


# ─────────────────────── PROXY STATS ─────────────────────────────────

class ProxyStats:
    """Conteo de éxitos y fallos por proxy. Persistido en JSON."""

    def __init__(self, proxies: list[str], path: str = PROXY_STATS_FILE):
        self._path = Path(path)
        self._lock = asyncio.Lock()
        # Cargar estado previo si existe
        existing = {}
        if self._path.exists():
            try:
                existing = json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                pass
        self._stats: dict[str, dict] = {}
        for p in proxies:
            self._stats[p] = existing.get(p, {"ok": 0, "fail": 0})

    async def record_ok(self, proxy: str) -> None:
        async with self._lock:
            if proxy in self._stats:
                self._stats[proxy]["ok"] += 1

    async def record_fail(self, proxy: str) -> None:
        async with self._lock:
            if proxy in self._stats:
                self._stats[proxy]["fail"] += 1

    def save(self) -> None:
        self._path.write_text(
            json.dumps(self._stats, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def summary(self) -> str:
        working   = sum(1 for s in self._stats.values() if s["ok"] > 0)
        failed    = sum(1 for s in self._stats.values() if s["fail"] > 0 and s["ok"] == 0)
        untested  = sum(1 for s in self._stats.values() if s["ok"] == 0 and s["fail"] == 0)
        return (
            f"Proxies — total: {len(self._stats)} | "
            f"funcionando: {working} | sólo fallos: {failed} | sin probar: {untested}"
        )


# ─────────────────────── PROXY POOL ──────────────────────────────────

class ProxyPool:
    """Round-robin thread-safe sobre una lista de proxies."""
    def __init__(self, proxies: list[str]):
        self._proxies = proxies
        self._idx     = 0
        self._lock    = asyncio.Lock()

    async def next(self) -> str:
        async with self._lock:
            proxy = self._proxies[self._idx % len(self._proxies)]
            self._idx += 1
            return proxy

    def __len__(self) -> int:
        return len(self._proxies)


# ─────────────────────── UTILS ───────────────────────────────────────

def _normalize_proxy(line: str) -> str:
    if re.match(r'^\w+://', line):
        return line
    if '@' in line:
        return f"http://{line}"
    parts = line.split(":")
    if len(parts) == 2:
        return f"http://{line}"
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    return f"http://{line}"


def load_proxies(path: str = PROXIES_FILE) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    proxies = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            proxies.append(_normalize_proxy(line))
    return proxies


def _mask(proxy: str) -> str:
    return re.sub(r'://([^:@]+):([^@]+)@', r'://\1:***@', proxy)


def _proxy_cfg(proxy_url: str) -> dict:
    m = re.match(r'(\w+)://([^:@]+):([^@]+)@(.+)', proxy_url)
    if m:
        return {
            "server":   f"{m.group(1)}://{m.group(4)}",
            "username": m.group(2),
            "password": m.group(3),
        }
    return {"server": proxy_url}


def _load_pw_cookies() -> list[dict]:
    p = Path(COOKIES_FILE)
    if not p.exists():
        return []
    raw = json.loads(p.read_text(encoding="utf-8"))
    result = []
    for c in raw:
        ss = _SAME_SITE_MAP.get(str(c.get("sameSite") or "Lax").lower(), "Lax")
        result.append({
            "name":     c["name"],
            "value":    c["value"],
            "domain":   c.get("domain", ".sistemaspnp.com"),
            "path":     c.get("path", "/"),
            "secure":   c.get("secure", False),
            "httpOnly": c.get("httpOnly", False),
            "sameSite": ss,
        })
    return result


async def _make_context(
    browser: Browser,
    proxy_url: str | None,
    pw_cookies: list[dict],
) -> BrowserContext:
    kwargs = {"locale": "es-VE", "timezone_id": "America/Caracas"}
    if proxy_url:
        kwargs["proxy"] = _proxy_cfg(proxy_url)
    context = await browser.new_context(**kwargs)
    if pw_cookies:
        await context.add_cookies(pw_cookies)
    return context


# ─────────────────────── PARSER ──────────────────────────────────────

def _field(html: str, label: str) -> str | None:
    m = re.search(rf'<strong>{re.escape(label)}:</strong>\s*([^<]+)', html)
    if not m:
        return None
    val = m.group(1).strip()
    return None if val in ("-", "", "N/A") else val


def _solve_captcha_text(text: str) -> int | None:
    m = re.search(r'(\d+)\s*([+\-×÷*])\s*(\d+)', text)
    if not m:
        return None
    a, op, b = int(m.group(1)), m.group(2), int(m.group(3))
    if op == '+':         return a + b
    if op == '-':         return a - b
    if op in ('*', '×'):  return a * b
    if op in ('/', '÷'):  return a // b if b else None
    return None


def _parse_result(html: str) -> tuple[dict, str]:
    low = html.lower()

    if any(kw in low for kw in _RATE_LIMIT_KW):
        return {}, "rate_limited"

    # Respuesta inválida: no viene del servidor real (error de proxy/red)
    if "card-body" not in low:
        return {}, "proxy_error"

    if any(kw in low for kw in _NOT_FOUND_KW):
        return {}, "not_found"

    data = {
        "rif":              _field(html, "RIF"),
        "primer_apellido":  _field(html, "Primer Apellido"),
        "segundo_apellido": _field(html, "Segundo Apellido"),
        "nombres":          _field(html, "Nombres"),
        "estado":           _field(html, "Estado"),
        "municipio":        _field(html, "Municipio"),
        "parroquia":        _field(html, "Parroquia"),
        "centro_electoral": _field(html, "Centro Electoral"),
    }

    if data["primer_apellido"] or data["nombres"]:
        return data, "found"

    # card-body presente pero nombre no parseado → revisar manualmente, NO guardar como found
    return {}, "proxy_error"


# ─────────────────────── FETCH ────────────────────────────────────────

async def _fetch_cedula(page: Page, cedula: int, worker_id: int) -> dict:
    """
    Retorna el dict del resultado.
    Lanza _NeedRotation si el proxy está bloqueado o devuelve respuesta inválida.
    """
    consecutive_bad = 0

    for attempt in range(1, _RETRY_MAX + 1):
        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)
            consecutive_bad = 0

            captcha_label = await page.text_content("label.captcha-question", timeout=5_000)
            if not captcha_label:
                logger.warning("[W%d] CAPTCHA label no encontrado (intento %d)", worker_id, attempt)
                consecutive_bad += 1
                if consecutive_bad >= _TIMEOUTS_BEFORE_ROTATE:
                    raise _NeedRotation()
                continue

            answer = _solve_captcha_text(captcha_label)
            if answer is None:
                logger.warning("[W%d] CAPTCHA no resuelto: '%s'", worker_id, captcha_label.strip())
                continue

            await page.fill('input[name="cedula"]',  str(cedula))
            await page.fill('input[name="captcha"]', str(answer))
            await page.press('input[name="captcha"]', "Enter")
            await page.wait_for_load_state("domcontentloaded", timeout=20_000)

            result_html = await page.content()
            data, status = _parse_result(result_html)

            if status in ("rate_limited", "proxy_error"):
                logger.warning(
                    "[W%d] %s en cédula %d — rotando proxy",
                    worker_id, status, cedula,
                )
                raise _NeedRotation()

            return {
                "cedula":           cedula,
                "rif":              data.get("rif"),
                "primer_apellido":  data.get("primer_apellido"),
                "segundo_apellido": data.get("segundo_apellido"),
                "nombres":          data.get("nombres"),
                "estado":           data.get("estado"),
                "municipio":        data.get("municipio"),
                "parroquia":        data.get("parroquia"),
                "centro_electoral": data.get("centro_electoral"),
                "raw_html":         result_html,
                "status":           status,
                "scraped_at":       datetime.now(timezone.utc).isoformat(),
            }

        except _NeedRotation:
            raise
        except Exception as exc:
            consecutive_bad += 1
            logger.debug("[W%d] Error cédula %d (intento %d): %s", worker_id, cedula, attempt, exc)
            if consecutive_bad >= _TIMEOUTS_BEFORE_ROTATE:
                logger.warning("[W%d] %d errores consecutivos — rotando proxy", worker_id, consecutive_bad)
                raise _NeedRotation()
            await asyncio.sleep(2.0 * attempt)

    raise _NeedRotation()


# ─────────────────────── WORKER ──────────────────────────────────────

async def _worker(
    worker_id: int,
    browser: Browser,
    proxy_pool: ProxyPool | None,
    proxy_stats: ProxyStats | None,
    pw_cookies: list[dict],
    counter: list,
    lock: asyncio.Lock,
    end: int,
    conn: sqlite3.Connection,
    counters: dict,
    headed: bool = False,
) -> None:
    proxy = await proxy_pool.next() if proxy_pool else None
    ctx   = await _make_context(browser, proxy, pw_cookies)
    page  = await ctx.new_page()
    await stealth_async(page)
    if headed:
        # Ventana pequeña en la esquina, workers ligeramente escalonados
        x, y = worker_id * 30, worker_id * 30
        await page.evaluate(f"window.resizeTo(480, 360); window.moveTo({x}, {y});")

    current_cedula = None

    try:
        while True:
            if current_cedula is None:
                async with lock:
                    if counter[0] > end:
                        return
                    current_cedula = counter[0]
                    counter[0] += 1

            if cedula_exists(conn, current_cedula):
                counters["skipped"] += 1
                current_cedula = None
                continue

            try:
                result = await _fetch_cedula(page, current_cedula, worker_id)
            except _NeedRotation:
                if proxy_stats and proxy:
                    await proxy_stats.record_fail(proxy)
                    proxy_stats.save()

                if proxy_pool:
                    await ctx.close()
                    proxy = await proxy_pool.next()
                    ctx   = await _make_context(browser, proxy, pw_cookies)
                    page  = await ctx.new_page()
                    await stealth_async(page)
                    if headed:
                        x, y = worker_id * 30, worker_id * 30
                        await page.evaluate(f"window.resizeTo(480, 360); window.moveTo({x}, {y});")
                    logger.info("[W%d] Rotado → %s", worker_id, _mask(proxy))
                    if proxy_stats:
                        logger.info("[W%d] %s", worker_id, proxy_stats.summary())
                else:
                    logger.warning("[W%d] Rate-limit sin proxies — pausa %ds", worker_id, _RATE_LIMIT_PAUSE)
                    await asyncio.sleep(_RATE_LIMIT_PAUSE)
                continue  # reintenta la misma cédula con nueva IP

            # Éxito
            if proxy_stats and proxy:
                await proxy_stats.record_ok(proxy)

            upsert_cedula(conn, result)
            counters["total"] += 1

            if result["status"] == "found":
                counters["found"] += 1
                nombre_completo = " ".join(filter(None, [
                    result.get("primer_apellido"),
                    result.get("segundo_apellido"),
                    result.get("nombres"),
                ])) or "?"
                logger.info("[W%d] %d → %s", worker_id, current_cedula, nombre_completo)
            else:
                logger.debug("[W%d] %d → %s", worker_id, current_cedula, result["status"])

            if counters["total"] % 500 == 0:
                logger.info(
                    "Progreso: %d procesadas | %d encontradas | última: %d",
                    counters["total"], counters["found"], current_cedula,
                )
                if proxy_stats:
                    logger.info(proxy_stats.summary())

            current_cedula = None
            await asyncio.sleep(_DELAY_BETWEEN)

    finally:
        await ctx.close()


# ─────────────────────── ENTRY POINT ─────────────────────────────────

async def scrape_pnp(
    conn: sqlite3.Connection,
    start: int = START_CEDULA,
    end: int = END_CEDULA,
    headless: bool = True,
    proxies: list[str] | None = None,
) -> None:
    from db.pnp_repository import get_max_cedula

    last = get_max_cedula(conn)
    if last is not None and last >= start:
        start = last + 1
        logger.info("Resumiendo desde cédula %d", start)

    if start > end:
        logger.info("Rango ya completado.")
        return

    pw_cookies = _load_pw_cookies()
    if pw_cookies:
        logger.info("Cookies cargadas desde %s (%d cookies)", COOKIES_FILE, len(pw_cookies))
    else:
        logger.warning("No se encontró %s — sin cookies del browser", COOKIES_FILE)

    proxy_pool  = ProxyPool(proxies) if proxies else None
    proxy_stats = ProxyStats(proxies) if proxies else None
    n_workers   = _WORKERS

    proxy_label = f"pool de {len(proxies)} proxies" if proxies else "sin proxy"
    logger.info(
        "PNP scraper iniciado → cédulas %d–%d | workers: %d | %s | delay: %.1fs",
        start, end, n_workers, proxy_label, _DELAY_BETWEEN,
    )

    counter  = [start]
    lock     = asyncio.Lock()
    counters = {"total": 0, "found": 0, "skipped": 0, "errors": 0}

    async with async_playwright() as pw:
        launch_args = {"headless": headless}
        if proxies:
            launch_args["proxy"] = {"server": "http://per-context"}
        browser = await pw.chromium.launch(**launch_args)

        tasks = [
            asyncio.create_task(
                _worker(i, browser, proxy_pool, proxy_stats, pw_cookies,
                        counter, lock, end, conn, counters, headed=not headless)
            )
            for i in range(n_workers)
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await browser.close()

            if proxy_stats:
                proxy_stats.save()
                logger.info(proxy_stats.summary())

            logger.info(
                "PNP scraper detenido — %d procesadas | %d encontradas | %d errores",
                counters["total"], counters["found"], counters["errors"],
            )
