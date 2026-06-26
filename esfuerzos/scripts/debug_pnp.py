"""
Debug de una sola cédula PNP con Playwright — imprime todo el flujo.

Uso:
    python scripts/debug_pnp.py 25755555
    python scripts/debug_pnp.py 25755555 --save
    python scripts/debug_pnp.py 25755555 --headed   # ver el browser
"""
import argparse
import asyncio
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.async_api import async_playwright
from playwright_stealth import stealth_async

BASE_URL     = "https://www.sistemaspnp.com/cedula/"
RESULT_URL   = "https://www.sistemaspnp.com/cedula/resultado.php"
COOKIES_FILE = "cookies_pnp.json"


def hr(title=""):
    print(f"\n{'─'*60}  {title}")


def solve_captcha(text: str):
    m = re.search(r'(\d+)\s*([+\-×÷*])\s*(\d+)', text)
    if not m:
        return None
    a, op, b = int(m.group(1)), m.group(2), int(m.group(3))
    if op == '+': return a + b
    if op == '-': return a - b
    if op in ('*', '×'): return a * b
    if op in ('/', '÷'): return a // b if b else None
    return None


async def debug_cedula(cedula: int, save: bool, headed: bool) -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not headed)
        context = await browser.new_context(locale="es-VE", timezone_id="America/Caracas")
        page = await context.new_page()
        await stealth_async(page)

        cookies_path = Path(COOKIES_FILE)
        if cookies_path.exists():
            raw = json.loads(cookies_path.read_text(encoding="utf-8"))
            _ss = {"strict": "Strict", "lax": "Lax", "none": "None", "no_restriction": "None", "unspecified": "Lax"}
            pw_cookies = [{
                "name": c["name"], "value": c["value"],
                "domain": c.get("domain", ".sistemaspnp.com"),
                "path": c.get("path", "/"),
                "secure": c.get("secure", False),
                "httpOnly": c.get("httpOnly", False),
                "sameSite": _ss.get(str(c.get("sameSite") or "Lax").lower(), "Lax"),
            } for c in raw]
            await context.add_cookies(pw_cookies)
            print(f"  Cookies cargadas: {[c['name'] for c in pw_cookies]}")
        else:
            print(f"  AVISO: {COOKIES_FILE} no encontrado — corriendo sin cookies del browser")

        # ── Step 1: GET ───────────────────────────────────────────
        hr("GET form page")
        print(f"  URL: {BASE_URL}")
        resp = await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)
        print(f"  Status: {resp.status}")
        print(f"  Cookies: {await context.cookies()}")

        form_html = await page.content()
        hr("Form HTML (primeros 2000 chars)")
        print(form_html[:2000])
        if save:
            Path("debug_form.html").write_text(form_html, encoding="utf-8")
            print("\n  → Guardado en debug_form.html")

        # ── Step 2: CAPTCHA ───────────────────────────────────────
        hr("CAPTCHA")
        try:
            label_text = await page.text_content("label.captcha-question", timeout=5_000)
            print(f"  Label text: '{label_text}'")
        except Exception as e:
            label_text = None
            print(f"  !! label.captcha-question no encontrado: {e}")
            print("  Intentando con cualquier label que contenga CAPTCHA...")
            labels = await page.query_selector_all("label")
            for lbl in labels:
                t = await lbl.text_content()
                if t and "captcha" in t.lower():
                    label_text = t
                    print(f"    Encontrado: '{t}'")
                    break

        if label_text:
            answer = solve_captcha(label_text)
            print(f"  Respuesta calculada: {answer}")
        else:
            answer = None
            print("  !! No se pudo resolver el CAPTCHA")

        # ── Step 3: Fill & Submit ─────────────────────────────────
        hr("Inputs disponibles en el form")
        inputs = await page.query_selector_all("input")
        for inp in inputs:
            name  = await inp.get_attribute("name")
            itype = await inp.get_attribute("type")
            val   = await inp.get_attribute("value")
            print(f"  name={name!r:20} type={itype!r:12} value={val!r}")

        if answer is not None:
            hr("Fill & Submit")
            await page.fill('input[name="cedula"]',  str(cedula))
            await page.fill('input[name="captcha"]', str(answer))
            print(f"  Llenado: cedula={cedula} captcha={answer}")
            await page.press('input[name="captcha"]', "Enter")
            await page.wait_for_load_state("domcontentloaded", timeout=20_000)

            result_html = await page.content()
            hr("Result HTML (completo)")
            print(result_html)
            if save:
                Path("debug_result.html").write_text(result_html, encoding="utf-8")
                print(f"\n  → Guardado en debug_result.html")
        else:
            print("\n!! Abortando — CAPTCHA no resuelto")

        hr("Done")
        await browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Debug de una cédula PNP con Playwright")
    parser.add_argument("cedula", type=int)
    parser.add_argument("--save",   action="store_true", help="Guardar HTML a archivos")
    parser.add_argument("--headed", action="store_true", help="Mostrar ventana del browser")
    args = parser.parse_args()

    asyncio.run(debug_cedula(args.cedula, args.save, args.headed))
