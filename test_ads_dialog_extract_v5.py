#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# test_ads_dialog_extract_v6.py  (tolerante a encabezados)
# Extrae SOLO el popup ads_dialog (ASUS/ROG) para CL/PE/CO
# y actualiza EXTRACT_LIM: solo TEXT, IMAGE/IMAGE_URL, URL
# en la fila del día (DATE o TIMESTAMP), COUNTRY y ITEM correctos.
# ------------------------------------------------------------

import os, re, json, time
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, urlencode

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# =========================
# Config scraping
# =========================
COUNTRIES = [
    ("CL", {"lat": -33.45, "lng": -70.66}),
    ("PE", {"lat": -12.0464, "lng": -77.0428}),
    ("CO", {"lat":   4.7110, "lng": -74.0721}),
]
SITES = ["asus", "rog"]  # asus.com / rog.asus.com

HEADLESS = True
NAV_TIMEOUT = 70000
MAX_WAIT_SECONDS = 45
POLL_EVERY_MS = 1000

# =========================
# Google Sheets
# =========================
GOOGLE_SHEET_ID = "1jVd25vYzU6ygqTEwbwYXJtEHD-ya8V4RrRTdNFkLr_A"
WORKSHEET_TITLE = "EXTRACT_LIM"  # hoja EXISTENTE

def today_date() -> str:
    # Compararemos contra YYYY-MM-DD en DATE o TIMESTAMP
    return datetime.now().strftime("%Y-%m-%d")

def gspread_client():
    raw = os.getenv("GCP_SA_JSON","").strip()
    if not raw:
        raise RuntimeError("GCP_SA_JSON vacío. Define el secret/variable con el JSON del Service Account.")
    # Permite JSON en texto directo, o ruta a archivo .json
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        with open(raw, "r", encoding="utf-8") as f:
            info = json.load(f)
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

# --------- Normalización de encabezados ---------
def _norm(s: str) -> str:
    # sin espacios/guiones bajos y minúsculas
    return re.sub(r"[\s_]+", "", s.strip().lower()) if isinstance(s, str) else ""

def map_headers_flexible(header_row):
    """
    Devuelve dict con índices de columnas usando equivalentes flexibles.
    Campos clave:
      - date_col: DATE | TIMESTAMP | DATE (normalizado a YYYY-MM-DD)
      - country_col: COUNTRY
      - item_col: ITEM
      - text_col: TEXT
      - image_col: IMAGE_URL | IMAGE
      - url_col: URL
    Lanza error si faltan COUNTRY/ITEM/TEXT/URL o NO hay ninguna opción válida para fecha e imagen.
    """
    idx = {h:i for i,h in enumerate(header_row)}
    norm_map = {_norm(h): h for h in header_row}

    def find_any(*candidates):
        for cand in candidates:
            for k, orig in norm_map.items():
                if k == _norm(cand):
                    return idx[orig]
        return None

    date_col = find_any("DATE", "timestamp", "date")
    country_col = find_any("COUNTRY")
    item_col = find_any("ITEM")
    text_col = find_any("TEXT")
    image_col = find_any("IMAGE_URL", "IMAGE", "imageurl")
    url_col = find_any("URL")

    missing = []
    if date_col is None: missing.append("DATE/timestamp")
    if country_col is None: missing.append("COUNTRY")
    if item_col is None: missing.append("ITEM")
    if text_col is None: missing.append("TEXT")
    if image_col is None: missing.append("IMAGE_URL/IMAGE")
    if url_col is None: missing.append("URL")

    if missing:
        raise RuntimeError(f"Faltan columnas requeridas en '{WORKSHEET_TITLE}': {', '.join(missing)}")

    return {
        "date": date_col,
        "country": country_col,
        "item": item_col,
        "text": text_col,
        "image": image_col,
        "url": url_col,
    }

def load_sheet_and_index():
    gc = gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.worksheet(WORKSHEET_TITLE)  # EXISTENTE
    values = ws.get_all_values() or []
    if not values:
        raise RuntimeError(f"La hoja '{WORKSHEET_TITLE}' está vacía. Debe existir con encabezados.")
    header = values[0]
    cols = map_headers_flexible(header)  # <- usa mapeo flexible
    return ws, values, header, cols

# =========================
# Helpers web
# =========================
def cache_bust(u: str) -> str:
    parts = list(urlsplit(u))
    parts[3] = (parts[3] + "&" if parts[3] else "") + f"_cb={int(time.time()*1000)}"
    return urlunsplit(parts)

COOKIE_JS = r"""
(() => {
  const texts = ["Aceptar todas","Aceptar todo","Aceptar","Accept All","Accept","Agree","Allow all"];
  const click = (el)=>{ try{el.click();return true;}catch(e){return false;} };
  const selectors = [
    "#onetrust-accept-btn-handler",
    "#onetrust-accept-all-handler",
    "#CybotCookiebotDialogBodyLevelButtonAccept",
    ".osano-cm-accept-all",
    ".truste_button_2",
    ".qc-cmp2-summary-buttons__button--accept-all",
    "button[aria-label*='aceptar' i]",
    "button[aria-label*='accept' i]",
  ];
  for (const sel of selectors){ const el=document.querySelector(sel); if (el && click(el)) return true; }
  const btns = Array.from(document.querySelectorAll('button,[role=button],a'));
  for (const b of btns){
    const t=(b.innerText||b.textContent||"").trim().toLowerCase();
    if (texts.some(v=>t.includes(v.toLowerCase()))){ if (click(b)) return true; }
  }
  return false;
})();
"""

FIND_POPUP_JS = r"""
(() => {
  const ret={found:false,title:"",image:"",href:""};
  const matches = (n)=> n && (n.matches(".PB_promotionBanner.PB_corner.PB_promotionMode") || n.id==="ads_dialog" || (n.id||"").includes("ads_dialog"));
  let host = document.querySelector(".PB_promotionBanner.PB_corner.PB_promotionMode, #ads_dialog, [id*='ads_dialog']");
  if (!host){
    const btn = document.querySelector("a.PB_button");
    let n=btn;
    for (let i=0;n && i<6;i++){ if (matches(n)) { host=n; break; } n=n?.parentElement; }
  }
  if (!host) return ret;
  const pb = host.querySelector(".PB_body") || host;
  const t = pb.querySelector(".PB_title"); if (t) ret.title = (t.textContent||"").trim();
  const img = pb.querySelector(".PB_picture img") || pb.querySelector("img"); if (img) ret.image = img.getAttribute("src")||"";
  const a = pb.querySelector("a.PB_button"); if (a) ret.href = (a.getAttribute("href")||"").trim();
  if (ret.title||ret.image||ret.href) ret.found=true;
  return ret;
})();
"""

def accept_cookies(page):
    try:
        if page.evaluate(COOKIE_JS):
            print("✅ Cookies aceptadas.")
        else:
            print("⚠️ No se encontró banner de cookies (continuando).")
    except Exception:
        print("⚠️ Error aceptando cookies (continuando).")

def find_popup(page):
    try:
        data = page.evaluate(FIND_POPUP_JS)
        if data and data.get("found"): return data
    except Exception:
        pass
    return None

def fire_triggers(page):
    try:
        page.mouse.wheel(0, 1200); page.wait_for_timeout(400)
        page.mouse.wheel(0, -600); page.wait_for_timeout(300)
        page.mouse.move(10, 3); page.wait_for_timeout(150)
        page.evaluate("() => document.dispatchEvent(new MouseEvent('mouseout',{bubbles:true,cancelable:true,relatedTarget:null,clientY:0}))")
        page.wait_for_timeout(250)
    except Exception:
        pass

# =========================
# Scrape por sitio
# =========================
def process_site(context, country: str, geo: dict, site: str):
    base = f"https://{'www' if site=='asus' else 'rog'}.asus.com/{country.lower()}/"
    url = cache_bust(base)
    print(f"🌍 [{country}] {site.upper()} → {url}")
    page = context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    except PlaywrightTimeout:
        print(f"⚠️ Timeout cargando {url}")
    accept_cookies(page)
    end = time.time() + MAX_WAIT_SECONDS
    found=None
    while time.time() < end and not found:
        fire_triggers(page)
        page.wait_for_timeout(POLL_EVERY_MS)
        found = find_popup(page)
    page.close()
    if found:
        return {
            "country":country,
            "site":site,  # 'asus' | 'rog'
            "title":(found.get("title") or "").strip(),
            "image":(found.get("image") or "").strip(),
            "href":(found.get("href") or "").strip(),
        }
    else:
        print(f"❌ No se encontró popup [{country}-{site}]")
        return None

# =========================
# Update selectivo en EXTRACT_LIM
# =========================
def make_item_label(site:str)->str:
    return "E-SHOP HOME POP UP ASUS.com" if site=="asus" else "E-SHOP HOME POP UP ROG.com"

def col_name_from_1based(col_idx_1based: int) -> str:
    name = ""
    x = col_idx_1based
    while x:
        x, rem = divmod(x-1, 26)
        name = chr(65 + rem) + name
    return name

def update_extract_lim(results):
    if not results:
        print("ℹ️ No hay resultados para actualizar en EXTRACT_LIM.")
        return
    ws, values, header, cols = load_sheet_and_index()
    date_today = today_date()

    # Mapear filas del día por (COUNTRY, ITEM)
    key_to_row = {}
    for r_i in range(1, len(values)):  # saltar header
        row = values[r_i]
        def safe_get(col_idx):
            return row[col_idx] if col_idx < len(row) else ""
        # Normalizamos la fecha de la celda a YYYY-MM-DD para comparar
        raw_date = safe_get(cols["date"]).strip()
        cell_date = raw_date[:10] if len(raw_date) >= 10 else raw_date
        country_val = safe_get(cols["country"])
        item_val = safe_get(cols["item"])
        if cell_date == date_today and country_val and item_val:
            key_to_row[(country_val, item_val)] = r_i + 1  # 1-based

    # Construimos updates TEXT..URL (tres columnas contiguas)
    reqs = []
    text_col_1 = cols["text"] + 1
    image_col_1 = cols["image"] + 1
    url_col_1 = cols["url"] + 1

    # Validamos que TEXT..URL estén contiguas; si no, hacemos 3 rangos separados
    contiguous = (image_col_1 == text_col_1 + 1) and (url_col_1 == image_col_1 + 1)

    for res in results:
        item = make_item_label(res["site"])
        key = (res["country"], item)
        if key not in key_to_row:
            print(f"⚠️ No existe fila para DATE={date_today}, COUNTRY={res['country']}, ITEM={item}. No se actualizará.")
            continue
        row_idx = key_to_row[key]
        title, image, href = res["title"], res["image"], res["href"]

        if contiguous:
            c1 = col_name_from_1based(text_col_1)
            c2 = col_name_from_1based(url_col_1)
            rng = f"{c1}{row_idx}:{c2}{row_idx}"
            reqs.append({"range": rng, "values": [[title, image, href]]})
        else:
            # Tres rangos independientes (por si columnas no están contiguas)
            c_text = col_name_from_1based(text_col_1)
            c_img  = col_name_from_1based(image_col_1)
            c_url  = col_name_from_1based(url_col_1)
            reqs.append({"range": f"{c_text}{row_idx}:{c_text}{row_idx}", "values": [[title]]})
            reqs.append({"range": f"{c_img}{row_idx}:{c_img}{row_idx}", "values": [[image]]})
            reqs.append({"range": f"{c_url}{row_idx}:{c_url}{row_idx}", "values": [[href]]})

    if not reqs:
        print("ℹ️ No hay coincidencias del día para actualizar.")
        return

    ws.batch_update(reqs, value_input_option="USER_ENTERED")
    print(f"✅ Actualizadas {len(reqs)} escritura(s) en '{WORKSHEET_TITLE}' (solo TEXT, IMAGE/IMAGE_URL, URL).")

# =========================
# Main
# =========================
def run():
    results=[]
    with sync_playwright() as p:
        browser=p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox","--disable-dev-shm-usage","--window-size=1600,900"]
        )
        context=browser.new_context(viewport={"width":1600,"height":900},
                                    locale="es-CL",
                                    timezone_id="America/Santiago",
                                    ignore_https_errors=True)
        for c,g in COUNTRIES:
            context.set_geolocation({"latitude":g["lat"],"longitude":g["lng"]})
            context.grant_permissions(["geolocation"])
            for s in SITES:
                r=process_site(context,c,g,s)
                if r: results.append(r)
        context.close();browser.close()
    update_extract_lim(results)

if __name__=="__main__":
    run()

