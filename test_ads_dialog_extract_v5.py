#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# test_ads_dialog_extract_v6.py
# Extrae SOLO el popup ads_dialog (ASUS/ROG) para CL/PE/CO
# y actualiza EXTRACT_LIM: solo TEXT, IMAGE_URL, URL
# en la fila del día (DATE=YYYY-MM-DD), COUNTRY y ITEM correctos.
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
WORKSHEET_TITLE = "EXTRACT_LIM"  # <- usamos la hoja existente

def today_date() -> str:
    # EXTRACT_LIM usa DATE (yyyy-mm-dd), no timestamp
    return datetime.now().strftime("%Y-%m-%d")

def gspread_client():
    raw = os.getenv("GCP_SA_JSON","").strip()
    if not raw:
        raise RuntimeError("GCP_SA_JSON vacío. Define el secret en GitHub (JSON completo del SA).")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        import base64
        info = json.loads(base64.b64decode(raw).decode("utf-8"))
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

def load_sheet_and_index():
    gc = gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.worksheet(WORKSHEET_TITLE)  # existente
    values = ws.get_all_values() or []
    if not values:
        raise RuntimeError(f"La hoja '{WORKSHEET_TITLE}' está vacía. Debe existir con encabezados.")
    header = values[0]
    idx = {h: i for i, h in enumerate(header)}
    # Campos obligatorios:
    need = ["DATE","COUNTRY","ITEM","TEXT","IMAGE_URL","URL"]
    missing = [c for c in need if c not in idx]
    if missing:
        raise RuntimeError(f"Faltan columnas requeridas en '{WORKSHEET_TITLE}': {', '.join(missing)}")
    return ws, values, header, idx

def batch_update_text_image_url(updates):
    """
    updates: lista de dicts con:
      row_idx (1-indexed), text, image_url, url, col_from, col_to
    Hace un batch_update por rangos contiguos (TEXT..URL).
    """
    if not updates:
        print("ℹ️ No hay filas para actualizar en Sheets.")
        return
    ws, _, header, idx = load_sheet_and_index()
    reqs = []
    # armamos los rangos A1 de TEXT..URL para cada fila
    def col_name(col_idx_1based: int) -> str:
        name = ""
        x = col_idx_1based
        while x:
            x, rem = divmod(x-1, 26)
            name = chr(65 + rem) + name
        return name

    text_col_1 = idx["TEXT"] + 1
    url_col_1 = idx["URL"] + 1  # incluimos hasta URL
    for u in updates:
        r = u["row_idx"]
        c1 = col_name(text_col_1)
        c2 = col_name(url_col_1)
        rng = f"{c1}{r}:{c2}{r}"
        reqs.append({"range": rng, "values": [[u["text"], u["image_url"], u["url"]]]})

    # batch update
    ws.batch_update(reqs, value_input_option="USER_ENTERED")
    print(f"✅ Actualizadas {len(reqs)} fila(s) en '{WORKSHEET_TITLE}' (solo TEXT, IMAGE_URL, URL).")

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
  const q = (s,root=document)=>root.querySelector(s);

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
  for (const sel of selectors){ const el=q(sel); if (el && click(el)) return true; }

  const tryText = (root=document) => {
    const nodes=[...root.querySelectorAll('button,[role=button],a')];
    for (const n of nodes){
      const t=(n.innerText||n.textContent||"").trim().toLowerCase();
      if (!t) continue;
      for (const needle of texts){ if (t.includes(needle.toLowerCase())) { if (click(n)) return true; } }
    }
    return false;
  };
  if (tryText()) return true;

  // shadow DOM
  const seen=new Set();
  const walk=(root)=>{
    if (!root || seen.has(root)) return false; seen.add(root);
    const nodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
    for (const n of nodes){
      if (tryText(n)) return true;
      if (n.shadowRoot && walk(n.shadowRoot)) return true;
    }
    return false;
  };
  if (walk(document)) return true;

  // iframes
  for (const fr of document.querySelectorAll("iframe")){
    try{
      const doc = fr.contentDocument || (fr.contentWindow && fr.contentWindow.document);
      if (!doc) continue;
      for (const sel of selectors){ const el=doc.querySelector(sel); if (el && click(el)) return true; }
      if (tryText(doc)) return true;
      const seen2=new Set();
      const walk2=(root)=>{
        if (!root || seen2.has(root)) return false; seen2.add(root);
        const nodes=root.querySelectorAll?root.querySelectorAll("*"):[];
        for (const n of nodes){
          if (n.shadowRoot && walk2(n.shadowRoot)) return true;
        }
        return false;
      };
      if (walk2(doc)) return true;
    }catch(e){}
  }
  return false;
})();
"""

FIND_POPUP_JS = r"""
(() => {
  const ret={found:false,title:"",image:"",href:""};

  const matches = (n)=> n && (n.matches(".PB_promotionBanner.PB_corner.PB_promotionMode") || n.id==="ads_dialog" || (n.id||"").includes("ads_dialog"));
  const extract = (root, host)=>{
    const get=(sel,base=host)=> (base && base.querySelector(sel)) || (root && root.querySelector(sel));
    const pb = get(".PB_body") || host;
    const t = get(".PB_title", pb);
    if (t) ret.title = (t.textContent||"").trim();
    const img = get(".PB_picture img", pb) || get("img", pb);
    if (img) ret.image = img.getAttribute("src") || "";
    const a = get("a.PB_button", pb);
    if (a) ret.href = (a.getAttribute("href")||"").trim();
    if (ret.title || ret.image || ret.href) ret.found=true;
  };

  const tryRoot = (root) => {
    let el = root.querySelector(".PB_promotionBanner.PB_corner.PB_promotionMode, #ads_dialog, [id*='ads_dialog']");
    if (el){ extract(root, el); if (ret.found) return true; }
    // heurística: encontrar botón PB_button y subir al contenedor
    const btn = root.querySelector("a.PB_button");
    if (btn){
      let n=btn;
      for (let i=0;i<6 && n;i++){
        if (matches(n)) { extract(root, n); if (ret.found) return true; }
        n=n.parentElement;
      }
    }
    return false;
  };

  if (tryRoot(document)) return ret;

  // shadow DOM
  const seen=new Set();
  const walk=(root)=>{
    if (!root || seen.has(root)) return false; seen.add(root);
    if (tryRoot(root)) return ret;
    const nodes=root.querySelectorAll?root.querySelectorAll("*"):[];
    for (const n of nodes){
      if (n.shadowRoot && walk(n.shadowRoot)) return ret;
    }
    return false;
  };
  if (walk(document)) return ret;

  // iframes
  for (const fr of document.querySelectorAll("iframe")){
    try{
      const doc=fr.contentDocument || (fr.contentWindow && fr.contentWindow.document);
      if (!doc) continue;
      if (tryRoot(doc)) return ret;
      const seen2=new Set();
      const walk2=(root)=>{
        if (!root || seen2.has(root)) return false; seen2.add(root);
        if (tryRoot(root)) return true;
        const nodes=root.querySelectorAll?root.querySelectorAll("*"):[];
        for (const n of nodes){
          if (n.shadowRoot && walk2(n.shadowRoot)) return true;
        }
        return false;
      };
      if (walk2(doc)) return ret;
    }catch(e){}
  }
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

def fire_triggers(page):
    try:
        page.mouse.wheel(0, 1500); page.wait_for_timeout(400)
        page.mouse.wheel(0, -800); page.wait_for_timeout(300)
        page.mouse.move(10, 3); page.wait_for_timeout(150)
        page.evaluate("() => document.dispatchEvent(new MouseEvent('mouseout',{bubbles:true,cancelable:true,relatedTarget:null,clientY:0}))")
        page.wait_for_timeout(250)
        page.evaluate("() => window.dispatchEvent(new Event('blur'))"); page.wait_for_timeout(120)
        page.evaluate("() => window.dispatchEvent(new Event('focus'))"); page.wait_for_timeout(300)
    except Exception:
        pass

def nav_subpage_roundtrip(page, base_url: str):
    try:
        store = base_url.rstrip("/") + "/store/"
        page.goto(cache_bust(store), wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(1200)
        page.go_back(wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(800)
    except Exception:
        pass

def find_popup(page):
    try:
        data = page.evaluate(FIND_POPUP_JS)
        if data and data.get("found"): return data
    except Exception:
        pass
    return None

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

    deadline = time.time() + MAX_WAIT_SECONDS
    found = None
    tries = 0

    while time.time() < deadline and not found:
        tries += 1
        fire_triggers(page)
        page.wait_for_timeout(POLL_EVERY_MS)
        found = find_popup(page)
        if found: break
        if tries == 3:
            nav_subpage_roundtrip(page, base)
            accept_cookies(page)

    if found:
        title = (found.get("title") or "").strip()
        image = (found.get("image") or "").strip()
        href = (found.get("href") or "").strip()
        page.close()
        return {
            "country": country,
            "site": site,  # 'asus' | 'rog'
            "title": title,
            "image": image,
            "href": href,
        }
    else:
        print(f"❌ [{country}-{site}] No se encontró popup.")
        page.close()
        return None

# =========================
# Update selectivo en EXTRACT_LIM
# =========================
def make_item_label(site: str) -> str:
    return "E-SHOP HOME POP UP ASUS.com" if site == "asus" else "E-SHOP HOME POP UP ROG.com"

def update_extract_lim(results):
    if not results:
        print("ℹ️ No hay resultados para actualizar en EXTRACT_LIM.")
        return
    ws, values, header, idx = load_sheet_and_index()
    date_today = today_date()

    # Índices 0-based → 1-based para A1
    text_col = idx["TEXT"] + 1
    image_col = idx["IMAGE_URL"] + 1
    url_col = idx["URL"] + 1

    # mapear filas existentes del día por (COUNTRY, ITEM)
    key_to_row = {}
    for r_i in range(1, len(values)):  # saltar header
        row = values[r_i]
        date_val = row[idx["DATE"]] if idx["DATE"] < len(row) else ""
        country_val = row[idx["COUNTRY"]] if idx["COUNTRY"] < len(row) else ""
        item_val = row[idx["ITEM"]] if idx["ITEM"] < len(row) else ""
        if date_val == date_today and country_val and item_val:
            key_to_row[(country_val, item_val)] = r_i + 1  # 1-indexed

    # construir updates solo donde exista fila
    reqs = []
    def col_name(col_idx_1based: int) -> str:
        name = ""
        x = col_idx_1based
        while x:
            x, rem = divmod(x-1, 26)
            name = chr(65 + rem) + name
        return name

    for res in results:
        item_label = make_item_label(res["site"])
        key = (res["country"], item_label)
        if key not in key_to_row:
            print(f"⚠️ No existe fila en {WORKSHEET_TITLE} para DATE={date_today}, COUNTRY={res['country']}, ITEM={item_label}. No se actualizará.")
            continue
        row_idx = key_to_row[key]
        c1 = col_name(text_col)
        c2 = col_name(url_col)  # TEXT..URL (tres columnas contiguas)
        rng = f"{c1}{row_idx}:{c2}{row_idx}"
        reqs.append({"range": rng, "values": [[res["title"], res["image"], res["href"]]]})

    if not reqs:
        print("ℹ️ No hay coincidencias de filas para actualizar hoy.")
        return
    ws.batch_update(reqs, value_input_option="USER_ENTERED")
    print(f"✅ Actualizadas {len(reqs)} fila(s) en '{WORKSHEET_TITLE}' (solo TEXT, IMAGE_URL, URL).")

# =========================
# Main
# =========================
def run():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox","--disable-dev-shm-usage",
                "--disable-gpu","--window-size=1600,900",
            ]
        )
        context = browser.new_context(
            viewport={"width":1600,"height":900},
            timezone_id="America/Santiago",
            locale="es-CL",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            ignore_https_errors=True,
            extra_http_headers={"Accept-Language":"es-CL,es;q=0.9,en;q=0.8"},
        )
        # Quitar bandera webdriver
        try:
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: ()=> undefined})")
        except Exception:
            pass

        for country, geo in COUNTRIES:
            try:
                context.grant_permissions(["geolocation"])
                context.set_geolocation({"latitude": geo["lat"], "longitude": geo["lng"]})
            except Exception:
                pass
            for site in SITES:
                r = process_site(context, country, geo, site)
                if r:
                    results.append(r)

        context.close(); browser.close()

    # Solo actualizar las filas existentes del día
    update_extract_lim(results)

if __name__ == "__main__":
    run()

