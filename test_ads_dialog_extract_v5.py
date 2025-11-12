#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# test_ads_dialog_extract_v6.py
# Headless + antifingerprint + locale/CL
# Busca SOLO ads_dialog y escribe al Sheet solicitado.
# Si no encuentra, deja evidencias en /tmp para depurar.
# ------------------------------------------------------------
import os, re, json, time
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, urlencode

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

COUNTRIES = [
    ("CL", {"lat": -33.45, "lng": -70.66}),
    ("PE", {"lat": -12.0464, "lng": -77.0428}),
    ("CO", {"lat":  4.7110, "lng": -74.0721}),
]
SITES = ["asus", "rog"]

GOOGLE_SHEET_ID = "1jVd25vYzU6ygqTEwbwYXJtEHD-ya8V4RrRTdNFkLr_A"
WORKSHEET_TITLE = "EXTRACT_ADS_DIALOG"

HEADERS = ["timestamp","COUNTRY","WEB","ITEM","HTML_SLOT","GA4 SLOT","ELEMENTS","TEXT","IMAGE","URL"]

HEADLESS = True
NAV_TIMEOUT = 70000
MAX_WAIT_SECONDS = 45
POLL_EVERY_MS = 1000

def ts(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def cache_bust(u: str) -> str:
    parts = list(urlsplit(u))
    parts[3] = (parts[3] + "&" if parts[3] else "") + f"_cb={int(time.time()*1000)}"
    return urlunsplit(parts)

def gspread_client():
    raw = os.getenv("GCP_SA_JSON","").strip()
    if not raw: raise RuntimeError("GCP_SA_JSON vacío.")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        import base64
        info = json.loads(base64.b64decode(raw).decode("utf-8"))
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

def write_rows(rows):
    gc = gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(WORKSHEET_TITLE, rows=200, cols=len(HEADERS))
    ws.clear()
    ws.append_row(HEADERS)
    if rows:
        ws.append_rows(rows)
    print(f"✅ Escrito en Sheets: {len(rows)} filas.")

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
          if (tryText(n)) return true;
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
    if (tryRoot(root)) return true;
    const nodes=root.querySelectorAll?root.querySelectorAll("*"):[];
    for (const n of nodes){
      if (n.shadowRoot && walk(n.shadowRoot)) return true;
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
        if page.evaluate(COOKIE_JS): print("✅ Cookies aceptadas.")
        else: print("⚠️ No se encontró banner de cookies (continuando).")
    except Exception:
        print("⚠️ Error aceptando cookies.")

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
    # Visita /store/ y vuelve, algunos popups disparan al “regresar”
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

def save_evidence(page, tag: str):
    try:
        png = f"/tmp/ads_dialog_{tag}.png"
        html = f"/tmp/ads_dialog_{tag}.html"
        page.screenshot(path=png, full_page=True)
        page_content = page.content()
        with open(html, "w", encoding="utf-8") as f: f.write(page_content)
        print(f"🧾 Evidencias: {png} | {html}")
    except Exception as e:
        print(f"⚠️ No se pudo guardar evidencias ({tag}): {e}")

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
        # mitad del tiempo: hacemos ida/vuelta a /store
        if tries == 3:
            nav_subpage_roundtrip(page, base)
            accept_cookies(page)  # por si reaparece CMP

    if found:
        print(f"✅ [{country}-{site}] Popup encontrado.")
        row = [
            ts(), country, site.upper(),
            "E-SHOP HOME POP UP", "PB_type_lowerRightCorner",
            "ads_dialog", "1",
            (found.get("title") or "").strip(),
            (found.get("image") or "").strip(),
            (found.get("href") or "").strip()
        ]
        page.close()
        return row
    else:
        print(f"❌ [{country}-{site}] No se encontró popup tras {MAX_WAIT_SECONDS}s.")
        save_evidence(page, f"{country}_{site}")
        page.close()
        return None

def run():
    rows=[]
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
                if r: rows.append(r)

        context.close(); browser.close()

    if rows:
        write_rows(rows)
    else:
        print("⚠️ No se obtuvieron datos. Revisa evidencias en /tmp para entender por qué.")

if __name__ == "__main__":
    run()

