#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# test_ads_dialog_extract_v5.py
# ------------------------------------------------------------
# Extrae SOLO el POP-UP (ads_dialog) en ASUS y ROG para CL/PE/CO.
# - 100% headless (sin ventana)
# - Acepta cookies automáticamente:
#     * OneTrust (#onetrust-accept-btn-handler)
#     * Cookiebot (#CybotCookiebotDialogBodyLevelButtonAccept)
#     * Didomi, TrustArc, Quantcast, genéricos y shadow DOM
#     * Fallback por texto: "Aceptar todas", "Aceptar", "Accept All", "Accept"
# - Dispara múltiples triggers para forzar el popup:
#     * scroll profundo, mouse al borde (exit-intent), mouseout, blur/focus
# - Busca el popup aunque NO sea visible:
#     * DOM principal, iframes, y atraviesa shadow DOM
# - Columnas: timestamp, COUNTRY, WEB, ITEM, HTML_SLOT, GA4 SLOT, ELEMENTS, TEXT, IMAGE, URL
# - Sobrescribe la hoja del día SOLO si encuentra el popup (no duplica).
# ------------------------------------------------------------

import os
import re
import json
import time
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, urlencode

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

COUNTRIES = ["CL", "PE", "CO"]
SITES = ["asus", "rog"]

GOOGLE_SHEET_ID = "1jVd25vYzU6ygqTEwbwYXJtEHD-ya8V4RrRTdNFkLr_A"
WORKSHEET_TITLE = "EXTRACT_ADS_DIALOG"

HEADERS = [
    "timestamp", "COUNTRY", "WEB", "ITEM", "HTML_SLOT",
    "GA4 SLOT", "ELEMENTS", "TEXT", "IMAGE", "URL"
]

HEADLESS = True
NAV_TIMEOUT = 60000
MAX_WAIT_SECONDS = 20  # tiempo total para reintentos por sitio
POLL_EVERY_MS = 1000   # poll DOM cada 1s


# =========================
# Helpers básicos
# =========================
def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def cache_bust(u: str) -> str:
    ts = int(time.time() * 1000)
    parts = list(urlsplit(u))
    q = parts[3]
    parts[3] = (q + "&" if q else "") + urlencode({"_cb": ts})
    return urlunsplit(parts)


# =========================
# Google Sheets
# =========================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    raw = os.getenv("GCP_SA_JSON", "").strip()
    if not raw:
        raise RuntimeError("GCP_SA_JSON está vacío. Define el secret en el repositorio.")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # intento base64 si lo pegaron así por error
        import base64
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = json.loads(decoded)
        except Exception as e:
            raise ValueError("Invalid JSON in GCP_SA_JSON") from e
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def write_rows(rows):
    gc = get_gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(WORKSHEET_TITLE, rows=100, cols=len(HEADERS))
    # Sobrescribe
    ws.clear()
    ws.append_row(HEADERS)
    if rows:
        ws.append_rows(rows)
    print(f"✅ Escrito en Sheets: {len(rows)} filas.")


# =========================
# Cookies: aceptación robusta (incluye shadow DOM)
# =========================
COOKIE_JS = r"""
(() => {
  const texts = ["Aceptar todas", "Aceptar todo", "Aceptar", "Accept All", "Accept", "Agree", "Allow all"];
  const tryClick = (btn) => {
    if (!btn) return false;
    try {
      const r = btn.getBoundingClientRect();
      if (r && (r.width === 0 || r.height === 0)) {} // igual intentamos
      btn.click();
      return true;
    } catch (e) { return false; }
  };

  // 1) IDs y selectores comunes (OneTrust, Cookiebot, TrustArc, Quantcast)
  const candidates = [
    "#onetrust-accept-btn-handler",
    "#onetrust-accept-all-handler",
    "#CybotCookiebotDialogBodyLevelButtonAccept",
    ".osano-cm-accept-all",
    ".truste_button_2",
    ".qc-cmp2-summary-buttons .qc-cmp2-summary-buttons__button--accept-all",
    "button[aria-label*='aceptar' i]",
    "button[aria-label*='accept' i]",
    "button[title*='aceptar' i]",
    "button[title*='accept' i]",
  ];
  for (const sel of candidates) {
    const el = document.querySelector(sel);
    if (el && tryClick(el)) return true;
  }

  // 2) Por texto (en DOM plano)
  const btns = Array.from(document.querySelectorAll('button, [role="button"], a'));
  for (const b of btns) {
    const t = (b.innerText || b.textContent || "").trim().toLowerCase();
    if (!t) continue;
    for (const needle of texts) {
      if (t.includes(needle.toLowerCase())) {
        if (tryClick(b)) return true;
      }
    }
  }

  // 3) Shadow DOM: recorre todos los shadow roots buscando botones por texto
  const getAllShadowButtons = () => {
    const out = [];
    const seen = new Set();
    const walk = (root) => {
      if (!root || seen.has(root)) return;
      seen.add(root);
      const nodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
      for (const n of nodes) {
        // botón por tag o role
        if (
          n.tagName === "BUTTON" ||
          n.getAttribute("role") === "button" ||
          n.tagName === "A"
        ) {
          out.push(n);
        }
        // si tiene shadowRoot, entrar
        if (n.shadowRoot) walk(n.shadowRoot);
      }
    };
    walk(document);
    return out;
  };

  const sbtns = getAllShadowButtons();
  for (const b of sbtns) {
    const t = (b.innerText || b.textContent || "").trim().toLowerCase();
    if (!t) continue;
    for (const needle of texts) {
      if (t.includes(needle.toLowerCase())) {
        if (tryClick(b)) return true;
      }
    }
  }

  // 4) Iframes de consentimiento conocidos
  const iframes = Array.from(document.querySelectorAll("iframe"));
  for (const fr of iframes) {
    try {
      const doc = fr.contentDocument || (fr.contentWindow && fr.contentWindow.document);
      if (!doc) continue;
      const b2 = Array.from(doc.querySelectorAll("button, [role='button'], a"));
      for (const b of b2) {
        const t = (b.innerText || b.textContent || "").trim().toLowerCase();
        for (const needle of texts) {
          if (t.includes(needle.toLowerCase())) {
            if (tryClick(b)) return true;
          }
        }
      }
      const el = doc.querySelector("#onetrust-accept-btn-handler, #onetrust-accept-all-handler, #CybotCookiebotDialogBodyLevelButtonAccept");
      if (el && tryClick(el)) return true;
    } catch (e) {}
  }
  return false;
})();
"""

def accept_cookies(page):
    try:
        accepted = page.evaluate(COOKIE_JS)
        if accepted:
            print("✅ Cookies aceptadas.")
        else:
            print("⚠️ No se encontró banner de cookies (continuando).")
    except Exception:
        print("⚠️ Error al intentar aceptar cookies (continuando).")


# =========================
# Triggers de popup
# =========================
def fire_triggers(page):
    try:
        # scroll profundo y vuelta
        page.mouse.wheel(0, 1200)
        page.wait_for_timeout(400)
        page.mouse.wheel(0, -800)
        page.wait_for_timeout(400)

        # exit-intent (mouse al borde)
        page.mouse.move(10, 5)
        page.wait_for_timeout(200)
        page.evaluate("""
            () => {
              document.dispatchEvent(new MouseEvent('mouseout', {bubbles:true, cancelable:true, relatedTarget:null, clientY:0}));
            }
        """)
        page.wait_for_timeout(300)

        # blur/focus
        page.evaluate("() => window.dispatchEvent(new Event('blur'))")
        page.wait_for_timeout(150)
        page.evaluate("() => window.dispatchEvent(new Event('focus'))")
        page.wait_for_timeout(350)
    except Exception:
        pass


# =========================
# Búsqueda del popup (incluye iframes + shadow DOM)
# =========================
FIND_POPUP_JS = r"""
(() => {
  // Devuelve un objeto con: title, image, href, found (bool)
  const ret = {found:false, title:"", image:"", href:""};

  // Función para extraer desde un nodo "banner"
  const extractFrom = (root, banner) => {
    const get = (sel, base=banner) => (base && base.querySelector(sel)) || (root && root.querySelector(sel));
    const pb = get(".PB_body") || banner;

    const titleEl = get(".PB_title", pb);
    if (titleEl) ret.title = (titleEl.textContent || "").trim();

    const img = get("img", pb);
    if (img) ret.image = img.getAttribute("src") || "";

    const btn = get("a.PB_button", pb);
    if (btn) ret.href = (btn.getAttribute("href") || "").trim();

    ret.found = !!(ret.title || ret.image || ret.href);
  };

  // 1) DOM principal
  let banner = document.querySelector(".PB_promotionBanner.PB_corner.PB_promotionMode, #ads_dialog, [id*='ads_dialog']");
  if (banner) { extractFrom(document, banner); if (ret.found) return ret; }

  // 2) Shadow DOM profundo: busca banners
  const banners = [];
  const seen = new Set();

  const walk = (root) => {
    if (!root || seen.has(root)) return;
    seen.add(root);
    const nodes = root.querySelectorAll ? root.querySelectorAll("*") : [];
    for (const n of nodes) {
      if (n.matches && (n.matches(".PB_promotionBanner.PB_corner.PB_promotionMode") || n.id === "ads_dialog" || (n.id && n.id.includes("ads_dialog")))) {
        banners.push({root, el:n});
      }
      if (n.shadowRoot) walk(n.shadowRoot);
    }
  };
  walk(document);

  for (const {root, el} of banners) {
    extractFrom(root, el);
    if (ret.found) return ret;
  }

  //
