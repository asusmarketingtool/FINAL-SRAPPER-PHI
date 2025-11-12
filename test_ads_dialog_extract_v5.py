#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# test_ads_dialog_extract_v5.py
# ------------------------------------------------------------
# Extrae solo el POP-UP (ads_dialog) en ASUS y ROG
# para los países PE, CL, CO
# Acepta cookies automáticamente (botón "Aceptar todas")
# y guarda en Google Sheet con las columnas solicitadas.
# ------------------------------------------------------------

import re
import time
import json
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, urlencode

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# ============================================================
# CONFIG
# ============================================================
COUNTRIES = ["CL", "PE", "CO"]
SITES = ["asus", "rog"]

GOOGLE_SHEET_ID = "1jVd25vYzU6ygqTEwbwYXJtEHD-ya8V4RrRTdNFkLr_A"
WORKSHEET_TITLE = "EXTRACT_ADS_DIALOG"
HEADERS = ["timestamp", "COUNTRY", "WEB", "ITEM", "HTML_SLOT", "GA4 SLOT", "ELEMENTS", "TEXT", "IMAGE", "URL"]

# ============================================================
# GOOGLE SHEETS AUTH
# ============================================================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    sa_json = None
    sa_env = None
    try:
        sa_env = json.loads(os.getenv("GCP_SA_JSON", ""))
    except Exception:
        import base64
        try:
            sa_env = json.loads(base64.b64decode(os.getenv("GCP_SA_JSON", "")).decode("utf-8"))
        except Exception:
            raise ValueError("⚠️ No se pudo leer GCP_SA_JSON")
    creds = Credentials.from_service_account_info(sa_env, scopes=scopes)
    return gspread.authorize(creds)

# ============================================================
# HELPERS
# ============================================================
def today_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def cache_bust(url):
    ts = int(time.time() * 1000)
    parts = list(urlsplit(url))
    q = parts[3]
    parts[3] = (q + "&" if q else "") + urlencode({"_cb": ts})
    return urlunsplit(parts)

def accept_cookies(page):
    sels = [
        "button:has-text('Aceptar todas')",
        "button:has-text('Aceptar')",
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
        "text=Aceptar todas",
        "text=Aceptar",
    ]
    for sel in sels:
        try:
            btn = page.locator(sel).first
            if btn and btn.count() > 0 and btn.is_visible():
                btn.click(timeout=2000)
                page.wait_for_timeout(800)
                print("✅ Cookies aceptadas.")
                return
        except Exception:
            pass
    print("⚠️ No se encontró botón de cookies (continuando).")

def find_popup(page):
    sel = ".PB_promotionBanner.PB_corner.PB_promotionMode, #ads_dialog"
    try:
        pb = page.query_selector(sel)
        if pb:
            return pb.query_selector(".PB_body") or pb
    except Exception:
        pass
    # iframes fallback
    try:
        for fr in page.frames:
            if fr == page.main_frame:
                continue
            pb = fr.query_selector(sel)
            if pb:
                return pb.query_selector(".PB_body") or pb
    except Exception:
        pass
    return None

def extract_popup_data(pb):
    title, image, url = "", "", ""
    try:
        t = pb.query_selector(".PB_title")
        if t:
            title = (t.text_content() or "").strip()
    except Exception:
        pass
    try:
        img = pb.query_selector("img")
        if img:
            image = img.get_attribute("src") or ""
    except Exception:
        pass
    try:
        btn = pb.query_selector("a.PB_button")
        if btn:
            url = (btn.get_attribute("href") or "").strip()
    except Exception:
        pass
    return title, image, url

# ============================================================
# WRITE TO SHEETS
# ============================================================
def write_to_sheets(rows):
    gc = get_gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(WORKSHEET_TITLE, rows=100, cols=len(HEADERS))
    ws.clear()
    ws.append_row(HEADERS)
    ws.append_rows(rows)
    print(f"✅ Datos subidos correctamente: {len(rows)} filas.")

# ============================================================
# MAIN SCRAPER
# ============================================================
def run():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1600, "height": 900},
            ignore_https_errors=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
        )
        for country in COUNTRIES:
            for site in SITES:
                url = f"https://{'www' if site=='asus' else 'rog'}.asus.com/{country.lower()}/"
                print(f"🌍 [{country}] {site.upper()} → {url}")
                page = context.new_page()
                page.goto(cache_bust(url), wait_until="domcontentloaded", timeout=60000)
                accept_cookies(page)
                page.wait_for_timeout(8000)
                pb = find_popup(page)
                if pb:
                    title, image, link = extract_popup_data(pb)
                    print(f"✅ [{country}-{site}] Popup encontrado: {title}")
                    results.append([
                        today_str(),
                        country,
                        site.upper(),
                        "E-SHOP HOME POP UP",
                        "PB_type_lowerRightCorner",
                        "ads_dialog",
                        "1",
                        title,
                        image,
                        link,
                    ])
                else:
                    print(f"❌ [{country}-{site}] No se encontró popup.")
                page.close()
        context.close()
        browser.close()
    if results:
        write_to_sheets(results)
    else:
        print("⚠️ No se obtuvieron datos.")

if __name__ == "__main__":
    run()
