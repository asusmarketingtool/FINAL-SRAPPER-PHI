#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# campaign_extract_to_sheets.py  (versión robusta con safe_goto)
#
# >>> MODIFICADO: agrega soporte para VIDEO BANNER <<<
#   - Lee el tab "Video Banner" del spreadsheet del scraper (1a6B41...)
#     con columnas: Fecha | País | BannerName | IMG | URL | VideoURL
#   - Hace upsert en EXTRACT_LIM con el MISMO esquema de los demás banners:
#       ITEM      = "VIDEO BANNER"
#       GA4_SLOT  = video_banner_<posición>
#       IMAGE_URL = IMG
#       URL       = URL (landing)
#       VIDEO_URL = VideoURL  (columna NUEVA al final de EXTRACT_LIM)
#   - Países: CL / CO / PE (toma la última Fecha disponible por país)
#
#   Cambios marcados con:  # === VIDEO BANNER ADDITION ===
#
# REQUISITO (una sola vez): compartir el spreadsheet del scraper
#   (1a6B41V05SJsuI2AVf7zneEuguXrRt5q1KoIL6zLZnUY) como LECTOR con el email
#   del Service Account (se imprime en consola al correr: "[INFO] Service Account: ...").
# ------------------------------------------------------------
#
# Países: PE/CL/CO (multipaís) → Hoja: EXTRACT_LIM
# SOLO ITEMS:
#   E-SHOP HOME POP UP ASUS.com      -> GA4: ads_dialog
#   PROMOTIONAL SLIM BANNER HOME     -> GA4: index_bar_banner_1
#   HOME BANNER ASUS.com             -> GA4: hero_banner_#
#   COLUMN BANNER                    -> GA4: column_banner_#
#   E-SHOP HOME POP UP ROG.com       -> GA4: ads_dialog
#   HOME BANNER ROG.com              -> GA4: hero_banner_#
#   BANNER PROMOTIONAL ROG.com       -> GA4: index_bar_banner_1
#   DEALS PAGE TAB                   -> GA4: pending
#   STORE PROMOTION BANNER           -> GA4: store_bar_banner_1
#   STORE BANNER                     -> GA4: store_home_hero_banner_1 (desde 2025-01-29)
#   STORE TABS                       -> GA4: pending
#   NEWS AND PROMOTIONS              -> GA4: store_home_card_banner_#
#   VIDEO BANNER                     -> GA4: video_banner_#   (NUEVO)
# ------------------------------------------------------------
import re
import csv
import time
import os
import json
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import (
    sync_playwright,
    Locator,
    TimeoutError as PlaywrightTimeout,
)
from urllib.parse import urlsplit, urlunsplit, urlencode
# =========================
# CONFIG
# =========================
COUNTRIES = ["PE", "CL", "CO"]  # se ejecutan SIEMPRE los 3
COUNTRY = "PE"
COUNTRY_PATH = "pe"
# ← ACTUALIZADO (ID nuevo)
GOOGLE_SHEET_ID = "1jVd25vYzU6ygqTEwbwYXJtEHD-ya8V4RrRTdNFkLr_A"
WORKSHEET_TITLE = "EXTRACT_LIM"

# === VIDEO BANNER ADDITION ===
# Spreadsheet del scraper (Apps Script) que ya genera el tab "Video Banner".
VIDEO_BANNER_SHEET_ID = "1a6B41V05SJsuI2AVf7zneEuguXrRt5q1KoIL6zLZnUY"
VIDEO_BANNER_WS_TITLE = "Video Banner"
VIDEO_BANNER_ITEM = "VIDEO BANNER"
VIDEO_BANNER_HTML_SLOT = "VideoBanner"
# Países que queremos volcar en EXTRACT_LIM para el componente del PHI.
VIDEO_BANNER_COUNTRIES = ["CL", "CO", "PE"]
# === END VIDEO BANNER ADDITION ===

# En GitHub Actions se usa GCP_SA_JSON; el path local es fallback solo si corres local.
SERVICE_ACCOUNT_JSON = r"C:\Users\eugenia_neira\OneDrive - ASUS\CODE BUDDY\Python\SITE SCRAPPER\site-scrapper-473615-a2f1587be280.json"
HEADLESS = True
# Antes: 45000 → se quedaba corto en /store/. Igual, ahora capturamos timeout.
NAV_TIMEOUT = 70000  # 70s
WAIT_MS = 1800
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/119.0.0.0 Safari/537.36"
)
WEB_ASUS = f"www.asus.com/{COUNTRY_PATH}/"
WEB_ROG  = f"rog.asus.com/{COUNTRY_PATH}/"
URLS = {
    "home_asus": f"https://{WEB_ASUS}",
    "home_rog":  f"https://{WEB_ROG}",
    "deals_all": f"https://www.asus.com/{COUNTRY_PATH}/deals/all-deals/",
    "store":     f"https://www.asus.com/{COUNTRY_PATH}/store/",
}
FALLBACK_CSV = "campaign_extract_fallback_lim.csv"
# =========================
# HEADERS (agregamos GA4_SLOT después de HTML_SLOT)
# === VIDEO BANNER ADDITION ===  -> se agrega "VIDEO_URL" como ÚLTIMA columna.
#     Se agrega al final para NO desalinear las columnas existentes (escritura posicional).
# =========================
HEADERS = [
    "DATE", "COUNTRY", "WEB", "ITEM", "HTML_SLOT", "GA4_SLOT", "ELEMENTS",
    "TEXT", "IMAGE_URL", "URL", "PRODUCT_NAME", "PRODUCT_PRICE", "POSITION",
    "VIDEO_URL",  # <- NUEVA
]
def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")
# =========================
# GA4 SLOT mapping
# =========================
# Fecha de corte para el cambio de nombre del slot STORE BANNER
_STORE_BANNER_CUTOFF = datetime(2025, 1, 29).date()
def ga4_slot_for(item: str, position: int) -> str:
    i = (item or "").strip().lower()
    if i == "e-shop home pop up asus.com": return "ads_dialog"
    if i == "e-shop home pop up rog.com":  return "ads_dialog"
    if i == "banner promotional rog.com":  return "index_bar_banner_1"
    if i == "promotional slim banner home": return "index_bar_banner_1"
    if i == "store promotion banner":      return "store_bar_banner_1"
    if i == "store banner":
        # Desde el 29-ene-2025 el slot pasó a llamarse store_home_hero_banner_1
        return "store_home_hero_banner_1" if datetime.now().date() >= _STORE_BANNER_CUTOFF else "store_home_1"
    if i == "store tabs":                  return "pending"
    if i == "home banner asus.com":        return f"hero_banner_{position if position>0 else 1}"
    if i == "home banner rog.com":         return f"hero_banner_{position if position>0 else 1}"
    if i == "column banner":               return f"column_banner_{position if position>0 else 1}"
    if i == "news and promotions":         return f"store_home_card_banner_{position if position>0 else 1}"
    # === VIDEO BANNER ADDITION ===
    if i == "video banner":                return f"video_banner_{position if position>0 else 1}"
    # === END VIDEO BANNER ADDITION ===
    return "pending"
# =========================
# add_row centralizado (incluye GA4_SLOT y VIDEO_URL)
# =========================
def add_row(
    rows: List[Dict[str, str]],
    country: str,
    web: str,
    item: str,
    html_slot: str,
    elements: str,
    text: str,
    image_url: str,
    url: str,
    position: int,
    product_name: str = "",
    product_price: str = "",
    video_url: str = "",   # === VIDEO BANNER ADDITION ===
):
    ga4_slot = ga4_slot_for(item, position)
    r = {
        "DATE": today_str(),
        "COUNTRY": country,
        "WEB": web,
        "ITEM": item,
        "HTML_SLOT": html_slot,
        "GA4_SLOT": ga4_slot,
        "ELEMENTS": elements,
        "TEXT": (text or "").strip(),
        "IMAGE_URL": (image_url or "").strip(),
        "URL": (url or "").strip(),
        "PRODUCT_NAME": (product_name or "").strip(),
        "PRODUCT_PRICE": (product_price or "").strip(),
        "POSITION": str(position),
        "VIDEO_URL": (video_url or "").strip(),   # === VIDEO BANNER ADDITION ===
    }
    if r["URL"].lower().startswith("javascript:") or r["URL"] in ("#", "##"):
        r["URL"] = ""
    rows.append(r)
# =========================
# No-cache & helpers
# =========================
def cache_bust(u: str) -> str:
    if not u:
        return u
    ts = int(time.time() * 1000)
    parts = list(urlsplit(u))
    q = parts[3]
    parts[3] = (q + "&" if q else "") + urlencode({"_cb": ts})
    return urlunsplit(parts)
def safe_goto(page, url: str, label: str) -> bool:
    """
    Navega a la URL con cache-buster y captura Timeout.
    Devuelve True si cargó, False si hubo timeout.
    """
    full = cache_bust(url)
    print(f"[NAV] {COUNTRY} {label} → {full}")
    try:
        page.goto(full, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        return True
    except PlaywrightTimeout:
        print(f"[TIMEOUT] {COUNTRY} {label} no cargó en {NAV_TIMEOUT}ms: {full}")
        return False
def absolutize_from_web(web_host: str, url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("//"):
        return "https:" + url
    host = web_host.split("/")[0]
    if not url.startswith("/"):
        url = "/" + url
    return "https://" + host + url
# =========================
# Imagen & link helpers
# =========================
_BG_URL_RE  = re.compile(r'url\(["\']?([^"\')]+)["\']?\)', re.I)
def _choose_from_srcset(srcset_value: str) -> Optional[str]:
    parts = [p.strip() for p in (srcset_value or "").split(",") if p.strip()]
    if not parts:
        return None
    preferred = None
    for p in parts:
        url = p.split()[0]
        if ("dlcdnwebimgs.asus.com" in url or "/fwebp" in url) and p.endswith(" 1x"):
            return url
        if "dlcdnwebimgs.asus.com" in url or "/fwebp" in url:
            preferred = preferred or url
    if preferred:
        return preferred
    for p in parts:
        if p.endswith(" 1x"):
            return p.split()[0]
    return parts[0].split()[0]
def _extract_onclick_href(s: str) -> str:
    if not s: return ""
    m = re.search(r"(?:location\.href|window\.open)\s*\(\s*['\"]([^'\"]+)['\"]", s)
    if m:
        return m.group(1)
    m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", s)
    return m.group(1) if m else ""
def _sanitize_link(link: str) -> str:
    if not link: return ""
    l = link.strip()
    if l.lower().startswith("javascript:"): return ""
    if l in ("#", "##"): return ""
    return l
def pick_best_image_from_picture_el(pic_eh, base_url: str) -> Optional[str]:
    if not pic_eh: return None
    def get_attr(el, name: str) -> str:
        try: return el.get_attribute(name) or ""
        except Exception: return ""
    try:
        sources = pic_eh.query_selector_all("source") or []
        for s in sources:
            media = (get_attr(s,"media") or "").lower()
            if "min-width" in media and "1280" in media:
                srcset = get_attr(s,"srcset")
                if srcset:
                    pick = _choose_from_srcset(srcset)
                    if pick: return pick if pick.startswith("http") else absolutize_from_web(WEB_ASUS, pick)
        for s in sources:
            srcset = get_attr(s,"srcset")
            if srcset:
                pick = _choose_from_srcset(srcset)
                if pick: return pick if pick.startswith("http") else absolutize_from_web(WEB_ASUS, pick)
        img = pic_eh.query_selector("img")
        if img:
            src = get_attr(img,"src")
            if src: return src if src.startswith("http") else absolutize_from_web(WEB_ASUS, src)
    except Exception:
        pass
    return None
def _get_img_from_node(page_el, base_url: str) -> str:
    try:
        src_info = page_el.evaluate("""
        (el)=>{
          const s = Array.from(el.querySelectorAll ? el.querySelectorAll('source[srcset]') : []);
          const img = (el.matches && el.matches('img')) ? el : el.querySelector && el.querySelector('img');
          const imgsrc = img ? (img.getAttribute('src') || img.getAttribute('data-src') || img.getAttribute('data-original') || img.getAttribute('data-lazy') || '') : '';
          const bg = (window.getComputedStyle ? getComputedStyle(el).backgroundImage : '') || '';
          return {sources: s.map(x=>x.getAttribute('srcset')||''), imgsrc, bg};
        }""")
        if src_info and isinstance(src_info, dict):
            srcsets = src_info.get("sources") or []
            for ss in srcsets:
                pick = _choose_from_srcset(ss)
                if pick and ("dlcdnwebimgs.asus.com" in pick or "/fwebp" in pick):
                    return pick if pick.startswith("http") else absolutize_from_web(WEB_ASUS, pick)
            for ss in srcsets:
                pick = _choose_from_srcset(ss)
                if pick:
                    return pick if pick.startswith("http") else absolutize_from_web(WEB_ASUS, pick)
            if src_info.get("imgsrc"):
                src = src_info["imgsrc"]
                if src: return src if src.startswith("http") else absolutize_from_web(WEB_ASUS, src)
            bg = src_info.get("bg") or ""
            if isinstance(bg, str) and bg and bg.lower() != "none":
                m = _BG_URL_RE.search(bg)
                if m:
                    u = m.group(1)
                    return u if u.startswith("http") else absolutize_from_web(base_url, u)
    except Exception:
        pass
    return ""
def _get_link_from_node(page_el, base_url: str) -> str:
    try:
        a = page_el.evaluate_handle("el => el.closest && el.closest('a[href]')")
        href = a.evaluate("el => el ? el.getAttribute('href') : ''")
        if href: return absolutize_from_web(WEB_ASUS, _sanitize_link(href))
    except Exception:
        pass
    try:
        href = page_el.evaluate("""
        (el)=> {
          const keys=["data-url","data-href","data-link","data-destination","data-ga-link","data-target","data-to","onclick","onClick"];
          let n=el;
          for (let i=0;i<8 && n;i++){
            for (const k of keys){
              const v=n.getAttribute && n.getAttribute(k);
              if (!v) continue;
              if (k.startsWith('data-')) return String(v);
              if (k.toLowerCase()==='onclick'){ return String(v); }
            }
            const a=n.querySelector && n.querySelector('a[href]'); if (a) return a.getAttribute('href');
            n=n.parentElement;
          }
          return "";
        }""")
        if href:
            if "location" in href or "window.open" in href:
                href = _extract_onclick_href(href)
            return absolutize_from_web(WEB_ASUS, _sanitize_link(href))
    except Exception:
        pass
    try:
        href = page_el.evaluate("(el)=>{const a=el.querySelector && el.querySelector('a[href]');return a?a.getAttribute('href'):''}")
        if href: return absolutize_from_web(WEB_ASUS, _sanitize_link(href))
    except Exception:
        pass
    try:
        val = page_el.evaluate("""(el)=> {
            const btn = el.query_selector && el.querySelector('button[onclick], [role="button"][onclick]');
            return btn ? btn.getAttribute('onclick') : '';
        }""")
        if val:
            href = _extract_onclick_href(val)
            if href: return absolutize_from_web(WEB_ASUS, _sanitize_link(href))
    except Exception:
        pass
    try:
        act = page_el.evaluate("(el)=>{const f=el.querySelector && el.querySelector('form[action]');return f?f.getAttribute('action'):''}")
        if act: return absolutize_from_web(WEB_ASUS, _sanitize_link(act))
    except Exception:
        pass
    return ""
def safe_text_from_locator(page, locator) -> str:
    try:
        if not locator or locator.count() == 0: return ""
        t = locator.inner_text(timeout=1200) or locator.text_content(timeout=1200) or ""
        return re.sub(r"\s+", " ", t).strip()
    except Exception:
        return ""
def robust_href_from_locator(page, locator) -> str:
    try:
        if not locator or locator.count() == 0: return ""
        href = locator.get_attribute("href") or ""
        if href: return _sanitize_link(href)
        oc = locator.get_attribute("onclick") or locator.get_attribute("onClick") or ""
        if oc:   return _sanitize_link(_extract_onclick_href(oc))
        a = locator.locator("a[href]").first
        if a and a.count() > 0:
            h = a.get_attribute("href") or ""
            return _sanitize_link(h)
        f = locator.locator("form[action]").first
        if f and f.count() > 0:
            h = f.get_attribute("action") or ""
            return _sanitize_link(h)
    except Exception:
        pass
    return ""
def ensure_visible(page, locator):
    try:
        if locator and locator.count() > 0:
            eh = locator.element_handle(timeout=800)
            if eh: page.evaluate("(el)=>el.scrollIntoView({block:'center'})", eh)
    except Exception:
        pass
# =========================
# HERO base
# =========================
HERO_SLOTS = 6
SEL_HERO_WRAPPERS = "#heroBanner, #liBanner, [id*='hero'][class*='Banner'], [class*='Hero'][class*='Banner']"
SEL_HERO_SLIDES   = ".swiper-slide, .slick-slide, [role='tabpanel'][id*='Slide'], [data-swiper-slide-index]"
def scrape_hero(page, base_url: str) -> List[Tuple[str,str]]:
    out: List[Tuple[str,str]] = []
    try:
        page.wait_for_selector(f"{SEL_HERO_WRAPPERS} {SEL_HERO_SLIDES}, #liBanner, #heroBanner", timeout=12000)
    except Exception:
        pass
    slides = []
    for sel in (f"{SEL_HERO_WRAPPERS} {SEL_HERO_SLIDES}", SEL_HERO_SLIDES):
        try:
            got = page.query_selector_all(sel) or []
            if got: slides = got; break
        except Exception:
            continue
    def slide_key(el) -> Tuple[int, str]:
        idx = -1
        try:
            val = el.get_attribute("data-swiper-slide-index")
            if val and val.isdigit(): idx = int(val)
        except Exception:
            pass
        try:
            cls = el.get_attribute("class") or ""
        except Exception:
            cls = ""
        return (idx, cls)
    if slides:
        slides_with_idx = []
        for i, el in enumerate(slides):
            k = slide_key(el)
            slides_with_idx.append((k[0] if k[0] >= 0 else i, el))
        slides_with_idx.sort(key=lambda x: x[0])
        ordered = [el for _, el in slides_with_idx]
    else:
        ordered = []
    for el in ordered:
        img = _get_img_from_node(el, base_url)
        if not img:
            target = el.query_selector("picture") or el.query_selector("img") or el
            img = _get_img_from_node(target, base_url)
        link = _get_link_from_node(el, base_url)
        pair = (img or "", link or "")
        if pair not in out and (img or link):
            out.append(pair)
        if len(out) >= HERO_SLOTS:
            break
    if len(out) < HERO_SLOTS:
        try:
            pics = page.query_selector_all("#heroBanner picture, #liBanner picture, picture") or []
        except Exception:
            pics = []
        for el in pics:
            img = _get_img_from_node(el, base_url)
            link = _get_link_from_node(el, base_url)
            pair = (img or "", link or "")
            if pair not in out and (img or link): out.append(pair)
            if len(out) >= HERO_SLOTS: break
    while len(out) < HERO_SLOTS: out.append(("", ""))
    return out[:HERO_SLOTS]
# =========================
# EXTRACTORES (solo los ITEMS requeridos)
# =========================
# 1) POPUP ASUS/ROG — SOLO TÍTULO, IMAGEN, URL (NO texto del botón)
def extract_home_popup(
    page,
    home_url: str,
    rows: List[Dict[str, str]],
    web_label: str,
    default_text: Optional[str] = None,
    default_img: Optional[str] = None,
):
    html_slot = "PB_type_lowerRightCorner"
    item_lbl = "E-SHOP HOME POP UP ASUS.com" if web_label.startswith("www.asus.com") else "E-SHOP HOME POP UP ROG.com"
    if not safe_goto(page, home_url, f"HOME POPUP {web_label}"):
        # Si hay timeout, registramos fila y seguimos
        add_row(rows, COUNTRY, web_label, item_lbl, html_slot, "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(3500)
    popup = page.locator(".PB_promotionBanner.PB_corner.PB_promotionMode").first
    if popup.count() == 0 or not popup.is_visible():
        add_row(rows, COUNTRY, web_label, item_lbl, html_slot, "0", "No visible", "", "", 0)
        return
    body = popup.locator(".PB_body").first
    # Título
    title = ""
    if body and body.count() > 0:
        try:
            title = safe_text_from_locator(page, body.locator(".PB_title").first) or ""
        except Exception:
            title = ""
    # Imagen
    img_src = ""
    pic = body.locator(".PB_picture picture").first if body and body.count() > 0 else popup.locator(".PB_picture picture").first
    if pic and pic.count() > 0:
        try:
            eh = pic.element_handle()
            if eh: img_src = pick_best_image_from_picture_el(eh, home_url) or ""
        except Exception:
            pass
    if not img_src:
        try:
            img = body.locator(".PB_picture img").first if body and body.count() > 0 else popup.locator(".PB_picture img").first
            if img and img.count() > 0:
                s = img.get_attribute("src") or ""
                if s: img_src = s if s.startswith("http") else absolutize_from_web(web_label, s)
        except Exception:
            pass
    # URL del botón
    href = ""
    try:
        btn = body.locator("a.PB_button").first if body and body.count() > 0 else popup.locator("a.PB_button").first
        if btn and btn.count() > 0:
            raw = btn.get_attribute("href") or ""
            if raw: href = raw if raw.startswith("http") else absolutize_from_web(web_label, raw)
    except Exception:
        pass
    # Fallbacks ASUS
    if web_label.startswith("www.asus.com"):
        if (not title) and default_text:
            title = default_text
        if (not img_src) and default_img:
            img_src = default_img
    add_row(rows, COUNTRY, web_label, item_lbl, html_slot, "1", title, img_src, href, 1)
# 2) PROMOTIONAL SLIM BANNER HOME (ASUS)
def extract_promotional_slim_banner(page, home_url: str, rows: List[Dict[str, str]]):
    item_lbl = "PROMOTIONAL SLIM BANNER HOME"
    if not safe_goto(page, home_url, "PROMOTIONAL SLIM BANNER HOME"):
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl, "PromotionBanner__swiperContainer__", "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    swiper = page.locator("[class^='PromotionBanner__swiperContainer__']").first
    if swiper and swiper.count() > 0:
        slides = swiper.locator(".swiper-slide")
        n = slides.count() or 1
        for i in range(n):
            slide = slides.nth(i) if slides.count() > 0 else swiper
            link = slide.locator("a").first
            href = absolutize_from_web(WEB_ASUS, robust_href_from_locator(page, link))
            text_block = slide.locator(".PromotionBanner__text__, .PromotionBanner__text__1HGpW").first
            text = safe_text_from_locator(page, text_block) if text_block and text_block.count() > 0 else ""
            pic = slide.locator("picture").first
            img_src = ""
            if pic and pic.count() > 0:
                eh = pic.element_handle()
                if eh: img_src = pick_best_image_from_picture_el(eh, home_url) or ""
            if not img_src:
                img = slide.locator("img").first
                if img and img.count() > 0:
                    s = img.get_attribute("src") or ""
                    if s: img_src = s if s.startswith("http") else absolutize_from_web(WEB_ASUS, s)
            add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                    "PromotionBanner__swiperContainer__", str(n), text, img_src, href, i+1)
# 3) HOME HERO (ASUS/ROG)
def extract_home_hero_all(page, home_url: str, rows: List[Dict[str, str]], web_label: str):
    item_lbl = "HOME BANNER ASUS.com" if web_label.startswith("www.asus.com") else "HOME BANNER ROG.com"
    if not safe_goto(page, home_url, f"HOME HERO {web_label}"):
        add_row(rows, COUNTRY, web_label, item_lbl, "#heroBanner", "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    pairs = scrape_hero(page, home_url)
    total = len(pairs)
    pos = 0
    for (img, ln) in pairs:
        if not (img or ln):
            continue
        pos += 1
        add_row(rows, COUNTRY, web_label, item_lbl, "#heroBanner", str(total), "", img, ln, pos)
# 4) COLUMN BANNER (ASUS)
SEL_COLUMN_CARDS = (
    ".ColumnBanner__colBannerCard__, .ColumnBanner__colBannerCard__3FBSI, "
    "[class*='ColumnBanner'] [class*='colBanner'], [class*='column'] [class*='banner']"
)
COLUMN_POSITIONS_GA = [1, 2, 3, 4, 5, 6]
def extract_column_banners(page, home_url: str, rows: List[Dict[str, str]]):
    item_lbl = "COLUMN BANNER"
    if not safe_goto(page, home_url, "COLUMN BANNERS"):
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl, "ColumnBanner__colBannerCard__", "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    try:
        cards = page.query_selector_all(SEL_COLUMN_CARDS) or []
    except Exception:
        cards = []
    total = min(len(cards), len(COLUMN_POSITIONS_GA))
    for i in range(total):
        card = cards[i]
        target = card.query_selector("picture") or card.query_selector("img") or card
        img = _get_img_from_node(target, home_url)
        ln  = _get_link_from_node(card, home_url) or _get_link_from_node(target, home_url)
        if img or ln:
            add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                    "ColumnBanner__colBannerCard__", str(total), "", img, ln, i+1)
# 5) BANNER PROMOTIONAL ROG.com
def extract_rog_promo_banner(page, home_url: str, rows: List[Dict[str, str]]):
    item_lbl = "BANNER PROMOTIONAL ROG.com"
    if not safe_goto(page, home_url, "ROG PROMO BANNER"):
        # si no carga, simplemente no hay fila (este banner es opcional)
        return
    page.wait_for_timeout(900)
    body = page.locator("[class^='BannerPromotionBar__bannerPromotionBarBody__']").first
    if not body or body.count()==0 or not body.is_visible():
        return
    text = safe_text_from_locator(page, body) or ""
    href = robust_href_from_locator(page, body) or ""
    href = absolutize_from_web(WEB_ROG, href)
    add_row(rows, COUNTRY, WEB_ROG, item_lbl,
            "BannerPromotionBar__bannerPromotionBarBody__", "1", text, "", href, 1)
# 6) DEALS PAGE TAB (ASUS)
def extract_deals_tabs(page, deals_url: str, rows: List[Dict[str, str]]):
    item_lbl = "DEALS PAGE TAB"
    if not safe_goto(page, deals_url, "DEALS PAGE"):
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                ".DealsPage__swiperWrapper__1GwMv > a", "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    tabs = page.locator(".DealsPage__swiperWrapper__1GwMv > a")
    n = tabs.count()
    if n == 0:
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                ".DealsPage__swiperWrapper__1GwMv > a", "0", "No se encontraron tabs", "", "", 0)
        return
    for i in range(n):
        a = tabs.nth(i)
        href = absolutize_from_web(WEB_ASUS, robust_href_from_locator(page, a))
        text = safe_text_from_locator(page, a.locator(".DealsPage__tabText__2EAxm span").first) or safe_text_from_locator(page, a)
        pic = a.locator(".DealsPage__tabImageBox__eTIp7 picture").first
        img_src = ""
        if pic and pic.count() > 0:
            eh = pic.element_handle()
            if eh: img_src = pick_best_image_from_picture_el(eh, deals_url) or ""
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                ".DealsPage__swiperWrapper__1GwMv", str(n), text, img_src, href, i+1)
# 7) STORE PROMOTION BANNER (ASUS)
def extract_store_promotion_banner(page, store_url: str, rows: List[Dict[str, str]]):
    item_lbl = "STORE PROMOTION BANNER"
    if not safe_goto(page, store_url, "STORE PROMOTION BANNER"):
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "StorePromotionBanner__slideContent__", "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    v1 = page.locator("[class^='StorePromotionBanner__slideContent__']").first
    if v1 and v1.count()>0:
        slides = page.locator("[class^='StorePromotionBanner__slideContent__']")
        n = slides.count()
        for i in range(n):
            s = slides.nth(i)
            link = s.locator("a").first
            href = absolutize_from_web(WEB_ASUS, robust_href_from_locator(page, link))
            text = safe_text_from_locator(page, s)
            pic = s.locator("picture").first
            img_src = ""
            if pic and pic.count()>0:
                eh = pic.element_handle()
                if eh: img_src = pick_best_image_from_picture_el(eh, store_url) or ""
            if not img_src:
                img = s.locator("img").first
                if img and img.count()>0:
                    ss = img.get_attribute("src") or ""
                    if ss: img_src = ss if ss.startswith("http") else absolutize_from_web(WEB_ASUS, ss)
            add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                    "StorePromotionBanner__slideContent__", str(n), text, img_src, href, i+1)
# 8) STORE BANNER (store_home_hero_banner_1 desde 2025-01-29) — ASUS
def extract_store_banner_home1(page, store_url: str, rows: List[Dict[str, str]]):
    item_lbl = "STORE BANNER"
    if not safe_goto(page, store_url, "STORE BANNER HOME1"):
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "store_home_hero_banner_1 (SlimBanner__item__1V1hw)", "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    first_item = page.locator("a.SlimBanner__item__1V1hw").first
    if first_item and first_item.count()>0:
        href = absolutize_from_web(WEB_ASUS, robust_href_from_locator(page, first_item))
        pic = first_item.locator("picture").first
        img_src = ""
        if pic and pic.count()>0:
            eh = pic.element_handle()
            if eh: img_src = pick_best_image_from_picture_el(eh, store_url) or ""
        if not img_src:
            img = first_item.locator("img").first
            if img and img.count()>0:
                s = img.get_attribute("src") or ""
                if s: img_src = s if s.startswith("http") else absolutize_from_web(WEB_ASUS, s)
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "store_home_hero_banner_1 (SlimBanner__item__1V1hw)", "1", "", img_src, href, 1)
    else:
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "store_home_hero_banner_1 (SlimBanner__item__1V1hw)", "0", "No visible", "", "", 0)
# 9) STORE TABS (ASUS)
def extract_store_tabs(page, store_url: str, rows: List[Dict[str, str]]):
    item_lbl = "STORE TABS"
    if not safe_goto(page, store_url, "STORE TABS"):
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "AllStore__sectionWrapper__2n7Ha > .AllStore__swiperWrapper__1uYYw",
                "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    tabs = page.locator(".AllStore__swiperWrapper__1uYYw > a")
    n = tabs.count()
    if n == 0:
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "AllStore__sectionWrapper__2n7Ha > .AllStore__swiperWrapper__1uYYw",
                "0", "No se encontraron tabs", "", "", 0)
        return
    for i in range(n):
        a = tabs.nth(i)
        href = absolutize_from_web(WEB_ASUS, robust_href_from_locator(page, a))
        text = (safe_text_from_locator(page, a.locator(".AllStore__tabText__3i5DV span").first)
                or safe_text_from_locator(page, a)).strip()
        pic = a.locator(".AllStore__tabImageBox__3PkVC picture").first
        img_src = ""
        if pic and pic.count()>0:
            eh = pic.element_handle()
            if eh: img_src = pick_best_image_from_picture_el(eh, store_url) or ""
        if not img_src:
            img = a.locator(".AllStore__tabImageBox__3PkVC img").first
            if img and img.count()>0:
                s = img.get_attribute("src") or ""
                if s: img_src = s if s.startswith("http") else absolutize_from_web(WEB_ASUS, s)
        if text and img_src and href:
            add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                    "AllStore__sectionWrapper__2n7Ha > .AllStore__swiperWrapper__1uYYw",
                    str(n), text, img_src, href, i+1)
# 10) NEWS AND PROMOTIONS (ASUS Store)
def extract_news_promotions(page, store_url: str, rows: List[Dict[str, str]]):
    item_lbl = "NEWS AND PROMOTIONS"
    if not safe_goto(page, store_url, "NEWS AND PROMOTIONS"):
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "AllStore__storeNewsWrapper__", "0", "Timeout cargando página", "", "", 0)
        return
    page.wait_for_timeout(WAIT_MS)
    section = page.locator("[class^='AllStore__storeNewsWrapper__']").first
    if section.count()==0:
        section = page.locator(
            "xpath=//h2[contains(normalize-space(),'Noticias') or contains(normalize-space(),'Promociones')]/ancestor::div[contains(@class,'AllStore__sectionWrapper__') or contains(@class,'AllStore__storeNewsWrapper__')][1]"
        ).first
    if section.count()==0:
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "AllStore__storeNewsWrapper__", "0", "Sección no encontrada", "", "", 0)
        return
    next_btn = section.locator("[class*='swiper-button-next']").first
    seen: Set[str] = set()
    cards_collected: List[Locator] = []
    def capture_once():
        nonlocal seen, cards_collected
        cards = section.locator("a[class^='PromotionCard__promotionCard__']")
        c = cards.count()
        for i in range(c):
            a = cards.nth(i)
            href = robust_href_from_locator(page, a)
            if not href:
                continue
            href_abs = absolutize_from_web(WEB_ASUS, href)
            if href_abs in seen:
                continue
            seen.add(href_abs)
            cards_collected.append(a)
    capture_once()
    turns = 0
    while next_btn and next_btn.count()>0 and turns < 80:
        try:
            if next_btn.is_disabled():
                break
        except Exception:
            pass
        try:
            next_btn.click(timeout=1200)
            page.wait_for_timeout(420)
            capture_once()
            turns += 1
        except Exception:
            break
    total = len(cards_collected)
    if total == 0:
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "AllStore__storeNewsWrapper__", "0", "Sin tarjetas", "", "", 0)
        return
    for idx, a in enumerate(cards_collected, start=1):
        href = absolutize_from_web(WEB_ASUS, robust_href_from_locator(page, a))
        img_src = ""
        pic = a.locator("picture").first
        if pic and pic.count()>0:
            eh = pic.element_handle()
            if eh: img_src = pick_best_image_from_picture_el(eh, store_url) or ""
        if not img_src:
            img = a.locator("img").first
            if img and img.count()>0:
                s = img.get_attribute("src") or ""
                if s: img_src = s if s.startswith("http") else absolutize_from_web(WEB_ASUS, s)
        add_row(rows, COUNTRY, WEB_ASUS, item_lbl,
                "AllStore__storeNewsWrapper__", str(total), "", img_src, href, idx)

# =========================
# === VIDEO BANNER ADDITION ===
# 11) VIDEO BANNER — NO scrapea el sitio: LEE el tab "Video Banner" del
#     spreadsheet del scraper (1a6B41...) que ya genera tu Apps Script, y
#     vuelca las filas en EXTRACT_LIM con el esquema del PHI.
# =========================
def _norm_key(s: str) -> str:
    """Normaliza encabezados: minúsculas, sin tildes/espacios/guiones."""
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    for a, b in (("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n")):
        s = s.replace(a, b)
    return re.sub(r"[\s_]+", "", s)

def _parse_fecha(v: str):
    """Intenta parsear varias fechas (dd/mm/yyyy, yyyy-mm-dd, etc.). Devuelve datetime o None."""
    v = (v or "").strip()
    if not v:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(v[:10], fmt)
        except Exception:
            continue
    return None

def extract_video_banner_from_scraper(gc, rows: List[Dict[str, str]]):
    """
    Lee VIDEO_BANNER_SHEET_ID -> tab "Video Banner" (Fecha|País|BannerName|IMG|URL|VideoURL),
    toma la ÚLTIMA fecha disponible por país (CL/CO/PE) y agrega filas a `rows`
    con ITEM='VIDEO BANNER' y GA4_SLOT='video_banner_N'.
    """
    global COUNTRY
    print(f"[VIDEO] Leyendo '{VIDEO_BANNER_WS_TITLE}' desde {VIDEO_BANNER_SHEET_ID} …")
    try:
        src_sh = _retry(gc.open_by_key, VIDEO_BANNER_SHEET_ID)
        src_ws = _retry(src_sh.worksheet, VIDEO_BANNER_WS_TITLE)
        values = _retry(src_ws.get_all_values) or []
    except gspread.WorksheetNotFound:
        print(f"[VIDEO][WARN] No existe el tab '{VIDEO_BANNER_WS_TITLE}'. Se omite Video Banner.")
        return
    except gspread.exceptions.APIError as e:
        print(f"[VIDEO][WARN] No se pudo abrir el sheet del scraper (¿compartido con el SA?): {e}")
        return
    except Exception as e:
        print(f"[VIDEO][WARN] Error leyendo Video Banner: {type(e).__name__}: {e}")
        return

    if len(values) < 2:
        print("[VIDEO][WARN] El tab 'Video Banner' no tiene datos. Se omite.")
        return

    header = values[0]
    hidx = {_norm_key(h): i for i, h in enumerate(header)}

    def col(*cands):
        for c in cands:
            k = _norm_key(c)
            if k in hidx:
                return hidx[k]
        return None

    c_fecha = col("Fecha", "Date")
    c_pais  = col("País", "Pais", "Country")
    c_name  = col("BannerName", "Banner", "Nombre")
    c_img   = col("IMG", "Image", "ImageUrl", "Imagen")
    c_url   = col("URL", "Link", "Landing")
    c_video = col("VideoURL", "Video", "VideoUrl")

    missing = [n for n, c in [("Fecha", c_fecha), ("País", c_pais), ("IMG", c_img),
                              ("URL", c_url), ("VideoURL", c_video)] if c is None]
    if missing:
        print(f"[VIDEO][WARN] Faltan columnas en 'Video Banner': {missing}. Se omite.")
        return

    # Agrupar por país y elegir la última fecha
    by_country: Dict[str, List[list]] = {}
    latest_by_country: Dict[str, datetime] = {}
    for r in values[1:]:
        def g(i):
            return r[i].strip() if (i is not None and i < len(r)) else ""
        pais = g(c_pais).upper()
        if pais not in VIDEO_BANNER_COUNTRIES:
            continue
        f = _parse_fecha(g(c_fecha))
        if f is None:
            continue
        prev = latest_by_country.get(pais)
        if prev is None or f > prev:
            latest_by_country[pais] = f
        by_country.setdefault(pais, []).append(r)

    total_added = 0
    for pais in VIDEO_BANNER_COUNTRIES:
        if pais not in latest_by_country:
            print(f"[VIDEO] Sin filas para {pais}.")
            continue
        last_f = latest_by_country[pais]
        # Filas SOLO de la última fecha de ese país
        sel = []
        for r in by_country.get(pais, []):
            def g(i):
                return r[i].strip() if (i is not None and i < len(r)) else ""
            if _parse_fecha(g(c_fecha)) == last_f:
                sel.append(r)
        n = len(sel)
        for pos, r in enumerate(sel, start=1):
            def g(i):
                return r[i].strip() if (i is not None and i < len(r)) else ""
            COUNTRY = pais  # para mensajes de log consistentes
            add_row(
                rows,
                country=pais,
                web=f"www.asus.com/{pais.lower()}/",
                item=VIDEO_BANNER_ITEM,
                html_slot=VIDEO_BANNER_HTML_SLOT,
                elements=str(n),
                text=g(c_name),
                image_url=g(c_img),
                url=g(c_url),
                position=pos,
                video_url=g(c_video),
            )
            total_added += 1
        print(f"[VIDEO] {pais}: {n} banner(s) de la fecha {last_f.strftime('%Y-%m-%d')}.")
    print(f"[VIDEO] Total filas Video Banner agregadas: {total_added}")
# === END VIDEO BANNER ADDITION ===

# =========================
# Sheets / CSV  — con reintentos + escritura determinística
# =========================
def get_gspread_client(json_path: str):
    """
    Lee credenciales desde:
    1) Variable de entorno GCP_SA_JSON (JSON directo o base64 del JSON)
    2) Archivo local (fallback)
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    sa_env = os.getenv("GCP_SA_JSON", "").strip()
    creds = None
    if sa_env:
        try:
            info = json.loads(sa_env)
        except json.JSONDecodeError:
            import base64
            try:
                decoded = base64.b64decode(sa_env).decode("utf-8")
                info = json.loads(decoded)
            except Exception as e:
                raise ValueError("Invalid JSON in GCP_SA_JSON") from e
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(json_path, scopes=scopes)
    return gspread.authorize(creds), creds.service_account_email
def _retry(fn, *args, **kwargs):
    max_attempts = 6
    delay = 1.0
    last_err = None
    for attempt in range(1, max_attempts+1):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if any(code in msg for code in [" 500"," 502"," 503"," 504"]):
                print(f"[WARN] Sheets API {msg.strip()} — intento {attempt}/{max_attempts}. Esperando {delay:.1f}s…")
                time.sleep(delay)
                delay *= 2
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            break
    if last_err:
        raise last_err
def _a1(col_idx: int, row_idx: int) -> str:
    # 1-indexed
    name = ""
    while col_idx:
        col_idx, rem = divmod(col_idx-1, 26)
        name = chr(65+rem) + name
    return f"{name}{row_idx}"
def append_or_upsert(sheet_id: str, ws_title: str, rows: List[Dict[str, str]]):
    gc, sa_email = get_gspread_client(SERVICE_ACCOUNT_JSON)
    print(f"[INFO] Service Account: {sa_email}  (comparte el Sheet con este email)")
    sh = _retry(gc.open_by_key, sheet_id)
    try:
        ws = _retry(sh.worksheet, ws_title)
    except gspread.WorksheetNotFound:
        ws = _retry(sh.add_worksheet, title=ws_title, rows=2000, cols=len(HEADERS)+2)
        _retry(ws.update, "A1:"+_a1(len(HEADERS),1), [HEADERS])
    values = _retry(ws.get_all_values) or []
    if not values:
        _retry(ws.update, "A1:"+_a1(len(HEADERS),1), [HEADERS])
        values = [HEADERS]
    header = values[0]

    # === VIDEO BANNER ADDITION ===
    # Asegura que exista la etiqueta de columna "VIDEO_URL" SIN renombrar las columnas
    # existentes (para no romper los campos del data source de Looker).
    if "VIDEO_URL" not in header:
        new_col_idx = len(header) + 1
        _retry(ws.update, _a1(new_col_idx, 1), [["VIDEO_URL"]])
        header.append("VIDEO_URL")
        if values:
            values[0] = header
        print(f"[INFO] Columna 'VIDEO_URL' agregada en {_a1(new_col_idx,1)} de '{ws_title}'.")
    # === END VIDEO BANNER ADDITION ===

    idx = {h:i for i,h in enumerate(header)}
    existing = {}
    for r_i in range(1, len(values)):
        row = values[r_i]
        key = (
            row[idx.get("DATE",-1)] if "DATE" in idx and len(row)>idx["DATE"] else "",
            row[idx.get("COUNTRY",-1)] if "COUNTRY" in idx and len(row)>idx["COUNTRY"] else "",
            row[idx.get("ITEM",-1)] if "ITEM" in idx and len(row)>idx["ITEM"] else "",
            row[idx.get("POSITION",-1)] if "POSITION" in idx and len(row)>idx["POSITION"] else "",
        )
        if key[0]:
            existing[key] = r_i+1  # 1-indexed
    today = today_str()
    to_update_ranges: List[Dict] = []
    to_append_rows: List[List[str]] = []
    for r in rows:
        row_list = [r.get(h,"") for h in HEADERS]
        key = (today, r.get("COUNTRY",""), r.get("ITEM",""), r.get("POSITION",""))
        if key in existing:
            row_idx = existing[key]
            rng = f"A{row_idx}:{_a1(len(HEADERS),row_idx)}"
            to_update_ranges.append({"range": rng, "values": [row_list]})
        else:
            to_append_rows.append(row_list)
    CHUNK = 80
    for i in range(0, len(to_update_ranges), CHUNK):
        chunk = to_update_ranges[i:i+CHUNK]
        try:
            _retry(ws.batch_update, chunk, value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"[WARN] batch_update falló; guardo en CSV. Motivo: {e}")
            for u in chunk:
                to_append_rows.append(u["values"][0])
    if to_append_rows:
        values = values or [HEADERS]
        last_row = len(values)
        start = last_row + 1
        need_rows = start + len(to_append_rows) + 10
        if ws.row_count < need_rows:
            _retry(ws.resize, rows=need_rows, cols=max(ws.col_count, len(HEADERS)))
        for i in range(0, len(to_append_rows), CHUNK):
            block = to_append_rows[i:i+CHUNK]
            r1 = start + i
            r2 = r1 + len(block) - 1
            rng = f"A{r1}:{_a1(len(HEADERS), r2)}"
            _retry(ws.update, rng, block, value_input_option="USER_ENTERED")
    print(f"[OK] {len(to_update_ranges)} filas actualizadas y {len(to_append_rows)} agregadas en '{ws_title}'. "
          f"Última fila: {len(values) + len(to_append_rows)}")
def write_fallback_csv(rows: List[Dict[str, str]]):
    try:
        with open(FALLBACK_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS)
            if f.tell() == 0:
                w.writeheader()
            for r in rows:
                w.writerow({h: r.get(h, "") for h in HEADERS})
        print(f"[FALLBACK] Guardado/append CSV local: {FALLBACK_CSV}")
    except Exception as e:
        print(f"[FALLBACK ERROR] {e}")
# =========================
# Main (POPUP primero) — recorre PE, CL, CO
# =========================
def run():
    rows: List[Dict[str, str]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1600,1000"],
        )
        context = browser.new_context(
            ignore_https_errors=True,
            viewport={"width": 1600, "height": 1000},
            user_agent=BROWSER_UA,
            bypass_csp=True,
        )
        context.set_extra_http_headers({"Cache-Control":"no-cache","Pragma":"no-cache"})
        page = context.new_page()
        for cc in COUNTRIES:
            global COUNTRY, COUNTRY_PATH, WEB_ASUS, WEB_ROG, URLS
            COUNTRY = cc
            COUNTRY_PATH = cc.lower()
            WEB_ASUS = f"www.asus.com/{COUNTRY_PATH}/"
            WEB_ROG  = f"rog.asus.com/{COUNTRY_PATH}/"
            URLS = {
                "home_asus": f"https://{WEB_ASUS}",
                "home_rog":  f"https://{WEB_ROG}",
                "deals_all": f"https://www.asus.com/{COUNTRY_PATH}/deals/all-deals/",
                "store":     f"https://www.asus.com/{COUNTRY_PATH}/store/",
            }
            # HOME (ASUS) — pasamos defaults para ads_dialog
            extract_home_popup(
                page,
                URLS["home_asus"],
                rows,
                WEB_ASUS,
                default_text="MY ASUS Regístrate",
                default_img="https://dlcdnwebimgs.asus.com/gain/5f77fa18-e244-488e-adff-181cdd651945/fwebp",
            )
            extract_promotional_slim_banner(page, URLS["home_asus"], rows)
            extract_home_hero_all(page, URLS["home_asus"], rows, WEB_ASUS)
            extract_column_banners(page, URLS["home_asus"], rows)
            # HOME (ROG)
            extract_home_popup(page, URLS["home_rog"], rows, WEB_ROG)
            extract_home_hero_all(page, URLS["home_rog"], rows, WEB_ROG)
            extract_rog_promo_banner(page, URLS["home_rog"], rows)
            # DEALS (ASUS)
            extract_deals_tabs(page, URLS["deals_all"], rows)
            # STORE (ASUS)
            extract_store_promotion_banner(page, URLS["store"], rows)
            extract_store_banner_home1(page, URLS["store"], rows)
            extract_store_tabs(page, URLS["store"], rows)
            extract_news_promotions(page, URLS["store"], rows)
        context.close()
        browser.close()

    # === VIDEO BANNER ADDITION ===
    # No requiere navegador: lee el tab "Video Banner" del scraper y lo agrega a `rows`.
    try:
        gc_src, sa_email = get_gspread_client(SERVICE_ACCOUNT_JSON)
        print(f"[INFO] Service Account (lectura Video Banner): {sa_email}")
        extract_video_banner_from_scraper(gc_src, rows)
    except Exception as e:
        print(f"[VIDEO][WARN] No se pudo procesar Video Banner: {type(e).__name__}: {e}")
    # === END VIDEO BANNER ADDITION ===

    try:
        append_or_upsert(GOOGLE_SHEET_ID, WORKSHEET_TITLE, rows)
    except gspread.exceptions.APIError as e:
        print(f"[ERROR Sheets API] {e}")
        write_fallback_csv(rows)
    except PermissionError:
        print("[ERROR Permisos] El service account no tiene acceso al Sheet. Compártelo como Editor.")
        write_fallback_csv(rows)
    except Exception as e:
        print(f"[ERROR Desconocido] {type(e).__name__}: {e}")
        write_fallback_csv(rows)
if __name__ == "__main__":
    run()
