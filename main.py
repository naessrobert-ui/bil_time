# main.py
import requests
import time
import re
import csv
import os
import random
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Proxy (IPRoyal Residential - NO) ----------
PROXY_HOST = "geo.iproyal.com"
PROXY_PORT = 12321
PROXY_USERNAME = "IX793Q5mJLdxDQDA"
PROXY_PASSWORD = "IYURzEcPE2Klkbkg_country-no"
PROXY_URL = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}

# ---------- Konfig ----------
BASE_URL = "https://www.finn.no/mobility/search/car?price_from=1500&registration_class=1&sales_form=1"
USER_AGENTS = [
    # roter én UA per session
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
BASE_HEADERS = {
    "Accept-Language": "nb-NO,nb;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.finn.no/",
}

# Vær konservativ først (403 i loggen tyder på blokkering)
MAX_PAGES = 50
MAX_WORKERS = 2          # start lavt; øk til 3–4 hvis stabilt
HTTP_TIMEOUT = 20
PER_REQUEST_SLEEP = (0.25, 0.6)  # tilfeldig pause etter vellykket kall


# ---------- Hjelpefunksjoner ----------
def build_url(page: int) -> str:
    return f"{BASE_URL}&page={page}"


def make_session():
    """Én session per tråd, med egen UA, proxy og headere."""
    s = requests.Session()
    s.trust_env = False                 # ignorer evt. system-proxy i Render
    s.proxies = PROXIES
    headers = BASE_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    s.headers.update(headers)
    return s


def fordel(verdier: str):
    YEAR_MAX = datetime.now().year + 1
    s = (verdier or "").replace("\xa0", " ").strip()
    s = re.sub(r"[∙•]", "·", s)
    tokens = [t.strip() for t in s.split("·") if t.strip()]

    def _is_year(tok):
        if not re.fullmatch(r"(19|20)\d{2}", tok): return False
        return 1950 <= int(tok) <= YEAR_MAX

    def _km_from(tok):
        m = re.search(r"(\d[\d\s]*)\s*km\b", tok, re.I)
        return int(re.sub(r"\s+", "", m.group(1))) if m else None

    aar, km, meta_start = "", 0, None
    for i in range(len(tokens) - 1):
        if _is_year(tokens[i]):
            km_val = _km_from(tokens[i + 1])
            if km_val is not None:
                aar, km, meta_start = tokens[i], km_val, i
                break

    if meta_start is None:
        m = re.search(r"\b((?:19|20)\d{2})\b\s*·\s*([\d\s]+)\s*km\b", s, re.I)
        if m and 1950 <= int(m.group(1)) <= YEAR_MAX:
            aar, km = m.group(1), int(re.sub(r"\s+", "", m.group(2)))

    def _fuel_of(txt: str):
        lt = txt.lower()
        if re.search(r"\b(el|elektrisk|bev|elbil)\b", lt): return "Elektrisk"
        if re.search(r"plug[-\s]*in|ladbar|phev", lt): return "Plug-in hybrid"
        if re.search(r"\b(mildhybrid|mhev|hev|hybrid)\b", lt): return "Hybrid"
        if re.search(r"\b(diesel|tdi|hdi|dci|cdti|crdi|d-?4d|multijet|jtd|bi-?tdi)\b", lt): return "Diesel"
        if re.search(r"\b(bensin|petrol|tsi|tfsi|mpi|gdi|ecoboost)\b", lt): return "Bensin"
        if "hydrogen" in lt: return "Hydrogen"
        return None

    drivstoff, gir = "", "Automat"
    search_order = tokens[meta_start + 2:] + tokens[:meta_start] if meta_start is not None else tokens
    for t in search_order:
        if not drivstoff:
            f = _fuel_of(t)
            if f: drivstoff = f

    if drivstoff in {"Bensin", "Diesel"}:
        gir_funnet = None
        for t in tokens:
            lt = t.lower()
            if re.search(r"\bmanuell\b", lt): gir_funnet = "Manuell"; break
            if re.search(r"\b(automat|cvt|trinnl[øo]s|aut\.)\b", lt): gir_funnet = "Automat"; break
        gir = gir_funnet or "Automat"

    rekkevidde = 0
    for t in search_order:
        lt = t.lower()
        if any(k in lt for k in ("rekkevidde", "wltp", "epa", "nedc")):
            m = re.search(r"(\d[\d\s]*)\s*km\b", t, re.I)
            if m: rekkevidde = int(re.sub(r"\s+", "", m.group(1))); break
    if rekkevidde == 0:
        for t in tokens:
            m = re.search(r"(\d[\d\s]*)\s*km\b", t, re.I)
            if m:
                val = int(re.sub(r"\s+", "", m.group(1)))
                if val != km and val > 20: rekkevidde = val; break

    return aar, km, drivstoff, gir, rekkevidde


def find_cards(soup: BeautifulSoup):
    ad_links = soup.select("a[href*='/mobility/item/']")
    seen, cards = set(), []
    for a in ad_links:
        href = a.get("href", "")
        if href in seen or not href: continue
        seen.add(href)
        card = a.find_parent("article")
        if card: cards.append((a, card))
    return cards


def extract_finnkode(href: str) -> str:
    m = re.search(r"/item/(\d+)", href or "")
    return m.group(1) if m else ""


def extract_title_info(card, link_tag):
    title_selectors = ["h2", "h3", ".ads__unit__content__title", ".sf-card-title", "div[data-testid='title']"]
    bilmerke = ""
    for selector in title_selectors:
        tag = card.select_one(selector)
        if tag and tag.get_text(strip=True):
            bilmerke = tag.get_text(strip=True); break
    if not bilmerke and link_tag:
        bilmerke = link_tag.get_text(strip=True)
    bilmerke = " ".join(bilmerke.split())
    parts = bilmerke.split(maxsplit=1)
    merke = parts[0] if parts else ""
    modell = parts[1] if len(parts) > 1 else ""
    info = bilmerke
    return bilmerke, merke, modell, info


def extract_meta(card) -> str:
    def clean(t: str) -> str:
        return re.sub(r"\s+", " ", (t or "").replace("\xa0", " ")).strip()

    def looks_like_meta(t: str) -> bool:
        return bool(re.search(r"\b(19|20)\d{2}\b", t) and "km" in t.lower())

    meta_selectors = [
        "div[data-testid='car-ad-metadata']",
        "div.justify-between > div.flex.gap-8",
        ".ads__unit__content__details",
        ".sf-card-body > div > div:nth-child(2)"
    ]
    for selector in meta_selectors:
        tag = card.select_one(selector)
        if tag:
            meta_text = clean(tag.get_text(" · ", strip=True))
            if looks_like_meta(meta_text):
                return meta_text

    candidates = []
    for tag in card.find_all(['div', 'span']):
        if tag.find(['div', 'span']):  # hopp over containere
            continue
        text = clean(tag.get_text())
        if looks_like_meta(text):
            candidates.append(text)
    if candidates:
        return max(candidates, key=len)
    return ""


def extract_price(card) -> str:
    price_selectors = [
        "span.font-bold.text-20", "div[class*='price'] > span",
        ".sf-card-price", ".ads__unit__content__keys > span", "span.t3"
    ]
    for selector in price_selectors:
        price_tag = card.select_one(selector)
        if price_tag:
            price_text = price_tag.get_text(strip=True)
            if "kr" in price_text or price_text.replace('\xa0', '').isdigit():
                return price_text.replace("kr", "").replace("\xa0", "").strip()
    return 'Solgt'


def mer_info(card):
    sted, selger = "", ""
    forhandler, garanti, service = "", 0, False

    span_list = card.select("div.text-detail span")
    if len(span_list) > 0:
        linje1 = span_list[0].get_text("·", strip=True)
        parts = [p.strip() for p in re.split(r"\s*[·∙•]\s*", linje1) if p.strip()]
        if parts: sted = parts[0]
        if len(parts) > 1: selger = parts[1]
    if len(span_list) > 1:
        linje2 = span_list[1].get_text("·", strip=True)
        biter = [p.strip() for p in re.split(r"\s*[·∙•]\s*", linje2) if p.strip()]
        for b in biter:
            lb = b.lower()
            if any(x in lb for x in ["forhandler", "privat", "bedrift"]): forhandler = b
        m = re.search(r"(\d+)\s*mnd", linje2, flags=re.IGNORECASE)
        if m: garanti = int(m.group(1))
        service = any("service" in b.lower() for b in biter)

    if sted or selger or forhandler or garanti:
        return sted, selger, forhandler, garanti, service

    if not sted:
        for selector in ["div[data-testid='ad-location-line']", ".ads__unit__content__location", ".sf-card-location"]:
            tag = card.select_one(selector)
            if tag:
                text = tag.get_text("·", strip=True)
                parts = [p.strip() for p in re.split(r"\s*[·∙•]\s*", text) if p.strip()]
                if parts: sted = parts[0]
                if len(parts) > 1: selger = parts[1]
                break

    if not forhandler and not garanti:
        for selector in ["div[data-testid='vehicle-ad-tags']", ".ads__unit__content__tags", ".sf-card-tags"]:
            tag = card.select_one(selector)
            if tag:
                text = tag.get_text("·", strip=True)
                biter = [p.strip() for p in re.split(r"\s*[·∙•]\s*", text) if p.strip()]
                for b in biter:
                    lb = b.lower()
                    if any(x in lb for x in ["forhandler", "privat", "bedrift"]): forhandler = b
                m = re.search(r"(\d+)\s*mnd", text, flags=re.IGNORECASE)
                if m: garanti = int(m.group(1))
                service = any("service" in b.lower() for b in biter)
                break

    return sted, selger, forhandler, garanti, service


def fetch_with_backoff(session: requests.Session, url: str, attempts: int = 4):
    """GET med enkel backoff for 403/429."""
    for i in range(1, attempts + 1):
        try:
            resp = session.get(url, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200:
                # liten, tilfeldig pause etter vellykket kall
                time.sleep(random.uniform(*PER_REQUEST_SLEEP))
                return resp
            if resp.status_code in (403, 429):
                time.sleep(1.2 * i)  # backoff
                continue
            resp.raise_for_status()
        except requests.RequestException:
            if i == attempts:
                raise
            time.sleep(0.8 * i)
    return None


def scrape_page(session, page_number):
    url = build_url(page_number)
    try:
        resp = fetch_with_backoff(session, url)
    except Exception as e:
        print(f"Feil ved henting av side {page_number}: {e}")
        return []

    if not resp:
        print(f"Side {page_number}: ingen respons.")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    cards = find_cards(soup)
    if not cards:
        print(f"Fant ingen biler på side {page_number}.")
        return []

    rows = []
    for a, card in cards:
        finnkode = extract_finnkode(a.get("href", ""))
        if not finnkode:
            continue
        bilmerke, merke, modell, info = extract_title_info(card, a)
        meta = extract_meta(card)
        aar, km, drivstoff, gir, rekkevidde = fordel(meta)
        price = extract_price(card)
        sted, selger, forhandler, garanti, service = mer_info(card)
        rows.append([
            finnkode, bilmerke, merke, modell, info, aar, km, gir, drivstoff,
            rekkevidde, price, selger, sted, garanti, forhandler, service
        ])
    return rows


# ---------- S3-opplasting (valgfri) ----------
def upload_to_s3(file_name, bucket_name, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_REGION')

    if not all([aws_access_key, aws_secret_key, aws_region, bucket_name]):
        print("S3-env ikke komplett → hopper over opplasting.")
        return False

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    try:
        print(f"Laster opp {file_name} til s3://{bucket_name}/{object_name} ...")
        s3_client.upload_file(file_name, bucket_name, object_name)
        print("Opplasting til S3 fullført.")
        return True
    except Exception as e:
        print(f"Feil under opplasting til S3: {e}")
        return False


# ---------- HOVED ----------
def main():
    dato_og_time = time.strftime("%d-%m-%Y_%H")
    file_name = f"biler_siste_{dato_og_time}.csv"

    # Verifiser proxy-IP én gang
    try:
        test_s = make_session()
        ip = test_s.get("https://ipv4.icanhazip.com/", timeout=15).text.strip()
        print("Proxy på IP:", ip)
    except Exception as e:
        print("Advarsel: Klarte ikke å verifisere proxy-IP:", e)

    print(f"Starter scraping → {file_name}")
    t0 = time.perf_counter()

    total_cars = 0
    with open(file_name, "w", newline="", encoding="utf-16") as csv_file:
        writer = csv.writer(csv_file, delimiter=";")
        writer.writerow([
            "FinnKode", "Bilmerke", "Merke", "Modell", "Info", "Årstall", "Kjørelengde",
            "Girkasse", "Drivstoff", "Rekkevidde", "Pris",
            "Selger", "Sted", "Garanti (mnd)", "Forhandler type", "Service oppgitt"
        ])

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = []
            for page in range(1, MAX_PAGES + 1):
                sess = make_session()  # egen session per tråd
                futures.append(ex.submit(scrape_page, sess, page))

            for fut in as_completed(futures):
                try:
                    rows = fut.result()
                    if rows:
                        writer.writerows(rows)
                        total_cars += len(rows)
                        csv_file.flush()
                except Exception as e:
                    print("En oppgave genererte en feil:", e)

    dt = time.perf_counter() - t0
    print(f"\nScraping ferdig! Fant totalt {total_cars} biler på {MAX_PAGES} sider.")
    print(f"Det tok {dt:.2f} sekunder.")

    # S3 (valgfritt)
    s3_bucket_name = os.environ.get('S3_BUCKET_NAME')
    uploaded = False
    if total_cars > 0 and s3_bucket_name:
        uploaded = upload_to_s3(file_name, s3_bucket_name)
    elif total_cars > 0:
        print("S3_BUCKET_NAME ikke satt → hopper over S3.")

    # Slett lokal fil bare hvis opplastet
    if uploaded and os.path.exists(file_name):
        try:
            os.remove(file_name)
            print(f"Slettet midlertidig fil: {file_name}")
        except OSError as e:
            print(f"Feil ved sletting av lokal fil: {e}")


if __name__ == "__main__":
    main()
