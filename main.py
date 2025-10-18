import requests
import time
import re
import csv
import os  # NY: For å håndtere filsystemet og environment variables
import boto3  # NY: For å kommunisere med AWS S3
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Konfig ----------
BASE_URL = "https://www.finn.no/mobility/search/car?price_from=1500&registration_class=1&sales_form=1"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nb-NO,nb;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Sett maks antall sider som skal hentes
MAX_PAGES = 50
# Juster dette tallet basert på nettverket og CPU-en din. 10 er en trygg start.
MAX_WORKERS = 10


# ---------- Hjelpefunksjoner (Uendret fra ditt originale script) ----------

def fordel(verdier: str):
    """
    Returnerer: (år:str, km:int, drivstoff:str, gir:str, rekkevidde:int)
    """
    YEAR_MAX = datetime.now ().year + 1
    s = (verdier or "").replace ("\xa0", " ").strip ()
    s = re.sub (r"[∙•]", "·", s)
    tokens = [t.strip () for t in s.split ("·") if t.strip ()]

    def _is_year(tok):
        if not re.fullmatch (r"(19|20)\d{2}", tok): return False
        return 1950 <= int (tok) <= YEAR_MAX

    def _km_from(tok):
        m = re.search (r"(\d[\d\s]*)\s*km\b", tok, re.I)
        return int (re.sub (r"\s+", "", m.group (1))) if m else None

    aar, km, meta_start = "", 0, None
    for i in range (len (tokens) - 1):
        if _is_year (tokens[i]):
            km_val = _km_from (tokens[i + 1])
            if km_val is not None:
                aar, km, meta_start = tokens[i], km_val, i
                break

    if meta_start is None:
        m = re.search (r"\b((?:19|20)\d{2})\b\s*·\s*([\d\s]+)\s*km\b", s, re.I)
        if m and 1950 <= int (m.group (1)) <= YEAR_MAX:
            aar, km = m.group (1), int (re.sub (r"\s+", "", m.group (2)))

    def _fuel_of(txt: str):
        lt = txt.lower ()
        if re.search (r"\b(el|elektrisk|bev|elbil)\b", lt): return "Elektrisk"
        if re.search (r"plug[-\s]*in|ladbar|phev", lt): return "Plug-in hybrid"
        if re.search (r"\b(mildhybrid|mhev|hev|hybrid)\b", lt): return "Hybrid"
        if re.search (r"\b(diesel|tdi|hdi|dci|cdti|crdi|d-?4d|multijet|jtd|bi-?tdi)\b", lt): return "Diesel"
        if re.search (r"\b(bensin|petrol|tsi|tfsi|mpi|gdi|ecoboost)\b", lt): return "Bensin"
        if "hydrogen" in lt: return "Hydrogen"
        return None

    drivstoff, gir = "", "Automat"
    search_order = tokens[meta_start + 2:] + tokens[:meta_start] if meta_start is not None else tokens
    for t in search_order:
        if not drivstoff:
            f = _fuel_of (t)
            if f: drivstoff = f

    if drivstoff in {"Bensin", "Diesel"}:
        gir_funnet = None
        for t in tokens:
            lt = t.lower ()
            if re.search (r"\bmanuell\b", lt): gir_funnet = "Manuell"; break
            if re.search (r"\b(automat|cvt|trinnl[øo]s|aut\.)\b", lt): gir_funnet = "Automat"; break
        gir = gir_funnet or "Automat"

    rekkevidde = 0
    for t in search_order:
        lt = t.lower ()
        if any (k in lt for k in ("rekkevidde", "wltp", "epa", "nedc")):
            m = re.search (r"(\d[\d\s]*)\s*km\b", t, re.I)
            if m: rekkevidde = int (re.sub (r"\s+", "", m.group (1))); break
    if rekkevidde == 0:
        for t in tokens:
            m = re.search (r"(\d[\d\s]*)\s*km\b", t, re.I)
            if m:
                val = int (re.sub (r"\s+", "", m.group (1)))
                if val != km and val > 20: rekkevidde = val; break

    return aar, km, drivstoff, gir, rekkevidde


def build_url(page: int) -> str:
    return f"{BASE_URL}&page={page}"


def find_cards(soup: BeautifulSoup):
    ad_links = soup.select ("a[href*='/mobility/item/']")
    seen = set ()
    cards = []
    for a in ad_links:
        href = a.get ("href", "")
        if href in seen or not href: continue
        seen.add (href)
        card = a.find_parent ("article")
        if card: cards.append ((a, card))
    return cards


def extract_finnkode(href: str) -> str:
    m = re.search (r"/item/(\d+)", href or "")
    return m.group (1) if m else ""


def extract_title_info(card, link_tag):
    title_selectors = [
        "h2", "h3", ".ads__unit__content__title",
        ".sf-card-title", "div[data-testid='title']"
    ]
    bilmerke = ""
    for selector in title_selectors:
        tag = card.select_one (selector)
        if tag and tag.get_text (strip=True):
            bilmerke = tag.get_text (strip=True)
            break

    if not bilmerke and link_tag:
        bilmerke = link_tag.get_text (strip=True)

    bilmerke = " ".join (bilmerke.split ())
    parts = bilmerke.split (maxsplit=1)
    merke = parts[0] if parts else ""
    modell = parts[1] if len (parts) > 1 else ""
    info = bilmerke
    return bilmerke, merke, modell, info


def extract_meta(card) -> str:
    def clean(t: str) -> str:
        return re.sub (r"\s+", " ", (t or "").replace ("\xa0", " ")).strip ()

    def looks_like_meta(t: str) -> bool:
        return bool (re.search (r"\b(19|20)\d{2}\b", t) and "km" in t.lower ())

    meta_selectors = [
        "div[data-testid='car-ad-metadata']",
        "div.justify-between > div.flex.gap-8",
        ".ads__unit__content__details",
        ".sf-card-body > div > div:nth-child(2)"
    ]
    for selector in meta_selectors:
        tag = card.select_one (selector)
        if tag:
            meta_text = clean (tag.get_text (" · ", strip=True))
            if looks_like_meta (meta_text):
                return meta_text

    candidates = []
    for tag in card.find_all (['div', 'span']):
        if tag.find (['div', 'span']):
            continue
        text = clean (tag.get_text ())
        if looks_like_meta (text):
            candidates.append (text)

    if candidates:
        return max (candidates, key=len)

    return ""


def extract_price(card) -> str:
    price_selectors = [
        "span.font-bold.text-20", "div[class*='price'] > span",
        ".sf-card-price", ".ads__unit__content__keys > span", "span.t3"
    ]
    for selector in price_selectors:
        price_tag = card.select_one (selector)
        if price_tag:
            price_text = price_tag.get_text (strip=True)
            if "kr" in price_text or price_text.replace ('\xa0', '').isdigit ():
                return price_text.replace ("kr", "").replace ("\xa0", "").strip ()
    return 'Solgt'


def mer_info(card):
    sted, selger = "", ""
    forhandler, garanti, service = "", 0, False

    span_list = card.select ("div.text-detail span")
    if len (span_list) > 0:
        linje1 = span_list[0].get_text ("·", strip=True)
        parts = [p.strip () for p in re.split (r"\s*[·∙•]\s*", linje1) if p.strip ()]
        if parts: sted = parts[0]
        if len (parts) > 1: selger = parts[1]
    if len (span_list) > 1:
        linje2 = span_list[1].get_text ("·", strip=True)
        biter = [p.strip () for p in re.split (r"\s*[·∙•]\s*", linje2) if p.strip ()]
        for b in biter:
            lb = b.lower ()
            if any (x in lb for x in ["forhandler", "privat", "bedrift"]): forhandler = b
        m = re.search (r"(\d+)\s*mnd", linje2, flags=re.IGNORECASE)
        if m: garanti = int (m.group (1))
        service = any ("service" in b.lower () for b in biter)

    if sted or selger or forhandler or garanti:
        return sted, selger, forhandler, garanti, service

    if not sted:
        location_selectors = ["div[data-testid='ad-location-line']", ".ads__unit__content__location",
                              ".sf-card-location"]
        for selector in location_selectors:
            tag = card.select_one (selector)
            if tag:
                text = tag.get_text ("·", strip=True)
                parts = [p.strip () for p in re.split (r"\s*[·∙•]\s*", text) if p.strip ()]
                if parts: sted = parts[0]
                if len (parts) > 1: selger = parts[1]
                break

    if not forhandler and not garanti:
        tags_selectors = ["div[data-testid='vehicle-ad-tags']", ".ads__unit__content__tags", ".sf-card-tags"]
        for selector in tags_selectors:
            tag = card.select_one (selector)
            if tag:
                text = tag.get_text ("·", strip=True)
                biter = [p.strip () for p in re.split (r"\s*[·∙•]\s*", text) if p.strip ()]
                for b in biter:
                    lb = b.lower ()
                    if any (x in lb for x in ["forhandler", "privat", "bedrift"]): forhandler = b
                m = re.search (r"(\d+)\s*mnd", text, flags=re.IGNORECASE)
                if m: garanti = int (m.group (1))
                service = any ("service" in b.lower () for b in biter)
                break

    return sted, selger, forhandler, garanti, service


def scrape_page(session, page_number):
    url = build_url (page_number)
    all_rows = []
    try:
        resp = session.get (url, headers=HEADERS, timeout=20)
        resp.raise_for_status ()
    except requests.exceptions.RequestException as e:
        print (f"Feil ved henting av side {page_number}: {e}")
        return []

    soup = BeautifulSoup (resp.text, "lxml")
    cards = find_cards (soup)

    if not cards:
        print (f"Fant ingen biler på side {page_number}, stopper.")
        return []

    print (f"Behandler side {page_number}, fant {len (cards)} biler.")

    for a, card in cards:
        finnkode = extract_finnkode (a.get ("href", ""))
        if not finnkode: continue

        bilmerke, merke, modell, info = extract_title_info (card, a)
        meta = extract_meta (card)
        aar, km, drivstoff, gir, rekkevidde = fordel (meta)
        price = extract_price (card)
        sted, selger, forhandler, garanti, service = mer_info (card)

        all_rows.append ([
            finnkode, bilmerke, merke, modell, info, aar, km, gir, drivstoff,
            rekkevidde, price, selger, sted, garanti, forhandler, service
        ])
    return all_rows


# ---------- NY FUNKSJON FOR AWS S3 OPPLASTING ----------

def upload_to_s3(file_name, bucket_name, object_name=None):
    """
    Laster opp en fil til en S3-bøtte.
    Henter credentials og konfigurasjon fra environment variables for sikkerhet.
    """
    if object_name is None:
        object_name = os.path.basename (file_name)

    # Hent credentials fra environment variables (disse settes i Render)
    aws_access_key = os.environ.get ('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get ('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get ('AWS_REGION')

    if not all ([aws_access_key, aws_secret_key, aws_region]):
        print ("Feil: AWS credentials (ID, Key, Region) er ikke satt som environment variables.")
        return False

    s3_client = boto3.client (
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    try:
        print (f"Laster opp {file_name} til S3-bøtte {bucket_name}...")
        s3_client.upload_file (file_name, bucket_name, object_name)
        print ("Opplasting til S3 fullført.")
        return True
    except Exception as e:
        print (f"Feil under opplasting til S3: {e}")
        return False


# ---------- HOVEDFUNKSJON (Oppdatert) ----------

def main():
    dato_og_time = time.strftime ("%d-%m-%Y_%H")

    # ENDRET: Filnavnet er nå relativt. Det lagres i serverens midlertidige filsystem.
    file_name = f'biler_siste_{dato_og_time}.csv'

    # NY: Hent bøttenavn fra environment variable. Dette er mer fleksibelt.
    s3_bucket_name = os.environ.get ('S3_BUCKET_NAME')
    if not s3_bucket_name:
        print ("Feil: S3_BUCKET_NAME er ikke satt som environment variable. Avslutter.")
        return

    print (f"Starter scraping. Resultatet lagres midlertidig som {file_name}")
    t0 = time.perf_counter ()

    # Denne delen er uendret. Scraper data og skriver til en lokal CSV-fil.
    with open (file_name, "w", newline="", encoding="utf-16") as csv_file:
        utfil = csv.writer (csv_file, delimiter=";")
        utfil.writerow ([
            "FinnKode", "Bilmerke", "Merke", "Modell", "Info", "Årstall", "Kjørelengde",
            "Girkasse", "Drivstoff", "Rekkevidde", "Pris",
            "Selger", "Sted", "Garanti (mnd)", "Forhandler type", "Service oppgitt"
        ])

        with requests.Session () as session:
            with ThreadPoolExecutor (max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit (scrape_page, session, page) for page in range (1, MAX_PAGES + 1)]
                total_cars = 0
                for future in as_completed (futures):
                    try:
                        rows = future.result ()
                        if rows:
                            utfil.writerows (rows)
                            total_cars += len (rows)
                            csv_file.flush ()  # Skriver til filen fortløpende
                    except Exception as e:
                        print (f"En oppgave genererte en feil: {e}")

    print (f"\nScraping ferdig! Fant totalt {total_cars} biler på {MAX_PAGES} sider.")
    print (f"Det tok {time.perf_counter () - t0:.2f} sekunder.")

    # NY DEL: Last opp filen til S3 hvis vi fant noen biler
    if total_cars > 0:
        upload_to_s3 (file_name, s3_bucket_name)
    else:
        print ("Ingen biler funnet, hopper over opplasting til S3.")

    # NY DEL: Slett den lokale filen etterpå for å rydde opp serverplass
    if os.path.exists (file_name):
        try:
            os.remove (file_name)
            print (f"Slettet midlertidig fil: {file_name}")
        except OSError as e:
            print (f"Feil ved sletting av lokal fil: {e}")


if __name__ == "__main__":
    main ()