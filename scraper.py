import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from xml.etree import ElementTree
from bs4 import BeautifulSoup
import pdfplumber
from tqdm import tqdm
import json
from datetime import datetime

BASE_DIR = "boe_normas"
HTML_DIR = os.path.join(BASE_DIR, "html")
PDF_DIR = os.path.join(BASE_DIR, "pdf_textos")
METADATA_DIR = os.path.join(BASE_DIR, "metadata")

os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)

HEADERS = {"User-Agent": "MiScraperLegal/1.0 (+tu_email@example.com)"}

# 1️⃣ Descargar sitemap índice
sitemap_index_url = "https://www.boe.es/eli/sitemap.xml"
print("Descargando sitemap índice...")
r = requests.get(sitemap_index_url, headers=HEADERS)
print(f"Status code: {r.status_code}")

root = ElementTree.fromstring(r.content)

# Fix: Look for loc elements within sitemap elements
sitemaps = []
for sitemap_elem in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
    loc_elem = sitemap_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
    if loc_elem is not None and loc_elem.text:
        sitemaps.append(loc_elem.text)

print(f"Sitemaps encontrados: {len(sitemaps)}")
print(f"Sitemaps: {sitemaps}")

# 2️⃣ Extraer todos los enlaces de normas con metadata
all_links = []
all_metadata = []

for i, sitemap_url in enumerate(sitemaps):
    print(f"Procesando sitemap {i+1}/{len(sitemaps)}: {sitemap_url}")
    try:
        r = requests.get(sitemap_url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        root = ElementTree.fromstring(r.content)
        
        # Extraer URLs y fechas de modificación
        for url_elem in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
            loc = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
            lastmod = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")
            
            if loc is not None:
                url = loc.text
                all_links.append(url)
                
                metadata = {
                    "url": url,
                    "lastmod": lastmod.text if lastmod is not None else None,
                    "sitemap_source": sitemap_url,
                    "scraped_at": datetime.now().isoformat()
                }
                all_metadata.append(metadata)
                
    except Exception as e:
        print(f"Error procesando sitemap {sitemap_url}: {e}")
        continue

print(f"Total de normas encontradas: {len(all_links)}")

# Guardar metadata
with open(os.path.join(METADATA_DIR, "all_metadata.json"), "w", encoding="utf-8") as f:
    json.dump(all_metadata, f, indent=2, ensure_ascii=False)

# 3️⃣ Función para descargar y guardar HTML
def save_html(url):
    filename = url.split("/")[-1] + ".html"
    filepath = os.path.join(HTML_DIR, filename)
    if os.path.exists(filepath):
        return filepath
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(r.content)
        return filepath
    except Exception as e:
        print(f"Error descargando HTML {url}: {e}")
        return None

# 4️⃣ Función para convertir PDF a texto (optimizada para memoria)
def pdf_to_text(pdf_path):
    try:
        # Verificar tamaño del archivo antes de procesar
        file_size = os.path.getsize(pdf_path)
        if file_size > 50 * 1024 * 1024:  # 50MB
            print(f"PDF muy grande ({file_size/1024/1024:.1f}MB), saltando: {pdf_path}")
            return None
            
        with pdfplumber.open(pdf_path) as pdf:
            # Procesar páginas en lotes para ahorrar memoria
            text_parts = []
            batch_size = 10  # Procesar 10 páginas a la vez
            
            for i in range(0, len(pdf.pages), batch_size):
                batch_pages = pdf.pages[i:i + batch_size]
                batch_text = "\n\n".join(page.extract_text() or "" for page in batch_pages)
                text_parts.append(batch_text)
                
                # Liberar memoria explícitamente
                del batch_pages
                
        full_text = "\n\n".join(text_parts)
        
        # Guardar texto
        filename = os.path.basename(pdf_path).replace(".pdf", ".txt")
        txt_path = os.path.join(PDF_DIR, filename)
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(full_text)
        return txt_path
    except Exception as e:
        print(f"Error extrayendo PDF {pdf_path}: {e}")
        return None

# 5️⃣ Función para extraer información adicional del HTML
def extract_html_info(html_path):
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")
        
        # Extraer título
        title = soup.find("title")
        title_text = title.get_text().strip() if title else "Sin título"
        
        # Extraer metadatos
        meta_info = {}
        for meta in soup.find_all("meta"):
            name = meta.get("name") or meta.get("property")
            content = meta.get("content")
            if name and content:
                meta_info[name] = content
        
        return {
            "title": title_text,
            "meta": meta_info,
            "extracted_at": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error extrayendo info de HTML {html_path}: {e}")
        return None

def build_session():
    session = requests.Session()
    # Configuración optimizada para máximo rendimiento
    retries = Retry(
        total=3,  # Reducido de 5 a 3 para mayor velocidad
        backoff_factor=0.3,  # Reducido de 0.5 a 0.3
        status_forcelist=[429, 500, 502, 503, 504]
    )
    # Aumentado pool de conexiones para 48 threads
    adapter = HTTPAdapter(
        max_retries=retries, 
        pool_connections=200,  # Aumentado de 100 a 200
        pool_maxsize=200,      # Aumentado de 100 a 200
        pool_block=False       # No bloquear cuando pool está lleno
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

def process_url(session, url):
    try:
        if url.endswith(".pdf"):
            filename = url.split("/")[-1] + ".pdf"
            filepath = os.path.join(PDF_DIR, filename)
            txt_filename = filename.replace(".pdf", ".txt")
            txt_path = os.path.join(PDF_DIR, txt_filename)
            
            # Verificar si ya existe tanto PDF como TXT
            if os.path.exists(filepath) and os.path.exists(txt_path):
                return (url, True, "pdf_cached")
            
            if not os.path.exists(filepath):
                r = session.get(url, timeout=20)
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    f.write(r.content)
            
            # Solo procesar PDF si no existe el TXT
            if not os.path.exists(txt_path):
                txt_path = pdf_to_text(filepath)
            
            return (url, True if txt_path else False, "pdf")
        else:
            filename = url.split("/")[-1] + ".html"
            filepath = os.path.join(HTML_DIR, filename)
            info_filename = filename.replace(".html", "_info.json")
            info_path = os.path.join(METADATA_DIR, info_filename)
            
            # Verificar si ya existe tanto HTML como metadata
            if os.path.exists(filepath) and os.path.exists(info_path):
                return (url, True, "html_cached")
            
            html_path = save_html_with_session(session, url)
            if html_path:
                # Solo extraer info si no existe
                if not os.path.exists(info_path):
                    info = extract_html_info(html_path)
                    if info:
                        with open(info_path, "w", encoding="utf-8") as f:
                            json.dump(info, f, indent=2, ensure_ascii=False)
                return (url, True, "html")
            return (url, False, "html")
    except Exception as e:
        return (url, False, "error: " + str(e))

def save_html_with_session(session, url):
    filename = url.split("/")[-1] + ".html"
    filepath = os.path.join(HTML_DIR, filename)
    if os.path.exists(filepath):
        return filepath
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(r.content)
        return filepath
    except Exception as e:
        print(f"Error descargando HTML {url}: {e}")
        return None

# 6️⃣ Descargar todas las normas (concurrency optimizada)
successful_downloads = 0
failed_downloads = 0

print("Iniciando descarga de todas las normas en paralelo...")

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Contador thread-safe para estadísticas
stats_lock = threading.Lock()

def update_stats(success):
    global successful_downloads, failed_downloads
    with stats_lock:
        if success:
            successful_downloads += 1
        else:
            failed_downloads += 1

session = build_session()

# Configuración de threads optimizada para scraping web
# Conservador: 12, Moderado: 24, Agresivo: 48
max_workers = min(48, os.cpu_count() * 6 if os.cpu_count() else 48)

# Procesamiento en lotes para mejor rendimiento
batch_size = max_workers * 2  # Procesar en lotes de 96 URLs
total_batches = (len(all_links) + batch_size - 1) // batch_size

print(f"Procesando {len(all_links)} URLs en {total_batches} lotes de {batch_size} con {max_workers} threads")

for batch_num in range(total_batches):
    start_idx = batch_num * batch_size
    end_idx = min(start_idx + batch_size, len(all_links))
    batch_urls = all_links[start_idx:end_idx]
    
    print(f"Procesando lote {batch_num + 1}/{total_batches} ({len(batch_urls)} URLs)")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_url, session, url) for url in batch_urls]
        
        for fut in tqdm(as_completed(futures), total=len(futures), 
                       desc=f"Lote {batch_num + 1}/{total_batches}"):
            url, ok, kind = fut.result()
            update_stats(ok)

print(f"\n=== RESUMEN DEL SCRAPING ===")
print(f"Total de URLs procesadas: {len(all_links)}")
print(f"Descargas exitosas: {successful_downloads}")
print(f"Descargas fallidas: {failed_downloads}")
print(f"Archivos HTML guardados en: {HTML_DIR}")
print(f"Archivos PDF y textos guardados en: {PDF_DIR}")
print(f"Metadata guardada en: {METADATA_DIR}")
print("Scraping completo!")