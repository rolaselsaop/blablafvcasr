import os
import asyncio
import aiohttp
import aiofiles
from xml.etree import ElementTree
from bs4 import BeautifulSoup
import pdfplumber
from tqdm.asyncio import tqdm
import json
from datetime import datetime
import time

BASE_DIR = "boe_normas"
HTML_DIR = os.path.join(BASE_DIR, "html")
PDF_DIR = os.path.join(BASE_DIR, "pdf_textos")
METADATA_DIR = os.path.join(BASE_DIR, "metadata")

os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)

HEADERS = {"User-Agent": "MiScraperLegal/1.0 (+tu_email@example.com)"}

# Configuración ULTRA AGRESIVA
MAX_CONCURRENT = 100  # 100 conexiones simultáneas
CONNECTOR_LIMIT = 200  # Límite total de conexiones
TIMEOUT = aiohttp.ClientTimeout(total=30)

async def download_sitemap_index():
    """Descargar sitemap índice"""
    print("Descargando sitemap índice...")
    async with aiohttp.ClientSession() as session:
        async with session.get("https://www.boe.es/eli/sitemap.xml", headers=HEADERS) as response:
            content = await response.read()
            print(f"Status code: {response.status}")
            return content

async def process_sitemap(session, sitemap_url):
    """Procesar un sitemap individual"""
    try:
        async with session.get(sitemap_url, headers=HEADERS, timeout=TIMEOUT) as response:
            content = await response.read()
            root = ElementTree.fromstring(content)
            
            links = []
            metadata = []
            
            for url_elem in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
                loc = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
                lastmod = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")
                
                if loc is not None:
                    url = loc.text
                    links.append(url)
                    
                    metadata.append({
                        "url": url,
                        "lastmod": lastmod.text if lastmod is not None else None,
                        "sitemap_source": sitemap_url,
                        "scraped_at": datetime.now().isoformat()
                    })
            
            return links, metadata
    except Exception as e:
        print(f"Error procesando sitemap {sitemap_url}: {e}")
        return [], []

async def extract_all_links():
    """Extraer todos los enlaces de normas"""
    sitemap_content = await download_sitemap_index()
    root = ElementTree.fromstring(sitemap_content)
    
    sitemaps = []
    for sitemap_elem in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
        loc_elem = sitemap_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if loc_elem is not None and loc_elem.text:
            sitemaps.append(loc_elem.text)
    
    print(f"Sitemaps encontrados: {len(sitemaps)}")
    
    all_links = []
    all_metadata = []
    
    # Procesar sitemaps en paralelo
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, limit_per_host=50)
    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        tasks = [process_sitemap(session, sitemap_url) for sitemap_url in sitemaps]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple):
                links, metadata = result
                all_links.extend(links)
                all_metadata.extend(metadata)
    
    print(f"Total de normas encontradas: {len(all_links)}")
    
    # Guardar metadata
    async with aiofiles.open(os.path.join(METADATA_DIR, "all_metadata.json"), "w", encoding="utf-8") as f:
        await f.write(json.dumps(all_metadata, indent=2, ensure_ascii=False))
    
    return all_links

async def pdf_to_text_async(pdf_path):
    """Convertir PDF a texto de forma asíncrona"""
    try:
        file_size = os.path.getsize(pdf_path)
        if file_size > 50 * 1024 * 1024:  # 50MB
            print(f"PDF muy grande ({file_size/1024/1024:.1f}MB), saltando: {pdf_path}")
            return None
            
        # Ejecutar en thread pool para no bloquear
        loop = asyncio.get_event_loop()
        with pdfplumber.open(pdf_path) as pdf:
            text_parts = []
            batch_size = 10
            
            for i in range(0, len(pdf.pages), batch_size):
                batch_pages = pdf.pages[i:i + batch_size]
                batch_text = "\n\n".join(page.extract_text() or "" for page in batch_pages)
                text_parts.append(batch_text)
                del batch_pages
                
        full_text = "\n\n".join(text_parts)
        
        filename = os.path.basename(pdf_path).replace(".pdf", ".txt")
        txt_path = os.path.join(PDF_DIR, filename)
        
        async with aiofiles.open(txt_path, "w", encoding="utf-8") as f:
            await f.write(full_text)
        return txt_path
    except Exception as e:
        print(f"Error extrayendo PDF {pdf_path}: {e}")
        return None

async def extract_html_info_async(html_path):
    """Extraer información del HTML de forma asíncrona"""
    try:
        async with aiofiles.open(html_path, "r", encoding="utf-8") as f:
            content = await f.read()
            soup = BeautifulSoup(content, "html.parser")
        
        title = soup.find("title")
        title_text = title.get_text().strip() if title else "Sin título"
        
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

async def process_url_async(session, url, semaphore):
    """Procesar una URL de forma asíncrona"""
    async with semaphore:  # Limitar concurrencia
        try:
            if url.endswith(".pdf"):
                filename = url.split("/")[-1] + ".pdf"
                filepath = os.path.join(PDF_DIR, filename)
                txt_filename = filename.replace(".pdf", ".txt")
                txt_path = os.path.join(PDF_DIR, txt_filename)
                
                # Verificar si ya existe
                if os.path.exists(filepath) and os.path.exists(txt_path):
                    return (url, True, "pdf_cached")
                
                if not os.path.exists(filepath):
                    async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
                        if response.status == 200:
                            content = await response.read()
                            async with aiofiles.open(filepath, "wb") as f:
                                await f.write(content)
                
                if not os.path.exists(txt_path):
                    txt_path = await pdf_to_text_async(filepath)
                
                return (url, True if txt_path else False, "pdf")
            else:
                filename = url.split("/")[-1] + ".html"
                filepath = os.path.join(HTML_DIR, filename)
                info_filename = filename.replace(".html", "_info.json")
                info_path = os.path.join(METADATA_DIR, info_filename)
                
                # Verificar si ya existe
                if os.path.exists(filepath) and os.path.exists(info_path):
                    return (url, True, "html_cached")
                
                if not os.path.exists(filepath):
                    async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
                        if response.status == 200:
                            content = await response.read()
                            async with aiofiles.open(filepath, "wb") as f:
                                await f.write(content)
                
                if not os.path.exists(info_path):
                    info = await extract_html_info_async(filepath)
                    if info:
                        async with aiofiles.open(info_path, "w", encoding="utf-8") as f:
                            await f.write(json.dumps(info, indent=2, ensure_ascii=False))
                
                return (url, True, "html")
        except Exception as e:
            return (url, False, f"error: {str(e)}")

async def main():
    """Función principal asíncrona"""
    print("🚀 INICIANDO SCRAPER ULTRA AGRESIVO ASÍNCRONO 🚀")
    print(f"Configuración: {MAX_CONCURRENT} conexiones simultáneas")
    print(f"Pool de conexiones: {CONNECTOR_LIMIT}")
    
    # Extraer todos los enlaces
    all_links = await extract_all_links()
    
    if not all_links:
        print("No se encontraron enlaces para procesar")
        return
    
    # Configurar semáforo para limitar concurrencia
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    # Configurar sesión HTTP
    connector = aiohttp.TCPConnector(
        limit=CONNECTOR_LIMIT,
        limit_per_host=100,
        ttl_dns_cache=300,
        use_dns_cache=True,
    )
    
    successful_downloads = 0
    failed_downloads = 0
    
    print(f"\n🔥 Procesando {len(all_links)} URLs con {MAX_CONCURRENT} conexiones simultáneas...")
    
    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        # Crear todas las tareas
        tasks = [process_url_async(session, url, semaphore) for url in all_links]
        
        # Procesar con barra de progreso
        for coro in tqdm.as_completed(tasks, desc="🔥 Procesando URLs"):
            url, ok, kind = await coro
            if ok:
                successful_downloads += 1
            else:
                failed_downloads += 1
    
    print(f"\n=== RESUMEN DEL SCRAPING ULTRA AGRESIVO ===")
    print(f"Total de URLs procesadas: {len(all_links)}")
    print(f"Descargas exitosas: {successful_downloads}")
    print(f"Descargas fallidas: {failed_downloads}")
    print(f"Archivos HTML guardados en: {HTML_DIR}")
    print(f"Archivos PDF y textos guardados en: {PDF_DIR}")
    print(f"Metadata guardada en: {METADATA_DIR}")
    print("🎉 Scraping ULTRA AGRESIVO completado!")

if __name__ == "__main__":
    asyncio.run(main())
