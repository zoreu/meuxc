from flask import Flask, request, Response, jsonify, stream_with_context
import requests
import logging
import json
import re
import socket
import threading
import struct
import random
import time
import os
import binascii
import ssl
import gzip
import zlib
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qs, urljoin, quote, unquote
from collections import deque
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import re
from urllib.parse import urlparse

app = Flask(__name__)

# ==================== CONFIGURAÇÕES ====================
TARGET = 'example'  # URL de destino para proxy
PROXY_HOST = "0.0.0.0"
PROXY_PORT = 8000
CACHE_DURATION_SECONDS = 5
CACHE_MAX_CHUNKS = 250
MAX_RETRIES = 7
RETRY_DELAY = 0.5
BUFFER_SIZE = 32768

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)

LISTAS_M3U = ['https://listas.oneplayhd.com/lista01.txt',
              'https://listas.oneplayhd.com/lista02.txt',
              'https://listas.oneplayhd.com/lista03.txt',
              'https://listas.oneplayhd.com/lista04.txt',
              'https://listas.oneplayhd.com/lista05.txt',
              'https://listas.oneplayhd.com/lista06.txt',
              'https://listas.oneplayhd.com/lista07.txt',
              'https://listas.oneplayhd.com/lista08.txt']

# Configuração dos logs
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s'
)

def log(msg, level='info'):
    if level == 'error':
        logging.error(f"🔴 {msg}")
    elif level == 'warning':
        logging.warning(f"🟡 {msg}")
    else:
        logging.info(f"🟢 {msg}")

def extrair_credenciais_xtream(conteudo_m3u):
    """
    Extrai host, username e password de qualquer lista M3U no formato Xtream Codes
    
    Funciona com URLs nos formatos:
    - http://host:porta/username/password/arquivo.m3u8
    - http://host/username/password/arquivo.m3u8
    - http://host:porta/live/username/password/arquivo.m3u8
    - http://host/live/username/password/arquivo.m3u8
    
    Args:
        conteudo_m3u (str): Conteúdo da lista M3U
    
    Returns:
        dict: {host, username, password, url_base, porta}
    """
    
    # Padrão para encontrar URLs de streaming
    padrao_url = r'(https?://[^\s]+\.m3u8?)'
    
    # Encontrar todas as URLs
    urls = re.findall(padrao_url, conteudo_m3u)
    
    if not urls:
        return None
    
    # Pegar a primeira URL
    url = urls[0]
    
    # Parsear a URL
    parsed = urlparse(url)
    
    # Extrair host (com porta se existir)
    host_completo = parsed.netloc
    host_sem_porta = host_completo.split(':')[0]
    porta = host_completo.split(':')[1] if ':' in host_completo else None
    
    # Extrair o caminho
    caminho = parsed.path.strip('/')
    partes = caminho.split('/')
    
    # Inicializar variáveis
    username = None
    password = None
    url_base = None
    
    # Caso 1: Formato direto: host/username/password/arquivo.m3u8
    # Exemplo: http://xewte.top/20264973172322/0369888741520/2550525.m3u8
    if len(partes) >= 3 and not any(p in partes for p in ['live', 'movie', 'series']):
        username = partes[0]
        password = partes[1]
        url_base = f"{parsed.scheme}://{host_completo}"
    
    # Caso 2: Formato com /live/: host/live/username/password/arquivo.m3u8
    # Exemplo: http://tv5play.xyz/live/Rodolfo0424B/843TJFbzw/144225.m3u8
    elif 'live' in partes:
        idx_live = partes.index('live')
        if len(partes) > idx_live + 2:
            username = partes[idx_live + 1]
            password = partes[idx_live + 2]
            url_base = f"{parsed.scheme}://{host_completo}/live"
    
    # Caso 3: Formato com /movie/ ou /series/
    elif 'movie' in partes or 'series' in partes:
        tipo = 'movie' if 'movie' in partes else 'series'
        idx_tipo = partes.index(tipo)
        if len(partes) > idx_tipo + 2:
            username = partes[idx_tipo + 1]
            password = partes[idx_tipo + 2]
            url_base = f"{parsed.scheme}://{host_completo}/{tipo}"
    
    # Se não encontrou, tentar extrair do cabeçalho x-tvg-url
    if not username or not password:
        padrao_header = r'x-tvg-url="[^"]*username=([^&]+)&password=([^"]+)"'
        match = re.search(padrao_header, conteudo_m3u)
        if match:
            username = match.group(1)
            password = match.group(2)
            # Se temos username/password do header, montar URL base
            if not url_base:
                url_base = f"{parsed.scheme}://{host_completo}"
    
    return {
        'host': host_sem_porta,
        'host_completo': host_completo,
        'porta': porta,
        'username': username,
        'password': password,
        'url_base': url_base,
        'url_completa': url,
        'scheme': parsed.scheme,
        'host_base': f"{parsed.scheme}://{host_completo}"
    }

def select_server(options):
    try:
        r = requests.get(LISTAS_M3U[options], headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36'}, timeout=8)
        return extrair_credenciais_xtream(r.text)
    except Exception as e:
        logging.error(f"❌ Erro ao selecionar servidor: {e}")
        return None

def log_stream(msg):
    print(f"\n{'='*80}")
    print(f"📺 STREAM: {msg}")
    print(f"{'='*80}\n")

def get_origin(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return "{}://{}".format(parsed.scheme, parsed.netloc)
    except Exception:
        pass
    return ''

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return '127.0.0.1'

LOCAL_IP = get_local_ip()
PROXY_HOST = LOCAL_IP

# ==================== DNS CUSTOM ====================
class SimpleDNS:
    def __init__(self):
        self.cache = {}
        self.dns_servers = ["1.1.1.1", "8.8.8.8", "208.67.222.222"]
        self.original_getaddrinfo = socket.getaddrinfo
        socket.getaddrinfo = self._resolver

    def _build_query(self, domain):
        transaction_id = random.randint(0, 65535)
        header = struct.pack(">HHHHHH", transaction_id, 0x0100, 1, 0, 0, 0)
        qname = b"".join(
            bytes([len(part)]) + part.encode() for part in domain.split(".")
        ) + b"\x00"
        return header + qname + struct.pack(">HH", 1, 1)

    def _parse_response(self, data):
        try:
            answer_count = struct.unpack(">H", data[6:8])[0]
            offset = 12
            while data[offset] != 0:
                offset += 1
            offset += 5

            for _ in range(answer_count):
                offset += 2
                rtype, _, _, rdlength = struct.unpack(">HHIH", data[offset:offset + 10])
                offset += 10
                if rtype == 1 and rdlength == 4:
                    ip = struct.unpack(">BBBB", data[offset:offset + 4])
                    return ".".join(map(str, ip))
                offset += rdlength
        except Exception:
            pass
        return None

    def resolve(self, domain):
        if domain in self.cache and self.cache[domain]["expires"] > time.time():
            return self.cache[domain]["ip"]

        for dns in self.dns_servers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(2)
                query = self._build_query(domain)
                sock.sendto(query, (dns, 53))
                data, _ = sock.recvfrom(512)
                sock.close()
                ip = self._parse_response(data)
                if ip:
                    self.cache[domain] = {"ip": ip, "expires": time.time() + 3600}
                    return ip
            except Exception:
                continue
        return None

    def _resolver(self, host, port, *args, **kwargs):
        try:
            socket.inet_aton(host)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (host, port))]
        except Exception:
            ip = self.resolve(host)
            if ip:
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))]
        return self.original_getaddrinfo(host, port, *args, **kwargs)

dns = SimpleDNS()

# ==================== CACHE CIRCULAR PARA CANAIS ====================
class CircularBuffer:
    def __init__(self, max_seconds=5, max_chunks=250):
        self.buffer = deque(maxlen=max_chunks)
        self.timestamps = deque(maxlen=max_chunks)
        self.max_seconds = max_seconds
        self.total_bytes = 0
        self.lock = threading.Lock()
        self.last_update = 0
        self.stream_started = False

    def add_chunk(self, chunk):
        with self.lock:
            self.buffer.append(chunk)
            self.timestamps.append(time.time())
            self.total_bytes += len(chunk)
            self.last_update = time.time()
            
            cutoff = time.time() - self.max_seconds
            while self.timestamps and self.timestamps[0] < cutoff:
                removed = self.buffer.popleft()
                self.timestamps.popleft()
                self.total_bytes -= len(removed)

    def get_recovery_chunks(self, duration=3):
        with self.lock:
            if not self.buffer:
                return []
            cutoff = time.time() - duration
            recovery = []
            for i, ts in enumerate(self.timestamps):
                if ts >= cutoff:
                    recovery.append(self.buffer[i])
            if not recovery and self.buffer:
                recovery = list(self.buffer)[-20:]
            return recovery

    def get_continuous_chunks(self, count=30):
        with self.lock:
            if not self.buffer:
                return []
            return list(self.buffer)[-count:]

    def clear(self):
        with self.lock:
            self.buffer.clear()
            self.timestamps.clear()
            self.total_bytes = 0
            self.stream_started = False

# ==================== CACHE MP4 ====================
class MP4Cache:
    def __init__(self, max_chunks=1000):
        self.chunks = {}
        self.max_chunks = max_chunks
        self.lock = threading.Lock()
        self.total_size = 0
        self.content_length = None
        self.content_type = 'video/mp4'

    def add_chunk(self, start_byte, data):
        if not data:
            return
        with self.lock:
            if start_byte not in self.chunks:
                self.chunks[start_byte] = data
                self.total_size += len(data)
                while len(self.chunks) > self.max_chunks:
                    oldest = min(self.chunks.keys())
                    self.total_size -= len(self.chunks[oldest])
                    del self.chunks[oldest]

    def get_range(self, start, end):
        with self.lock:
            keys = sorted(self.chunks.keys())
            if not keys:
                return None

            result = bytearray()
            pos = start

            while pos < end:
                found = False
                for chunk_start in keys:
                    chunk = self.chunks[chunk_start]
                    chunk_end = chunk_start + len(chunk)
                    if chunk_start <= pos < chunk_end:
                        offset = pos - chunk_start
                        take = min(end - pos, chunk_end - pos)
                        result.extend(chunk[offset:offset + take])
                        pos += take
                        found = True
                        break
                if not found:
                    return None

            return bytes(result)

    def get_total_size(self):
        return self.content_length

    def has_data(self, start, end):
        with self.lock:
            keys = sorted(self.chunks.keys())
            if not keys:
                return False
            
            # Verifica se temos todos os dados no range
            pos = start
            while pos < end:
                found = False
                for chunk_start in keys:
                    chunk = self.chunks[chunk_start]
                    chunk_end = chunk_start + len(chunk)
                    if chunk_start <= pos < chunk_end:
                        pos = min(end, chunk_end)
                        found = True
                        break
                if not found:
                    return False
            return True

# ==================== PROXY UNIFICADO ====================
class UnifiedStreamProxy:
    def __init__(self):
        self.channel_caches = {}
        self.mp4_caches = {}
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.stream_lock = threading.Lock()
        self.cache_lock = threading.Lock()
        self.stream_count = 0

    def get_random_user_agent(self):
        random_bytes = binascii.b2a_hex(os.urandom(20))[:32].decode('ascii')
        return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random_bytes} Safari/537.36"

    def get_channel_cache(self, url):
        clean_url = re.sub(r'(_=\d+|timestamp=\d+|t=\d+|seq=\d+)', '', url)
        with self.stream_lock:
            if clean_url not in self.channel_caches:
                self.channel_caches[clean_url] = CircularBuffer(CACHE_DURATION_SECONDS, CACHE_MAX_CHUNKS)
                log(f"📦 Novo cache criado para: {clean_url[:50]}")
            return self.channel_caches[clean_url]

    def get_mp4_cache(self, url):
        clean_url = re.sub(r'(_=\d+|timestamp=\d+|t=\d+|seq=\d+)', '', url)
        with self.cache_lock:
            if clean_url not in self.mp4_caches:
                self.mp4_caches[clean_url] = MP4Cache(1000)
                log(f"📦 Novo cache MP4 criado para: {clean_url[:50]}")
            return self.mp4_caches[clean_url]

    def fetch_with_fallback(self, url, headers=None, range_header=None):
        if headers is None:
            headers = {}
        
        if hasattr(headers, 'items'):
            headers = {k: v for k, v in headers.items()}
        
        log(f"🌐 FETCH: Tentando buscar {url[:80]}...")
        
        for attempt in range(MAX_RETRIES):
            if attempt == 0:
                user_agent = CHROME_UA
            else:
                user_agent = self.get_random_user_agent()
            
            origin = get_origin(url)
            req_headers = {
                'User-Agent': user_agent,
                'Accept': '*/*',
                'Accept-Language': 'pt-BR,pt;q=0.9',
                'Connection': 'keep-alive'
            }
            
            if origin:
                req_headers['Origin'] = origin
                req_headers['Referer'] = origin + '/'
            
            for key, value in headers.items():
                if key.lower() not in ['host', 'connection', 'content-length', 'range', 'user-agent', 'accept-encoding']:
                    req_headers[key] = value
            
            if range_header:
                req_headers['Range'] = range_header
                log(f"📊 Range header enviado: {range_header}")
            
            log(f"🔄 Tentativa {attempt + 1}/{MAX_RETRIES} para {url[:60]}...")
            
            try:
                req = Request(url, headers=req_headers)
                
                if url.startswith('https'):
                    response = urlopen(req, timeout=30, context=self.ssl_context)
                else:
                    response = urlopen(req, timeout=30)
                
                status_code = response.getcode()
                content_encoding = response.headers.get('content-encoding', '').lower()
                
                log(f"✅ Sucesso! Status: {status_code}, Encoding: {content_encoding}")
                
                if status_code not in [200, 206]:
                    log(f"⚠️ Status inesperado: {status_code}", 'warning')
                    return None, status_code, None
                
                return response, status_code, content_encoding
                
            except HTTPError as e:
                log(f"❌ HTTP Error {e.code}: {e.reason}", 'error')
                if attempt < MAX_RETRIES - 1 and e.code in [403, 406, 451, 500, 502, 503, 504, 523]:
                    wait = RETRY_DELAY * (attempt + 1)
                    log(f"⏳ Aguardando {wait}s antes de tentar novamente...", 'warning')
                    time.sleep(wait)
                    continue
                return None, e.code, None
                
            except Exception as e:
                log(f"❌ Erro: {str(e)}", 'error')
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_DELAY * (attempt + 1)
                    log(f"⏳ Aguardando {wait}s antes de tentar novamente...", 'warning')
                    time.sleep(wait)
                    continue
                return None, 0, None
        
        log(f"❌ Falha após {MAX_RETRIES} tentativas", 'error')
        return None, 0, None

    def rewrite_m3u8_urls(self, playlist_content, base_url):
        proxy_host = PROXY_HOST
        proxy_port = PROXY_PORT
        
        if proxy_host == '0.0.0.0':
            proxy_host = get_local_ip()
        
        proxy_base = f"http://{proxy_host}:{proxy_port}"
        log(f"📡 Proxy base para M3U8: {proxy_base}")
        
        def proxify(raw_url):
            raw_url = raw_url.strip()
            if not raw_url or raw_url.startswith('#'):
                return raw_url
            try:
                absolute = urljoin(base_url + '/', raw_url)
                if absolute.startswith('http://127.0.0.1') or absolute.startswith('http://localhost') or absolute.startswith(proxy_base):
                    return absolute
                if absolute.startswith(('http://', 'https://')):
                    new_url = f"{proxy_base}/?url={quote(absolute, safe='')}"
                    log(f"🔄 M3U8 URL reescrita: {absolute[:60]} -> {new_url[:60]}")
                    return new_url
            except Exception as e:
                log(f"⚠️ Erro ao reescrever URL: {e}", 'warning')
            return raw_url
        
        lines = []
        for line in playlist_content.split('\n'):
            line = line.rstrip()
            if line and not line.startswith('#'):
                line = proxify(line)
            lines.append(line)
        
        return '\n'.join(lines)

    def handle_channel_stream(self, url, headers):
        self.stream_count += 1
        stream_id = self.stream_count
        
        log_stream(f"🎬 INICIANDO STREAM #{stream_id}")
        log(f"📍 URL: {url}")
        
        cache = self.get_channel_cache(url)
        response = None
        
        try:
            log(f"🔄 Buscando stream #{stream_id}...")
            response, status_code, content_encoding = self.fetch_with_fallback(url, headers, None)
            
            if response is None:
                log(f"⚠️ Stream #{stream_id} - Usando cache de recuperação...", 'warning')
                recovery_chunks = cache.get_recovery_chunks(CACHE_DURATION_SECONDS)
                if recovery_chunks:
                    log(f"✅ Stream #{stream_id} - Enviando {len(recovery_chunks)} chunks do cache")
                    for chunk in recovery_chunks:
                        yield chunk
                        time.sleep(0.03)
                else:
                    log(f"❌ Stream #{stream_id} - Sem dados em cache", 'error')
                return
            
            if response and status_code in [200, 206]:
                content_type = response.headers.get('content-type', '').lower()
                content_url = response.geturl()
                
                log(f"✅ Stream #{stream_id} - Conectado! Status: {status_code}")
                log(f"📄 Content-Type: {content_type}")
                log(f"🔗 URL final: {content_url}")
                
                if 'mpegurl' in content_type or '.m3u8' in content_url.lower():
                    log(f"📺 Stream #{stream_id} - Processando M3U8...")
                    raw_content = response.read()
                    
                    try:
                        if content_encoding == 'gzip':
                            content = gzip.decompress(raw_content)
                            log(f"📦 M3U8 descomprimido (gzip)")
                        elif content_encoding == 'deflate':
                            content = zlib.decompress(raw_content)
                            log(f"📦 M3U8 descomprimido (deflate)")
                        else:
                            content = raw_content
                    except Exception as e:
                        log(f"⚠️ Erro ao descomprimir M3U8: {e}", 'warning')
                        content = raw_content
                    
                    try:
                        playlist_text = content.decode('utf-8', errors='ignore')
                        base_url = content_url.rsplit('/', 1)[0]
                        log(f"📝 M3U8 original: {len(playlist_text)} caracteres")
                        
                        rewritten = self.rewrite_m3u8_urls(playlist_text, base_url)
                        log(f"✅ M3U8 reescrito: {len(rewritten)} caracteres")
                        
                        response.close()
                        log(f"✅ Stream #{stream_id} - M3U8 processado com sucesso!")
                        yield rewritten.encode('utf-8')
                        return
                    except Exception as e:
                        log(f"❌ Erro M3U8: {e}", 'error')
                        return
                
                log(f"📺 Stream #{stream_id} - Iniciando stream contínuo MPEG-TS...")
                cache.stream_started = True
                consecutive_errors = 0
                chunk_count = 0
                
                while True:
                    try:
                        if response:
                            chunk = response.read(BUFFER_SIZE)
                            if chunk:
                                chunk_count += 1
                                if chunk_count % 100 == 0:
                                    log(f"📊 Stream #{stream_id} - {chunk_count} chunks enviados, {cache.total_bytes/1024/1024:.2f} MB")
                                cache.add_chunk(chunk)
                                yield chunk
                                consecutive_errors = 0
                            else:
                                log(f"📊 Stream #{stream_id} - Fim do stream, {chunk_count} chunks enviados")
                                break
                        else:
                            log(f"⚠️ Stream #{stream_id} - Response None, tentando recuperar...", 'warning')
                            cache_chunks = cache.get_continuous_chunks(30)
                            if cache_chunks:
                                log(f"📊 Stream #{stream_id} - Enviando {len(cache_chunks)} chunks do cache")
                                for chunk in cache_chunks:
                                    yield chunk
                                    time.sleep(0.03)
                            
                            try:
                                log(f"🔄 Stream #{stream_id} - Tentando reconectar...")
                                new_response, new_status, _ = self.fetch_with_fallback(
                                    url, headers, "bytes={}-".format(cache.total_bytes)
                                )
                                if new_response and new_status in [200, 206]:
                                    if response:
                                        response.close()
                                    response = new_response
                                    log(f"✅ Stream #{stream_id} - Reconectado com sucesso!")
                                    continue
                            except Exception as e:
                                log(f"❌ Stream #{stream_id} - Erro na reconexão: {e}", 'error')
                            
                            time.sleep(1)
                            
                    except Exception as e:
                        consecutive_errors += 1
                        log(f"⚠️ Stream #{stream_id} - Erro: {e} (erros consecutivos: {consecutive_errors})", 'warning')
                        
                        if consecutive_errors >= 3:
                            log(f"🔄 Stream #{stream_id} - Tentando reconexão após {consecutive_errors} erros...")
                            try:
                                if response:
                                    response.close()
                                    response = None
                                
                                new_response, new_status, _ = self.fetch_with_fallback(
                                    url, headers, "bytes={}-".format(cache.total_bytes)
                                )
                                if new_response and new_status in [200, 206]:
                                    response = new_response
                                    consecutive_errors = 0
                                    log(f"✅ Stream #{stream_id} - Reconectado com sucesso!")
                                    continue
                            except Exception as e:
                                log(f"❌ Stream #{stream_id} - Falha na reconexão: {e}", 'error')
                        
                        if cache.get_continuous_chunks(20):
                            log(f"📊 Stream #{stream_id} - Enviando chunks do cache para recuperação")
                            for chunk in cache.get_continuous_chunks(20):
                                yield chunk
                                time.sleep(0.03)
                
        except Exception as e:
            log(f"❌ Stream #{stream_id} - Erro geral: {e}", 'error')
        finally:
            if response:
                try:
                    response.close()
                    log(f"✅ Stream #{stream_id} - Conexão fechada")
                except:
                    pass
            log_stream(f"🏁 STREAM #{stream_id} FINALIZADO")

    def handle_mp4_stream_response(self, url, method, req_headers):
        log_stream(f"🎬 INICIANDO PROXY MP4 VOD (Suporta Seeking/Avançar)")
        log(f"📍 URL: {url}")
        log(f"📋 Método: {method}")

        # Limpa e formata os headers a serem repassados ao upstream
        headers = {}
        for k, v in req_headers.items():
            k_lower = k.lower()
            if k_lower not in ['host', 'connection', 'content-length', 'accept-encoding']:
                headers[k] = v

        # Força o encoding identity para que o upstream envie os bytes brutos sem compressão (gzipped mp4 quebra playback)
        headers['Accept-Encoding'] = 'identity'

        # Busca e repassa o Range header do cliente se existir (fundamental para seek no IPTV Smarters)
        range_header = req_headers.get('range') or req_headers.get('Range')
        if range_header:
            headers['Range'] = range_header
            log(f"📊 Forwarding Range Header para o upstream: {range_header}")

        response = None
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    stream=True,
                    timeout=30,
                    allow_redirects=True
                )
                if response.status_code in [200, 206]:
                    break
                else:
                    log(f"⚠️ Upstream status inesperado {response.status_code} na tentativa {attempt+1}", "warning")
            except Exception as e:
                log(f"❌ Erro de conexão com upstream na tentativa {attempt+1}: {e}", "error")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))

        if not response or response.status_code not in [200, 206]:
            log(f"❌ Falha crítica ao conectar no upstream MP4 após {MAX_RETRIES} tentativas.", "error")
            return Response("Video temporariamente indisponível", status=502)

        # Copia todos os headers fundamentais vindos da origem
        response_headers = {}
        for k, v in response.headers.items():
            k_lower = k.lower()
            if k_lower in ['content-type', 'content-length', 'content-range', 'accept-ranges', 'etag', 'last-modified']:
                response_headers[k] = v

        # Certifica-se que headers de CORS e conexões permaneçam consistentes
        response_headers['Access-Control-Allow-Origin'] = '*'
        response_headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response_headers['Connection'] = 'keep-alive'

        log(f"✅ Conectado com sucesso. Status upstream: {response.status_code}")
        log(f"📊 Content-Type: {response_headers.get('Content-Type') or response_headers.get('content-type')}")
        log(f"📊 Content-Length: {response_headers.get('Content-Length') or response_headers.get('content-length')}")
        log(f"📊 Content-Range: {response_headers.get('Content-Range') or response_headers.get('content-range')}")

        @stream_with_context
        def generate():
            try:
                for chunk in response.iter_content(chunk_size=BUFFER_SIZE):
                    if chunk:
                        yield chunk
            except Exception as e:
                log(f"❌ Erro na transferência da stream MP4: {e}", "error")
            finally:
                try:
                    response.close()
                except:
                    pass
                log("🏁 Proxy circular e conexão MP4 finalizados.")

        return Response(generate(), status=response.status_code, headers=response_headers)

    def _parse_range(self, range_header):
        if not range_header:
            return None
        match = re.search(r'bytes=(\d+)-(\d*)', range_header)
        if not match:
            return None
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else None
        return start, end

    def _detect_mp4(self, url):
        lower = url.lower()
        if any(ext in lower for ext in ['.mp4', '.mkv', '.webm', '.f4v', '.mov', '.avi']):
            return True
        if '/play/' in lower:
            return True
        if 'xtream' in lower and ('/movie/' in lower or '/series/' in lower):
            return True
        return False

    def get_content_length(self, url):
        cache = self.get_mp4_cache(url)
        if cache.get_total_size():
            return cache.get_total_size()
        
        try:
            req = Request(url, method='HEAD')
            req.add_header('User-Agent', CHROME_UA)
            
            if url.startswith('https'):
                response = urlopen(req, timeout=10, context=self.ssl_context)
            else:
                response = urlopen(req, timeout=10)
            
            content_length = response.headers.get('Content-Length')
            if content_length:
                size = int(content_length)
                cache.content_length = size
                return size
        except Exception as e:
            log(f"⚠️ Erro ao obter content-length: {e}", 'warning')
        
        return None

# ==================== INICIALIZAÇÃO ====================
stream_proxy = UnifiedStreamProxy()

# ==================== FUNÇÕES DO FLASK ====================
def log_request():
    print("\n" + "=" * 80)
    print(f"[{datetime.now()}]")
    print(f"Método: {request.method}")
    print(f"URL: {request.url}")
    print(f"Path: {request.path}")
    print(f"Query: {request.query_string.decode()}")
    print(f"IP: {request.remote_addr}")

    print("\nHeaders:")
    for k, v in request.headers.items():
        print(f"  {k}: {v}")

    print("=" * 80 + "\n")

def modify_xtream_response(content, path, username, password):
    try:
        if isinstance(content, bytes):
            try:
                content_str = content.decode('utf-8')
            except:
                content_str = content.decode('latin-1', errors='ignore')
        else:
            content_str = content
        
        content_stripped = content_str.strip()
        is_json = (content_stripped.startswith('{') or content_stripped.startswith('['))
        
        if is_json:
            try:
                data = json.loads(content_stripped)
                if 'user_info':
                    data['user_info']['username'] = username
                    data['user_info']['password'] = password
                
                if 'server_info' not in data:
                    data['server_info'] = {
                        'url': f"http://{PROXY_HOST}:{PROXY_PORT}",
                        'port': PROXY_PORT
                    }
                elif isinstance(data['server_info'], dict):
                    data['server_info']['url'] = f"http://{PROXY_HOST}:{PROXY_PORT}"
                    data['server_info']['port'] = PROXY_PORT
                
                log(f"📝 Resposta player_api.php modificada com server_info")
                return json.dumps(data), 'application/json'
            
            except json.JSONDecodeError as e:
                logging.warning(f"Erro ao decodificar JSON: {e}")
                return content_str, 'application/json'
        
        return content_str, 'text/plain'
    
    except Exception as e:
        logging.error(f"Erro ao modificar resposta: {e}")
        return content, 'text/plain'

# ==================== ROTAS FLASK ====================
@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
def proxy(path):
    log_request()

    # ============================================
    # FORMATO /?url=
    # ============================================
    if path == '' and request.args.get('url'):
        url = request.args.get('url')
        print("\n" + "🌐" * 40)
        print(f"🌐 PROXY UNIFICADO (formato /?url=)")
        print(f"🌐 URL: {url}")
        print("🌐" * 40 + "\n")
        
        headers_dict = {k: v for k, v in request.headers.items()}
        method = request.method
        
        if stream_proxy._detect_mp4(url):
            return stream_proxy.handle_mp4_stream_response(url, method, headers_dict)
        else:
            @stream_with_context
            def generate_stream():
                for chunk in stream_proxy.handle_channel_stream(url, headers_dict):
                    yield chunk
            
            if '.m3u8' in url.lower():
                content_type = 'application/vnd.apple.mpegurl'
            else:
                content_type = 'video/mp2t'
            
            return Response(
                generate_stream(),
                status=200,
                headers={
                    'Content-Type': content_type,
                    'Access-Control-Allow-Origin': '*',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                }
            )

    # ============================================
    # STREAMS (live, movie, series)
    # ============================================
    is_stream = False
    stream_type = None
    
    if path.startswith('live/') or '/live/' in path:
        is_stream = True
        stream_type = 'LIVE'
    elif path.startswith('movie/') or '/movie/' in path:
        is_stream = True
        stream_type = 'MOVIE'
    elif path.startswith('series/') or '/series/' in path:
        is_stream = True
        stream_type = 'SERIES'
    
    if is_stream:
        if path.startswith('http://') or path.startswith('https://'):
            target_url = path
        else:
            target_url = f"{TARGET}/{path}"
        
        if request.query_string:
            target_url += "?" + request.query_string.decode()

        # adaptar para oneplay
        if 'lista1' in target_url.lower():
            
            dados = select_server(0)
            target_url = target_url.replace('example', dados['host_base']).replace('lista1', dados['username']).replace('12345', dados['password'])
        if 'lista2' in target_url.lower():
            dados = select_server(1)
            target_url = target_url.replace('example', dados['host_base']).replace('lista2', dados['username']).replace('12345', dados['password'])
        if 'lista3' in target_url.lower():
            dados = select_server(2)
            target_url = target_url.replace('example', dados['host_base']).replace('lista3', dados['username']).replace('12345', dados['password']) 
        if 'lista4' in target_url.lower():
            dados = select_server(3)
            target_url = target_url.replace('example', dados['host_base']).replace('lista4', dados['username']).replace('12345', dados['password'])
        if 'lista5' in target_url.lower():
            dados = select_server(4)
            target_url = target_url.replace('example', dados['host_base']).replace('lista5', dados['username']).replace('12345', dados['password'])  
        if 'lista6' in target_url.lower():
            dados = select_server(5)
            target_url = target_url.replace('example', dados['host_base']).replace('lista6', dados['username']).replace('12345', dados['password']) 
        if 'lista7' in target_url.lower():
            dados = select_server(6)
            target_url = target_url.replace('example', dados['host_base']).replace('lista7', dados['username']).replace('12345', dados['password']) 
        if 'lista8' in target_url.lower():
            dados = select_server(7)
            target_url = target_url.replace('example', dados['host_base']).replace('lista8', dados['username']).replace('12345', dados['password'])                                                             


        
        print("\n" + "🔥" * 40)
        print(f"🔥 STREAM INTERCEPTADO PELO PROXY UNIFICADO!")
        print(f"🔥 Tipo: {stream_type}")
        print(f"🔥 URL Original: {target_url}")
        print(f"🔥 Path: {path}")
        print("🔥" * 40 + "\n")
        
        headers_dict = {k: v for k, v in request.headers.items()}
        method = request.method
        
        if stream_proxy._detect_mp4(target_url):
            return stream_proxy.handle_mp4_stream_response(target_url, method, headers_dict)
        else:
            log(f"📺 Stream detectado como MPEG-TS/M3U8")
            
            @stream_with_context
            def generate_stream():
                for chunk in stream_proxy.handle_channel_stream(target_url, headers_dict):
                    yield chunk
            
            if '.m3u8' in target_url.lower():
                content_type = 'application/vnd.apple.mpegurl'
                log(f"📄 Content-Type: M3U8")
            else:
                content_type = 'video/mp2t'
                log(f"📄 Content-Type: MPEG-TS")
            
            return Response(
                generate_stream(),
                status=200,
                headers={
                    'Content-Type': content_type,
                    'Access-Control-Allow-Origin': '*',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                }
            )

    # ============================================
    # OUTROS FORMATOS (/tsdownloader, /http://, etc)
    # ============================================
    if '/tsdownloader' in path and '?url=' in path:
        url = extract_url_from_path(path)
        if url:
            print("\n" + "🌐" * 40)
            print(f"🌐 PROXY UNIFICADO (formato /tsdownloader)")
            print(f"🌐 URL: {url}")
            print("🌐" * 40 + "\n")
            
            headers_dict = {k: v for k, v in request.headers.items()}
            method = request.method
            
            if stream_proxy._detect_mp4(url):
                return stream_proxy.handle_mp4_stream_response(url, method, headers_dict)
            else:
                @stream_with_context
                def generate_stream():
                    for chunk in stream_proxy.handle_channel_stream(url, headers_dict):
                        yield chunk
                
                return Response(
                    generate_stream(),
                    status=200,
                    headers={
                        'Content-Type': 'video/mp2t',
                        'Access-Control-Allow-Origin': '*',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                )

    if path.startswith('/http://') or path.startswith('/https://'):
        url = extract_url_from_path(path)
        if url:
            print("\n" + "🌐" * 40)
            print(f"🌐 PROXY UNIFICADO (formato /http://)")
            print(f"🌐 URL: {url}")
            print("🌐" * 40 + "\n")
            
            headers_dict = {k: v for k, v in request.headers.items()}
            method = request.method
            
            if stream_proxy._detect_mp4(url):
                return stream_proxy.handle_mp4_stream_response(url, method, headers_dict)
            else:
                @stream_with_context
                def generate_stream():
                    for chunk in stream_proxy.handle_channel_stream(url, headers_dict):
                        yield chunk
                
                return Response(
                    generate_stream(),
                    status=200,
                    headers={
                        'Content-Type': 'video/mp2t',
                        'Access-Control-Allow-Origin': '*',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                )

    # ============================================
    # API
    # ============================================
    target_url = f"{TARGET}/{path}"
    if request.query_string:
        target_url += "?" + request.query_string.decode()
    # adaptar para oneplay
    if 'lista1' in target_url.lower():
        dados = select_server(0)
        target_url = target_url.replace('example', dados['host_base']).replace('lista1', dados['username']).replace('12345', dados['password'])
        username = 'lista1'
        password = '12345'
    if 'lista2' in target_url.lower():
        dados = select_server(1)
        target_url = target_url.replace('example', dados['host_base']).replace('lista2', dados['username']).replace('12345', dados['password'])
        username = 'lista2'
        password = '12345'
    if 'lista3' in target_url.lower():
        dados = select_server(2)
        target_url = target_url.replace('example', dados['host_base']).replace('lista3', dados['username']).replace('12345', dados['password'])
        username = 'lista3'
        password = '12345'
    if 'lista4' in target_url.lower():
        dados = select_server(3)
        target_url = target_url.replace('example', dados['host_base']).replace('lista4', dados['username']).replace('12345', dados['password'])
        username = 'lista4'
        password = '12345'
    if 'lista5' in target_url.lower():
        dados = select_server(4)
        target_url = target_url.replace('example', dados['host_base']).replace('lista5', dados['username']).replace('12345', dados['password'])
        username = 'lista5'
        password = '12345'
    if 'lista6' in target_url.lower():
        dados = select_server(5)
        target_url = target_url.replace('example', dados['host_base']).replace('lista6', dados['username']).replace('12345', dados['password'])
        username = 'lista6'
        password = '12345'
    if 'lista7' in target_url.lower():
        dados = select_server(6)
        target_url = target_url.replace('example', dados['host_base']).replace('lista7', dados['username']).replace('12345', dados['password'])
        username = 'lista7'
        password = '12345'
    if 'lista8' in target_url.lower():
        dados = select_server(7)
        target_url = target_url.replace('example', dados['host_base']).replace('lista8', dados['username']).replace('12345', dados['password'])
        username = 'lista8'
        password = '12345'
    
    headers = {k: v for k, v in request.headers.items() if k.lower() not in ['host', 'accept-encoding']}
    headers['Accept-Encoding'] = 'identity'
    
    try:
        log(f"🔄 API Request: {target_url}")
        
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            timeout=30,
            stream=False
        )
        
        log(f"✅ API Response: Status {resp.status_code}")
        
        content = resp.content
        
        if 'player_api.php' in path:
            log(f"📝 Modificando resposta do player_api.php")
            
            modified_content, modified_content_type = modify_xtream_response(content, path, username, password)
            
            if isinstance(modified_content, str):
                content = modified_content.encode('utf-8')
            else:
                content = modified_content
            
            excluded = {'content-encoding', 'content-length', 'transfer-encoding', 'connection'}
            response_headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in excluded]
            response_headers.append(('X-Proxy', 'xtream-codes-proxy'))
            response_headers = [(k, v) for k, v in response_headers if k.lower() != 'content-encoding']
            response_headers = [(k, v) if k.lower() != 'content-type' else ('Content-Type', 'application/json; charset=utf-8') 
                              for k, v in response_headers]
            
            return Response(content, status=resp.status_code, headers=response_headers)
        else:
            excluded = {'content-encoding', 'content-length', 'transfer-encoding', 'connection'}
            response_headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in excluded]
            
            return Response(content, status=resp.status_code, headers=response_headers)
    
    except Exception as e:
        log(f"❌ Erro no proxy: {e}", 'error')
        return {"success": False, "error": str(e)}, 500

def extract_url_from_path(path):
    if '/?url=' in path:
        url_part = path.split('/?url=', 1)[1]
        if '&' in url_part:
            url_part = url_part.split('&', 1)[0]
        if ' ' in url_part:
            url_part = url_part.split(' ', 1)[0]
        return unquote(url_part)
    
    if '/tsdownloader' in path and '?url=' in path:
        params = path.split('?', 1)[1]
        for param in params.split('&'):
            if param.startswith('url='):
                return unquote(param[4:])
    
    if path.startswith('/http://') or path.startswith('/https://'):
        return unquote(path[1:])
    
    if path.startswith('http://') or path.startswith('https://'):
        return unquote(path)
    
    return None

@app.route('/health', methods=['GET'])
def health():
    return {
        "status": "ok",
        "proxy": f"http://{PROXY_HOST}:{PROXY_PORT}",
        "local_ip": LOCAL_IP,
        "cache": {
            "channels": len(stream_proxy.channel_caches),
            "mp4": len(stream_proxy.mp4_caches)
        },
        "streams_served": stream_proxy.stream_count
    }

def run_service():
    print(f"""
    ╔═══════════════════════════════════════════════╗
    ║  FREE IPTV                                    ║
    ╠═══════════════════════════════════════════════╣
    ╠══ LOGIN: ═════════════════════════════════════╣
    ║  USERNAME: lista1, lista2, ..., lista8        ║
    ║  PASSWORD: 123456                             ║
    ║  HOST:  http://{PROXY_HOST}:{PROXY_PORT:<36}  ║
    ╚═══════════════════════════════════════════════╝
    
    🔥 AGUARDANDO CONEXÕES...
    """)
    
    app.run(
        host="0.0.0.0",
        port=PROXY_PORT,
        threaded=True,
        debug=True
    )

 
