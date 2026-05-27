import socket
import sys
import select
import threading
import time
import datetime

from io import BytesIO
from cache import LRUCache

ZID = "z5480035"
BUFFER_SIZE = 8192
CLIENT_TIMEOUT = 10.0
SERVER_TIMEOUT = 2.0

proxy_host = None
proxy_port = None

def send_error_response(client_socket, status_code, reason, message):
    body = f"{status_code} {reason}\n\n{message}".encode("utf-8")
    response = (
        f"HTTP/1.1 {status_code} {reason}\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("utf-8") + body
    try:
        client_socket.sendall(response)
    except:
        pass

def log_transaction(addr, cache_result, request_line, status, obj_size):
    host, port = addr
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    timestamp = now.strftime("[%d/%b/%Y:%H:%M:%S %z]")
    cache_char = cache_result if cache_result else "-"
    log_line = f"{host} {port} {cache_char} {timestamp} \"{request_line}\" {status} {obj_size}"
    print(log_line)

cache = None

def parse_request(stream):
    request = stream.read(BUFFER_SIZE)
    headers_end = request.find(b"\r\n\r\n") + 4
    header_part = request[:headers_end].decode("iso-8859-1")
    body_part = request[headers_end:]

    lines = header_part.split("\r\n")
    request_line = lines[0].strip()
    method, full_url, version = request_line.split()

    if not full_url.startswith("http://"):
        raise ValueError("Invalid URL")
    url = full_url[len("http://"):]
    path_index = url.find("/")
    if path_index == -1:
        host_port = url
        path = "/"
    else:
        host_port = url[:path_index]
        path = url[path_index:]

    if not host_port:
        raise ValueError("no host")

    if ":" in host_port:
        host, port = host_port.split(":")
        port = int(port)
    else:
        host = host_port
        port = 80

    headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()

    content_length = int(headers.get("Content-Length", "0"))
    body = body_part
    while len(body) < content_length:
        chunk = stream.read(BUFFER_SIZE)
        if not chunk:
            break
        body += chunk

    return method, path, version, headers, body, host, port

def build_request(method, path, version, headers, body, host):
    headers.pop("Proxy-Connection", None)
    if "Connection" not in headers:
        headers["Connection"] = "close"
    headers["Host"] = headers.get("Host", host)
    if "Via" in headers:
        headers["Via"] += f", 1.1 {ZID}"
    else:
        headers["Via"] = f"1.1 {ZID}"

    request = f"{method} {path} {version}\r\n"
    for k, v in headers.items():
        request += f"{k}: {v}\r\n"
    request += "\r\n"

    return request.encode("iso-8859-1") + body

def transform_response(response_bytes, keep_alive):
    headers_end = response_bytes.find(b"\r\n\r\n") + 4
    header_part = response_bytes[:headers_end].decode("iso-8859-1")
    body = response_bytes[headers_end:]

    lines = header_part.split("\r\n")
    status_line = lines[0]
    status_code = int(status_line.split()[1])
    headers = {}
    header_order = []

    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()
            header_order.append(k.strip())

    headers["Connection"] = "keep-alive" if keep_alive else "close"
    headers["Via"] = f"1.1 {ZID}" + (", " + headers["Via"] if "Via" in headers else "")

    response_text = status_line + "\r\n"
    for k in header_order:
        if k in headers:
            response_text += f"{k}: {headers[k]}\r\n"
            del headers[k]
    for k, v in headers.items():
        response_text += f"{k}: {v}\r\n"
    response_text += "\r\n"

    return response_text.encode("iso-8859-1") + body, status_code, len(body)


def forward(src, dst):
    try:
        while True:
            data = src.recv(BUFFER_SIZE)
            if not data:
                break
            dst.sendall(data)
    except:
        pass

def handle_connect(client_socket, request_line):
    try:
        _, target, _ = request_line.split()
        if ':' not in target:
            send_error_response(client_socket, 400, "Bad Request", "invalid port")
            return
        host, port = target.split(":")
        port = int(port)
        if port != 443:
            send_error_response(client_socket, 400, "Bad Request", "invalid port")
            return

        server_socket = socket.create_connection((host, port))
        client_socket.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")

        t1 = threading.Thread(target=forward, args=(client_socket, server_socket))
        t2 = threading.Thread(target=forward, args=(server_socket, client_socket))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except Exception as e:
        print(f"[CONNECT ERROR] {e}")
    finally:
        for s in [client_socket, server_socket]:
            try:
                s.shutdown(socket.SHUT_RDWR)
                s.close()
            except:
                pass

def handle_client(client_socket, addr):
    client_socket.settimeout(CLIENT_TIMEOUT)
    try:
        while True:
            try:
                initial = b""
                while b"\r\n" not in initial:
                    chunk = client_socket.recv(1)
                    if not chunk:
                        return
                    initial += chunk
                request_line = initial.decode("iso-8859-1").strip()

                rest = b""
                while b"\r\n\r\n" not in rest:
                    chunk = client_socket.recv(BUFFER_SIZE)
                    if not chunk:
                        return
                    rest += chunk
                full_request = initial + rest

                if request_line.startswith("CONNECT"):
                    handle_connect(client_socket, request_line)
                    return

                stream = BytesIO(full_request)
                method, path, version, headers, body, host, port = parse_request(stream)

                
                proxy_ips = {proxy_host, "127.0.0.1", "localhost"}
                if host in proxy_ips and port == proxy_port:
                    send_error_response(client_socket, 421, "Misdirected Request", "proxy address")
                    return

            except socket.timeout:
                return
            except ValueError as ve:
                msg = str(ve)
                if msg == "no host":
                    send_error_response(client_socket, 400, "Bad Request", "no host")
                else:
                    send_error_response(client_socket, 400, "Bad Request", msg)
                return

            keep_alive = headers.get("Connection", "").lower() != "close"
            cache_key = cache.normalize_url(host, port, path)

            # Handle GET cache lookup
            if method == "GET":
                cached_response = cache.get(cache_key)
                if cached_response:
                    client_socket.sendall(cached_response)
                    log_transaction(addr, "H", request_line, 200, len(cached_response.split(b"\r\n\r\n", 1)[1]))
                    if not keep_alive:
                        break
                    continue

            request_bytes = build_request(method, path, version, headers, body, host)

            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    try:
                        server_socket.connect((host, port))
                    except socket.gaierror:
                        send_error_response(client_socket, 502, "Bad Gateway", "could not resolve")
                        return
                    except ConnectionRefusedError:
                        send_error_response(client_socket, 502, "Bad Gateway", "connection refused")
                        return

                    server_socket.sendall(request_bytes)
                    server_socket.settimeout(SERVER_TIMEOUT)
                    response = b""
                    try:
                        while True:
                            data = server_socket.recv(BUFFER_SIZE)
                            if not data:
                                break
                            response += data
                    except socket.timeout:
                        if not response:
                            send_error_response(client_socket, 504, "Gateway Timeout", "timed out")
                            return
                    except Exception:
                        send_error_response(client_socket, 502, "Bad Gateway", "closed unexpectedly")
                        return
            except Exception:
                break

            transformed, status_code, body_length = transform_response(response, keep_alive)
            client_socket.sendall(transformed)

            # Cache only 200 GET responses that fit
            if method == "GET" and status_code == 200 and body_length <= cache.max_object_size:
                cache.put(cache_key, transformed)
                log_transaction(addr, "M", request_line, status_code, body_length)
            else:
                log_transaction(addr, "-", request_line, status_code, body_length)

            if not keep_alive:
                break
    finally:
        client_socket.close()


def start_proxy(port):
    global proxy_host, proxy_port

    proxy_host = socket.gethostname()
    proxy_port = port
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as proxy_socket:
        proxy_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        proxy_socket.bind(("127.0.0.1", port))
        proxy_socket.listen(20)
        print(f"Proxy running on port {port}...")

        while True:
            try:
                client_socket, addr = proxy_socket.accept()
                print(f"Connection from {addr}")
                thread = threading.Thread(target=handle_client, args=(client_socket, addr))
                thread.daemon = True
                thread.start()
            except KeyboardInterrupt:
                print("Shutting down proxy...")
                break
            except Exception as e:
                print(f"[ERROR] Failed to accept connection: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: python3 {sys.argv[0]} <port> <max_object_size> <max_cache_size>")
        sys.exit(1)
    port = int(sys.argv[1])
    max_object_size = int(sys.argv[2])
    max_cache_size = int(sys.argv[3])
    cache = LRUCache(max_object_size, max_cache_size)
    start_proxy(port)
