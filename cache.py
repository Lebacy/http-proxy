import threading

from collections import OrderedDict

class LRUCache:
    def __init__(self, max_object_size, max_cache_size):
        self.max_object_size = max_object_size
        self.max_cache_size = max_cache_size
        self.cache = OrderedDict()
        self.current_size = 0
        self.lock = threading.Lock()

    def normalize_url(self, host, port, path):
        scheme_host = host.lower()
        if port == 80:
            port_str = ""
        else:
            port_str = f":{port}"
        if not path:
            path = "/"
        return f"http://{scheme_host}{port_str}{path}"

    def get(self, key):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def put(self, key, value):
        size = len(value)
        if size > self.max_object_size:
            return
        with self.lock:
            while self.current_size + size > self.max_cache_size:
                _, v = self.cache.popitem(last=False)
                self.current_size -= len(v)
            self.cache[key] = value
            self.current_size += size
