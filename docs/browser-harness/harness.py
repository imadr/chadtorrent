import http.server, urllib.request, urllib.error, threading, sys

UP = 'http://127.0.0.1:8971'
HTML = open('page.html', encoding='utf-8').read()

class H(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def do_GET(self):
        if self.path == '/' or self.path.startswith('/?'):
            body = HTML.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        url = UP + self.path
        try:
            r = urllib.request.urlopen(url)
            self.send_response(r.status)
            ct = r.headers.get('Content-Type')
            if ct: self.send_header('Content-Type', ct)
            self.send_header('Connection', 'close')
            self.end_headers()
            while True:
                c = r.read(64 * 1024)
                if not c: break
                self.wfile.write(c)
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            self.end_headers()
            self.wfile.write(e.read())
        except Exception:
            pass
    def log_message(self, *a): pass

if __name__ == '__main__':
    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 8972), H)
    print('harness on :8972', flush=True)
    srv.serve_forever()
