import json, sys, time, os
from playwright.sync_api import sync_playwright

def run(url, timeout=90):
    with sync_playwright() as p:
        b = p.chromium.launch(headless=(os.environ.get('HEADLESS', '1') == '1'))
        pg = b.new_page()
        pg.goto(url, timeout=30000)
        t0 = time.time()
        while time.time() - t0 < timeout:
            if pg.title() == 'DONE':
                break
            time.sleep(1)
        res = pg.evaluate('window.R ? JSON.stringify(window.R) : "{}"')
        b.close()
        return json.loads(res)

if __name__ == '__main__':
    print(json.dumps(run(sys.argv[1]), indent=1))
