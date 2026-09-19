import json, sys, time, os
from selenium import webdriver
from selenium.webdriver.firefox.service import Service

def run(url, timeout=90):
    opts = webdriver.FirefoxOptions()
    if os.environ.get('HEADLESS', '1') == '1':
        opts.add_argument('-headless')
    svc = Service('/opt/homebrew/bin/geckodriver')
    d = webdriver.Firefox(service=svc, options=opts)
    d.set_page_load_timeout(30)
    try:
        d.get(url)
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                if d.title == 'DONE':
                    break
            except Exception:
                pass
            time.sleep(1)
        res = d.execute_script('return window.R ? JSON.stringify(window.R) : "{}";')
        return json.loads(res)
    finally:
        d.quit()

if __name__ == '__main__':
    url = sys.argv[1]
    print(json.dumps(run(url), indent=1))
