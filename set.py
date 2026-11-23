#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SET Factsheet Scraper (Local + GitHub Actions Ready)
====================================================
ดึงข้อมูล Factsheet จากตลาดหลักทรัพย์แห่งประเทศไทย (SET) แบบ Parallel Processing
รองรับการรันทั้งบนเครื่อง Local (Windows/macOS/Linux) และ GitHub Actions (ubuntu-latest)

การใช้งาน:
    python set.py
    python set.py --workers 3
    python set.py --test-stocks ADVANC,PTT

ขั้นตอน:
    1. ดาวน์โหลดรายชื่อหุ้นจาก SET (listedCompanies_th_TH.xls -> listedCompanies_th_TH.csv)
    2. ดึงข้อมูล Factsheet แบบ Multi-threaded แยกตามกลุ่มอุตสาหกรรม
    3. ตรวจสอบและ Retry ข้อมูลที่ขาดหาย (Script A -> B -> C -> D -> E)
"""

import argparse
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from io import StringIO
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
import urllib3
from lxml import html as lxml_html
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ปิด SSL Warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ให้แสดงผล UTF-8 และ flush log ทันที (ป้องกัน error ภาษาไทย/emoji และช่วยให้เห็น log real-time บน GitHub Actions)
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    except Exception:
        pass

# ==========================================
# CONFIGURATION
# ==========================================
IS_CI = os.getenv("GITHUB_ACTIONS", "").lower() == "true"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5" if IS_CI else "6"))
HEADLESS_MODE = os.getenv("HEADLESS_MODE", "true").lower() != "false"
WAIT_TIMEOUT = int(os.getenv("WAIT_TIMEOUT", "10"))
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "45"))
MAX_URLS_PER_DRIVER = int(os.getenv("MAX_URLS_PER_DRIVER", "100"))
MAX_RETRY_PER_URL = int(os.getenv("MAX_RETRY_PER_URL", "1"))

# Path Configuration (ใช้ตำแหน่งไฟล์ set.py เป็นหลัก จึงรันได้ทั้ง Local และ GitHub Actions)
BASE_DIR = Path(__file__).resolve().parent
LOCAL_PATH = str(BASE_DIR)
LOCAL_PATH_DATA = str(BASE_DIR / "data_factsheet")

# Lock ป้องกัน Race Condition ตอนสร้าง Driver (แก้ปัญหา se-metadata.json error)
DRIVER_INIT_LOCK = threading.Lock()

# ==========================================
# XPATH MAPPINGS
# ==========================================
XPATH_MAPPINGS = {
    "Last Update": '//div[@class="card-product card-product shadow-none"]//div[@class="card-product-body d-flex flex-column"]',
    "Nature Business": '//div[@class="nature-business"]//div[@class="business-col col-12"]',
    "Card Minimal Left": '//div[@class="price-info-wrapper"]//div[@class="card-minimal"]//div//div[@class="price-left-col"]',
    "Card Minimal Right": '//div[@class="price-info-wrapper"]//div[@class="card-minimal"]//div//div[@class="price-right-col"]',
    "Company Info": '//div[@class="company-info d-flex flex-column me-auto"]',
    "Basic Information": '//div[@class="row issuer-info border-info"]',
    "Annual Report": '//div[@class="row issuer-info border-info"]//div[@class="basic-content align-self-baseline"]//div[label[contains(text(), "รายงานประจำปี")]]//div//a',
    "Basic Content 1": '//div[@class="col-6 basic-content align-self-baseline"]',
    "Basic Content 2": '//div[@class="col-6 mb-2 basic-content align-self-baseline"]',
    "Dividend Policy": '//div[@class="d-flex flex-wrap text-dark text-policy fs-12px pe-3 ps-3 py-1"]',
    "Auditor": '//div[@class="auditor col-basic-info col-12 col-lg-6 mb-3 mb-lg-0 px-4"]',
    "News Wrapper": '//div[@class="row news-wrapper"]',
    "Name": '//h1[@class="company-name title-font-family fs-24px"]',
    "Card Holder": '//div[@class="card-holder"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Card Board": '//div[@class="card-board"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Adjust Price": '//div[@class="card-history"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Stat": '//div[@class="card-stat"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Return": '//div[@class="card-yield"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Dividend": '//div[@class="card-dividend"]//div[@class="card-minimal mb-3"]//div[@class="card-minimal-body mb-0 p-0"]',
    "FinanStateSubmit": '//div[@class="FinanStateSubmit col-basic-info col-12 col-lg-6 mb-3 mb-lg-0"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "BS": '(//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-1 table-lg-noresponsive"])[1]',
    "PL": '(//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-1 table-lg-noresponsive"])[2]',
    "CF": '(//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-1 table-lg-noresponsive"])[3]',
    "Ratio": '//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-5 table-lg-noresponsive"]',
    "Change": '//div[@class="factsheet-financial mt-2"]//div[@class="row row-mini-table"]//div[@class="col-12 col-lg-6 rate-change"]',
    "Cycle": '//div[@class="factsheet-financial mt-2"]//div[@class="row row-mini-table"]//div[@class="col-12 col-lg-6 cash-cycle"]',
    "Cap": '(//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6 mb-lg-0"]//div[@class="card-minimal mb-4"])[1]',
    "Sign": '(//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6 mb-lg-0"]//div[@class="card-minimal mb-4"])[2]',
    "Oth": '//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6"]//div[@class="card-minimal mb-4"]',
    "Alert": '//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6"]//div[@class="card-minimal"]',
}

SECTOR_BASES = [
    "agro_company",
    "consump_company",
    "fincial_company",
    "indus_company",
    "propcon_company",
    "resourc_company",
    "service_company",
    "tech_company",
    "na_company",
]

GROUP_COL1 = [
    "Return",
    "Stat",
    "Card Holder",
    "Card Board",
    "Adjust Price",
    "Ratio",
    "FinanStateSubmit",
]
GROUP_COL3 = ["BS", "PL", "CF"]


# ==========================================
# HELPER FUNCTIONS
# ==========================================


def kill_orphan_chrome():
    """ล้าง Process Chrome/ChromeDriver ที่อาจค้างอยู่ (เฉพาะ Linux / GitHub Actions)"""
    if not sys.platform.startswith("linux"):
        return
    for proc_name in ("chrome", "chromedriver"):
        try:
            subprocess.run(["pkill", "-f", proc_name], stderr=subprocess.DEVNULL, check=False)
        except Exception:
            pass


def find_chrome_binary():
    """ค้นหา path ของ Google Chrome / Chromium ให้รองรับทั้ง Windows, Linux และ macOS"""
    env_chrome = os.getenv("CHROME_BIN")
    if env_chrome and os.path.isfile(env_chrome):
        return env_chrome

    if sys.platform == "win32":
        win_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        ]
        return next((p for p in win_paths if os.path.isfile(p)), None)

    for cmd in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"):
        found = shutil.which(cmd)
        if found:
            return found

    mac_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if os.path.isfile(mac_path):
        return mac_path

    return None


def make_driver():
    """สร้าง WebDriver instance (รองรับทั้ง Linux/GitHub Actions และ Windows)"""
    chrome_binary = find_chrome_binary()

    # บน Linux / macOS หรือเมื่อเจอ Chrome บน Windows -> ใช้ Chrome (Selenium Manager จัดการ ChromeDriver ให้เอง)
    if chrome_binary or sys.platform != "win32":
        opts = webdriver.ChromeOptions()
        if chrome_binary:
            opts.binary_location = chrome_binary
        if HEADLESS_MODE:
            opts.add_argument("--headless=new")

        # ปรับแต่งเพื่อความเร็วและเสถียรภาพบน CI
        opts.page_load_strategy = "eager"  # คืนสิทธิ์ทันทีที่ DOM พร้อม
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--blink-settings=imagesEnabled=false")
        opts.add_argument("--ignore-certificate-errors")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--log-level=3")
        opts.add_argument("--disable-logging")

        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        opts.add_experimental_option("prefs", prefs)

        max_init_retries = 3
        last_err = None
        for attempt in range(1, max_init_retries + 1):
            try:
                with DRIVER_INIT_LOCK:
                    service = webdriver.ChromeService(log_path=os.devnull)
                    driver = webdriver.Chrome(options=opts, service=service)
                
                return driver
            except Exception as e:
                last_err = e
                print(f"⚠️ Attempt {attempt}/{max_init_retries}: Failed to start Chrome ({e})")
                time.sleep(5)

        raise RuntimeError(f"ไม่สามารถเปิด Chrome ได้หลังลอง {max_init_retries} ครั้ง: {last_err}")

    # Edge Fallback (เฉพาะ Windows กรณีไม่มี Chrome)
    edge_paths = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge\Application\msedge.exe"),
    ]
    edge_binary = next((p for p in edge_paths if os.path.isfile(p)), None)
    if edge_binary:
        from selenium.webdriver.edge.options import Options as EdgeOptions
        from selenium.webdriver.edge.webdriver import WebDriver as EdgeDriver

        eopts = EdgeOptions()
        eopts.page_load_strategy = "eager"
        if HEADLESS_MODE:
            eopts.add_argument("--headless=new")
        eopts.add_argument("--disable-gpu")
        eopts.add_argument("--no-sandbox")
        with DRIVER_INIT_LOCK:
            driver = EdgeDriver(options=eopts)
                driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                # บล็อกทรัพยากรที่ไม่จำเป็น (รูป/ฟอนต์/ตัวติดตาม) ช่วยให้หน้าเว็บโหลดเร็วขึ้นมาก
                try:
                    driver.execute_cdp_cmd("Network.enable", {})
                    driver.execute_cdp_cmd(
                        "Network.setBlockedURLs",
                        {
                            "urls": [
                                "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg", "*.ico",
                                "*.woff", "*.woff2", "*.ttf", "*.eot",
                                "*google-analytics*", "*googletagmanager*", "*doubleclick*",
                                "*facebook*", "*hotjar*", "*clarity.ms*",
                            ]
                        },
                    )
                except Exception:
                    pass
                return driver

    raise RuntimeError("ไม่พบ Chrome หรือ Edge ในระบบ")

def verify_driver():
    """ทดสอบเปิด Chrome 1 ครั้งก่อนเริ่มงานจริง (Fail-fast + อุ่น Selenium Manager cache)"""
    print("🧪 กำลังทดสอบเปิด WebDriver...")
    driver = make_driver()
    try:
        browser_ver = driver.capabilities.get("browserVersion", "unknown")
        driver_ver = (
            driver.capabilities.get("chrome", {}).get("chromedriverVersion", "").split(" ")[0]
            or "unknown"
        )
        print(f"✅ WebDriver พร้อมใช้งาน (Browser: {browser_ver}, Driver: {driver_ver})")
    finally:
        try:
            driver.quit()
        except Exception:
            pass

def setup_environment():
    """เตรียม Folder และดาวน์โหลด/แปลงไฟล์ Master จาก SET เป็น CSV"""
    print("🔧 กำลังเตรียม Environment...")
    if os.path.exists(LOCAL_PATH_DATA):
        shutil.rmtree(LOCAL_PATH_DATA)
    os.makedirs(LOCAL_PATH_DATA, exist_ok=True)
    print(f"✅ สร้างโฟลเดอร์ {LOCAL_PATH_DATA} แล้ว")

    xls_path = os.path.join(LOCAL_PATH, "listedCompanies_th_TH.xls")
    csv_path = os.path.join(LOCAL_PATH, "listedCompanies_th_TH.csv")

    for p in (xls_path, csv_path):
        if os.path.exists(p):
            os.remove(p)

    # 1. Download (พร้อม Retry เผื่อเครือข่ายสะดุด)
    url = "https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_th_TH.xls"
    print(f"📥 กำลังดาวน์โหลด {url}...")

    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Referer": "https://www.set.or.th/",
    }

    resp = None
    for attempt in range(1, 4):
        try:
            resp = session.get(url, headers=headers, verify=False, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            print(f"⚠️ ดาวน์โหลดรอบ {attempt}/3 ล้มเหลว: {e}")
            if attempt == 3:
                raise
            time.sleep(5)

    with open(xls_path, "wb") as f:
        f.write(resp.content)

    print("🔄 กำลังแปลงไฟล์ HTML (TIS-620) เป็น CSV...")

    # 2. Decode ภาษาไทย TIS-620 & Parse HTML Table
    html_text = resp.content.decode("tis-620", errors="ignore")
    tables = pd.read_html(StringIO(html_text))
    df_raw = tables[0]

    # 3. ค้นหาแถวที่เป็น Header ตัวจริง
    header_idx = None
    for idx, row in df_raw.iterrows():
        row_vals = [str(v).strip() for v in row.values]
        if "กลุ่มอุตสาหกรรม" in row_vals or "หลักทรัพย์" in row_vals:
            header_idx = idx
            break

    if header_idx is not None:
        df_clean = df_raw.iloc[header_idx + 1 :].copy().reset_index(drop=True)
        df_clean.columns = [str(v).strip() for v in df_raw.iloc[header_idx].values]
    else:
        df_clean = df_raw.copy()

    df_clean.columns = df_clean.columns.astype(str).str.strip()

    # 4. Save เป็นไฟล์ CSV แท้ๆ (UTF-8-SIG)
    df_clean.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"💾 บันทึกไฟล์ CSV เรียบร้อยแล้วที่: {csv_path}")

    # 5. อ่าน CSV กลับมาเป็น df_master
    df_master = pd.read_csv(csv_path)
    df_master.columns = df_master.columns.astype(str).str.strip()

    print(f"📋 รายชื่อคอลัมน์ที่พร้อมใช้งาน: {df_master.columns.tolist()[:5]}")
    print(f"📊 จำนวนหลักทรัพย์ทั้งหมด: {len(df_master)} รายการ")
    print("✅ เตรียมไฟล์ตั้งต้นเสร็จสิ้น")
    return df_master


def parse_page_content(html_content, page_url):
    """แกะข้อมูลจาก HTML string โดยใช้ lxml"""
    tree = lxml_html.fromstring(html_content)
    extracted_data = []

    for label, xpath in XPATH_MAPPINGS.items():
        try:
            elements = tree.xpath(xpath)

            for el in elements:
                # News Wrapper: เก็บข้อความเดิม และเพิ่ม URL ข่าว
                if label == "News Wrapper":
                    original_text = el.text_content().strip()
                    news_links = []
                    seen = set()

                    for anchor in el.xpath(".//a[@href]"):
                        href = (anchor.get("href") or "").strip()
                        if not href:
                            continue

                        link = urljoin(page_url, href)
                        candidates = [link]

                        # URL ข่าวอาจฝังอยู่ในลิงก์แชร์
                        query = parse_qs(urlparse(link).query)
                        for key in ("u", "url"):
                            candidates.extend(query.get(key, []))

                        for candidate in candidates:
                            candidate = urljoin(page_url, candidate)
                            parsed = urlparse(candidate)

                            if (
                                parsed.netloc == "www.set.or.th"
                                and "/market/news-and-alert/newsdetails" in parsed.path
                                and candidate not in seen
                            ):
                                seen.add(candidate)
                                news_links.append(candidate)

                    if original_text:
                        extracted_data.append(
                            [
                                label,
                                page_url,
                                original_text,          # Column1: ข้อความเดิมทั้งหมด
                                "\n".join(news_links),  # Column2: URL ข่าว แยกด้วยบรรทัดใหม่
                            ]
                        )
                    continue

                # Annual Report: ดึง URL จาก href
                if label == "Annual Report":
                    link = el.get("href")
                    if link:
                        extracted_data.append([label, page_url, link])
                    continue

                # ส่วนอื่น: ตรวจสอบว่ามีตารางหรือไม่
                el_html = lxml_html.tostring(el, encoding="unicode")

                if "<table" in el_html.lower():
                    try:
                        tables = pd.read_html(StringIO(el_html))

                        for df in tables:
                            if isinstance(df.columns, pd.MultiIndex):
                                df.columns = [
                                    " | ".join(str(c) for c in col if c)
                                    for col in df.columns
                                ]
                            else:
                                df.columns = df.columns.astype(str)

                            header_row = [label, page_url] + df.columns.tolist()
                            extracted_data.append(header_row)

                            for row in df.itertuples(index=False, name=None):
                                extracted_data.append([label, page_url] + list(row))
                    except Exception:
                        pass
                else:
                    text = el.text_content().strip()
                    if text:
                        extracted_data.append([label, page_url, text])

        except Exception:
            pass

    return extracted_data


# ==========================================
# PARALLEL SCRAPING WORKER
# ==========================================


def scrape_single_url(driver, url):
    """ดึงและแกะข้อมูล 1 URL ด้วย Smart Wait"""
    driver.get(url)

    # Smart Wait 1: รอให้โครงสร้างหลักโผล่มาก่อน (Company Info)
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_element_located((By.CLASS_NAME, "company-info"))
    )

    # Smart Wait 2: รอให้ Body/DOM พร้อมใช้งาน (แก้ Error scrollHeight)
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        lambda d: d.execute_script(
            "return document.body != null && document.body.scrollHeight > 0;"
        )
    )

    # สั่ง Scroll ลงมากลาง ๆ เพื่อกระตุ้น Lazy Load
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 3);")

    # Smart Wait 3: รอให้ตารางงบการเงินโผล่มา (ถ้าไม่มีงบจะผ่านไปเองหลัง 5 วิ)
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".factsheet-financial, .table-custom-field-main")
            )
        )
    except TimeoutException:
        pass

    html_source = driver.page_source
    return parse_page_content(html_source, url)


def scrape_urls_parallel(urls, max_workers, action_label="Scraped", show_rows=False):
    """รัน Worker Threads พร้อมรีไซเคิล Chrome อัตโนมัติและ Retry เมื่อ Error"""
    if not urls:
        return []

    url_queue = queue.Queue()
    for u in urls:
        url_queue.put(u)

    result_list = []
    result_lock = threading.Lock()
    retry_count = {}
    retry_lock = threading.Lock()

    def _worker(worker_idx):
        # หน่วงเวลาเริ่มเล็กน้อยไม่ให้ทุก Thread แย่งเปิด Chrome พร้อมกัน
        if worker_idx > 0:
            time.sleep(worker_idx * 0.5)

        consecutive_init_errors = 0
        while True:
            try:
                url = url_queue.get(timeout=0.5)
            except queue.Empty:
                break

            driver = None
            urls_processed = 0
            try:
                driver = make_driver()
                consecutive_init_errors = 0
                while url is not None:
                    need_restart = False
                    symbol = url.split("/")[-2]
                    try:
                        data = scrape_single_url(driver, url)
                        if data:
                            with result_lock:
                                result_list.extend(data)
                            if show_rows:
                                print(f"  ✓ {action_label}: {symbol} | rows={len(data)}")
                            else:
                                print(f"  ✓ {action_label}: {symbol}")
                        else:
                            print(f"  ⚠️ {action_label} (No Data): {symbol}")
                        urls_processed += 1

                    except TimeoutException:
                        with retry_lock:
                            count = retry_count.get(url, 0)
                            if count < MAX_RETRY_PER_URL:
                                retry_count[url] = count + 1
                                print(
                                    f"  ❌ Timeout {symbol}: Retrying ({retry_count[url]}/{MAX_RETRY_PER_URL})"
                                )
                                url_queue.put(url)
                            else:
                                print(
                                    f"  ❌ Error {url}: Timeout (Page did not load key elements)"
                                )
                        time.sleep(1)

                    except WebDriverException as e:
                        err_line = (e.msg or str(e)).split("\n")[0]
                        with retry_lock:
                            count = retry_count.get(url, 0)
                            if count < MAX_RETRY_PER_URL:
                                retry_count[url] = count + 1
                                print(
                                    f"  ❌ WebDriver {symbol}: Retrying ({retry_count[url]}/{MAX_RETRY_PER_URL}) - {err_line}"
                                )
                                url_queue.put(url)
                            else:
                                print(f"  ❌ Error {url}: WebDriver Error ({err_line})")
                        need_restart = True

                    except Exception as e:
                        with retry_lock:
                            count = retry_count.get(url, 0)
                            if count < MAX_RETRY_PER_URL:
                                retry_count[url] = count + 1
                                print(
                                    f"  ❌ Error {symbol} ({type(e).__name__}): Retrying ({retry_count[url]}/{MAX_RETRY_PER_URL})"
                                )
                                url_queue.put(url)
                            else:
                                print(
                                    f"  ❌ Error {url}: Unexpected Error ({type(e).__name__}) - {e}"
                                )
                        time.sleep(1)

                    finally:
                        url_queue.task_done()

                    if need_restart or urls_processed >= MAX_URLS_PER_DRIVER:
                        break

                    try:
                        url = url_queue.get_nowait()
                    except queue.Empty:
                        url = None

            except Exception as e:
                consecutive_init_errors += 1
                print(
                    f"🔥 Worker Initialization Error ({consecutive_init_errors}/3): {e}"
                )
                with retry_lock:
                    count = retry_count.get(url, 0)
                    if count < MAX_RETRY_PER_URL:
                        retry_count[url] = count + 1
                        url_queue.put(url)
                url_queue.task_done()
                if consecutive_init_errors >= 3:
                    print("🛑 หยุด Worker นี้เนื่องจากเปิด Browser ไม่สำเร็จติดต่อกัน 3 ครั้ง")
                    break
                time.sleep(3)
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

    num_workers = min(max_workers, len(urls))
    threads = []
    for idx in range(num_workers):
        t = threading.Thread(target=_worker, args=(idx,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return result_list


def rows_to_dataframe(rows):
    """แปลง list ของแถวความยาวไม่เท่ากันเป็น DataFrame พร้อมเติม None"""
    max_cols = max(len(r) for r in rows)
    cols = ["TableName", "URL"] + [f"Column{i + 1}" for i in range(max_cols - 2)]
    padded_data = [r + [None] * (max_cols - len(r)) for r in rows]
    return pd.DataFrame(padded_data, columns=cols)

SECTOR_NAME_MAP = {
    "AGRO": "agro_company.csv",
    "CONSUMP": "consump_company.csv",
    "FINCIAL": "fincial_company.csv",
    "INDUS": "indus_company.csv",
    "PROPCON": "propcon_company.csv",
    "RESOURC": "resourc_company.csv",
    "SERVICE": "service_company.csv",
    "TECH": "tech_company.csv",
    "-": "na_company.csv",
}


def save_sector_rows(sector_name, sector_data):
    """บันทึกข้อมูลของ 1 Sector ลงไฟล์ CSV"""
    if sector_data:
        df_out = rows_to_dataframe(sector_data)
        filename = SECTOR_NAME_MAP.get(sector_name, f"{sector_name.lower()}_company.csv")
        out_path = os.path.join(LOCAL_PATH_DATA, filename)
        df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"💾 บันทึก {filename} เรียบร้อย ({len(df_out)} rows)")
    else:
        print(f"⚠️ ไม่มีข้อมูลสำหรับ {sector_name}")


def process_sector(sector_name, stock_list):
    """จัดการการดึงข้อมูลของ 1 Sector"""
    print(f"\n🚀 เริ่มประมวลผลกลุ่ม: {sector_name} ({len(stock_list)} บริษัท)")

    urls = [
        f"https://www.set.or.th/th/market/product/stock/quote/{stock}/factsheet"
        for stock in stock_list
    ]

    sector_data = scrape_urls_parallel(urls, MAX_WORKERS, action_label="Scraped")
    save_sector_rows(sector_name, sector_data)


def process_all_sectors(sector_stocks):
    """ดึงหลาย Sector ด้วย Browser Pool ชุดเดียว แล้วแยกเซฟเป็น CSV ตามกลุ่ม (ไม่ต้องปิด-เปิด Chrome ใหม่ทุกกลุ่ม)"""
    all_urls = []
    url_to_sector = {}
    sector_rows = {}

    for sector_en, stock_list in sector_stocks:
        sector_rows[sector_en] = []
        for stock in stock_list:
            url = f"https://www.set.or.th/th/market/product/stock/quote/{stock}/factsheet"
            all_urls.append(url)
            url_to_sector[url] = sector_en

    sector_summary = ", ".join(f"{sec}({len(stocks)})" for sec, stocks in sector_stocks)
    print(f"\n🚀 เริ่มประมวลผลรวม {len(all_urls)} บริษัท [{sector_summary}] ด้วย Browser Pool ชุดเดียว")

    all_rows = scrape_urls_parallel(all_urls, MAX_WORKERS, action_label="Scraped")

    for row in all_rows:
        sec = url_to_sector.get(row[1])
        if sec is not None:
            sector_rows[sec].append(row)

    for sector_en, _ in sector_stocks:
        save_sector_rows(sector_en, sector_rows[sector_en])


# ==========================================
# STEP 2: DATA VERIFICATION & RETRY (A -> E)
# ==========================================


def _missing_condition(df):
    """สร้าง Boolean Mask สำหรับแถวที่เข้าเงื่อนไข 'ไม่มีข้อมูล' โดยไม่แก้ค่าเดิมใน df"""
    table_name = (
        df["TableName"].astype(str).str.strip()
        if "TableName" in df.columns
        else pd.Series("", index=df.index)
    )
    col1 = (
        df["Column1"].astype(str).str.strip()
        if "Column1" in df.columns
        else pd.Series("", index=df.index)
    )
    col3 = (
        df["Column3"].astype(str).str.strip()
        if "Column3" in df.columns
        else pd.Series("", index=df.index)
    )

    mask_col1 = table_name.isin(GROUP_COL1) & (col1 == "ไม่มีข้อมูล")
    mask_col3 = table_name.isin(GROUP_COL3) & (col3 == "ไม่มีข้อมูล")
    return mask_col1 | mask_col3


def script_a_check(save_summary=True, bases=None):
    """Script A/D: ตรวจสอบข้อมูลที่ขาดหาย และสร้างไฟล์ _missing_data_summary.csv"""
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.expand_frame_repr", False)

    folder = Path(LOCAL_PATH_DATA)
    target_bases = bases if bases is not None else SECTOR_BASES
    all_results = []

    for base in target_bases:
        file_path = folder / f"{base}.csv"
        print(f"\n===== {base} =====")
        try:
            df = pd.read_csv(file_path, header=0, encoding="utf-8-sig", low_memory=False)
            df.columns = df.columns.astype(str).str.strip()

            cond = _missing_condition(df)
            if "TableName" not in df.columns or "URL" not in df.columns:
                print("(ไม่มีคอลัมน์ TableName หรือ URL)")
                continue

            df_out = (
                df.loc[cond, ["TableName", "URL"]]
                .apply(lambda s: s.astype(str).str.strip())
                .drop_duplicates()
            )

            if df_out.empty:
                print("(ไม่พบรายการที่เข้าเงื่อนไข)")
            else:
                print(df_out.to_string(index=False))
                tmp = df_out.copy()
                tmp.insert(0, "SourceFile", f"{base}.csv")
                all_results.append(tmp)

        except FileNotFoundError:
            print(f"ไม่พบไฟล์: {file_path}")
        except Exception as e:
            print(f"เกิดข้อผิดพลาดกับไฟล์ {file_path}: {e}")

    if all_results and save_summary:
        all_df = pd.concat(all_results, ignore_index=True).drop_duplicates()
        out_path = folder / "_missing_data_summary.csv"
        all_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 บันทึกสรุปที่: {out_path}")


def script_b_delete(apply_delete=True, bases=None):
    """Script B: Cascade Delete ทุกแถวของ URL ที่ข้อมูลไม่ครบ"""
    folder = Path(LOCAL_PATH_DATA)
    target_bases = bases if bases is not None else SECTOR_BASES
    summary = []

    for base in target_bases:
        file_path = folder / f"{base}.csv"
        print(f"\n===== {base} =====")
        try:
            df = pd.read_csv(file_path, header=0, encoding="utf-8-sig", low_memory=False)
            df.columns = df.columns.astype(str).str.strip()

            if "URL" not in df.columns:
                print("(ไม่มีคอลัมน์ URL)")
                summary.append((base, 0, 0))
                continue

            cond = _missing_condition(df)
            urls_to_delete = (
                df.loc[cond, "URL"]
                .dropna()
                .astype(str)
                .str.strip()
                .drop_duplicates()
                .tolist()
            )

            print(f"> พบ URL ที่ต้องลบแบบ cascade: {len(urls_to_delete)} รายการ")
            if urls_to_delete:
                cascade_mask = df["URL"].astype(str).str.strip().isin(urls_to_delete)
                n_to_delete = int(cascade_mask.sum())
                print(f"> จำนวนแถวที่มี URL ตรงกันและจะถูกลบ: {n_to_delete}")

                if apply_delete and n_to_delete > 0:
                    df_after = df.loc[~cascade_mask].copy()
                    df_after.to_csv(file_path, index=False, encoding="utf-8-sig")
                    print(f"> ลบแล้ว: จาก {len(df)} เหลือ {len(df_after)}")
                    summary.append((base, n_to_delete, len(urls_to_delete)))
                else:
                    print("> DRY RUN หรือไม่มีอะไรให้ลบ")
                    summary.append((base, 0, len(urls_to_delete)))
            else:
                print("(ไม่พบ URL ที่จะลบ)")
                summary.append((base, 0, 0))

        except FileNotFoundError:
            print(f"ไม่พบไฟล์: {file_path}")
            summary.append((base, -1, 0))
        except Exception as e:
            print(f"เกิดข้อผิดพลาดกับไฟล์ {file_path}: {e}")
            summary.append((base, -2, 0))

    print("\n===== SUMMARY =====")
    for base, n_rows, n_urls in summary:
        if n_rows == -1:
            print(f"{base}: ไม่พบไฟล์")
        elif n_rows == -2:
            print(f"{base}: เกิดข้อผิดพลาด")
        else:
            print(f"{base}: ลบ {n_rows} แถว (จาก URL เป้าหมาย {n_urls} รายการ)")


def script_c_refresh():
    """Script C: ดึงข้อมูล URL ที่ขาดหายซ้ำแล้วต่อท้ายกลับเข้าไฟล์ต้นทาง"""
    folder = Path(LOCAL_PATH_DATA)
    url_list_primary = folder / "_missing_data_summary.csv"

    if not url_list_primary.exists():
        print(f"[SKIP] ไม่พบไฟล์รายการ URL สำหรับรีเฟรช: '{url_list_primary.name}'")
        return

    print(f"[INFO] ใช้ไฟล์รายการ URL: {url_list_primary.name}")
    url_list_df = pd.read_csv(url_list_primary, encoding="utf-8-sig")
    url_list_df.columns = url_list_df.columns.astype(str).str.strip()

    if "SourceFile" not in url_list_df.columns or "URL" not in url_list_df.columns:
        raise ValueError("ไฟล์รายการ URL ต้องมีคอลัมน์ 'SourceFile' และ 'URL'")

    url_list_df["SourceFile"] = url_list_df["SourceFile"].astype(str).str.strip()
    url_list_df["URL"] = url_list_df["URL"].astype(str).str.strip()

    for source_file, g in url_list_df.groupby("SourceFile"):
        target_path = folder / source_file
        urls_to_retry = (
            g["URL"].dropna().astype(str).str.strip().drop_duplicates().tolist()
        )

        print(f"\n===== REFRESH -> {source_file} (Multi-threaded) =====")
        print(f"> จำนวน URL: {len(urls_to_retry)}")

        if not target_path.exists():
            print(f"[SKIP] ข้าม {source_file} เพราะยังไม่มีไฟล์ปลายทาง")
            continue

        retry_data = scrape_urls_parallel(
            urls_to_retry,
            MAX_WORKERS,
            action_label="Refreshed",
            show_rows=True,
        )

        if not retry_data:
            print("> ไม่มีข้อมูลใหม่จาก URL เหล่านี้ — ข้ามการบันทึก")
            continue

        new_df = rows_to_dataframe(retry_data)

        old_df = pd.read_csv(target_path, header=0, encoding="utf-8-sig", low_memory=False)
        old_df.columns = old_df.columns.astype(str).str.strip()

        # ปรับคอลัมน์ให้ยูเนียนกัน
        all_cols = list(dict.fromkeys(list(old_df.columns) + list(new_df.columns)))
        for c in all_cols:
            if c not in old_df.columns:
                old_df[c] = pd.NA
            if c not in new_df.columns:
                new_df[c] = pd.NA

        old_df = old_df[all_cols]
        new_df = new_df[all_cols]

        before_rows = len(old_df)
        merged = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates()
        after_rows = len(merged)

        merged.to_csv(target_path, index=False, encoding="utf-8-sig")
        print(
            f"> บันทึกกลับ {source_file}: เดิม {before_rows} แถว → ใหม่ {after_rows} แถว (+{after_rows - before_rows})"
        )

    print("\n✅ เสร็จสิ้นการรีเฟรชข้อมูลด้วย Multi-threading")


def script_e_cleanup():
    """Script E: ลบไฟล์ _missing_data_summary.csv"""
    target_file = os.path.join(LOCAL_PATH_DATA, "_missing_data_summary.csv")
    if os.path.exists(target_file):
        os.remove(target_file)
        print(f"ลบไฟล์ {target_file} เรียบร้อยแล้ว")
    else:
        print(f"ไม่พบไฟล์ {target_file} ที่ต้องการลบ")


# ==========================================
# MAIN EXECUTION
# ==========================================


def parse_args():
    parser = argparse.ArgumentParser(description="SET Factsheet Scraper")
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"จำนวน Chrome workers พร้อมกัน (default: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--sectors",
        type=str,
        default=os.getenv("SECTORS", ""),
        help="เลือกเฉพาะบาง Sector คั่นด้วยจุลภาค เช่น AGRO,FINCIAL (เว้นว่าง = ทุกกลุ่ม)",
    )
    parser.add_argument(
        "--test-stocks",
        type=str,
        default=os.getenv("TEST_STOCKS", ""),
        help="รายชื่อหุ้นทดสอบคั่นด้วยจุลภาค เช่น ADVANC,PTT (ถ้าไม่ระบุจะดึงทั้งหมด)",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="ข้ามขั้นตอน Verification & Retry (Script A-E)",
    )
    return parser.parse_args()


def main():
    global MAX_WORKERS
    args = parse_args()
    MAX_WORKERS = max(1, args.workers)

    print(
        f"⚙️ Config: IS_CI={IS_CI}, MAX_WORKERS={MAX_WORKERS}, "
        f"HEADLESS={HEADLESS_MODE}, WAIT_TIMEOUT={WAIT_TIMEOUT}s, "
        f"MAX_URLS_PER_DRIVER={MAX_URLS_PER_DRIVER}, MAX_RETRY_PER_URL={MAX_RETRY_PER_URL}"
    )

    start_time = time.time()
    kill_orphan_chrome()

    # ทดสอบเปิด Chrome ก่อนเริ่ม ถ้าเปิดไม่ได้ให้หยุดทันทีไม่วนลูปยาว
    verify_driver()

    # ---- STEP 1: SCRAPE ----
    df_master = setup_environment()

    test_stocks = [s.strip().upper() for s in args.test_stocks.split(",") if s.strip()]
    if test_stocks:
        print(f"\n🧪 TEST MODE: ดึงเฉพาะหุ้น {test_stocks}")
        process_sector("SINGLE", test_stocks)
        elapsed = time.time() - start_time
        print(f"\n✨ เสร็จสิ้นการทดสอบในเวลา: {elapsed / 60:.2f} นาที ✨")
        return

    all_sectors = [
        ("เกษตรและอุตสาหกรรมอาหาร", "AGRO"),
        ("สินค้าอุปโภคบริโภค", "CONSUMP"),
        ("ธุรกิจการเงิน", "FINCIAL"),
        ("สินค้าอุตสาหกรรม", "INDUS"),
        ("อสังหาริมทรัพย์และก่อสร้าง", "PROPCON"),
        ("ทรัพยากร", "RESOURC"),
        ("บริการ", "SERVICE"),
        ("เทคโนโลยี", "TECH"),
        ("-", "-"),
    ]

    selected_sectors = {
        s.strip().upper() for s in args.sectors.split(",") if s.strip()
    }
    sectors = [
        (th, en)
        for th, en in all_sectors
        if not selected_sectors or en in selected_sectors
    ]

    sector_stocks = []
    target_bases = []
    for sector_th, sector_en in sectors:
        stock_list = df_master.loc[
            df_master["กลุ่มอุตสาหกรรม"] == sector_th, "หลักทรัพย์"
        ].tolist()

        if not stock_list:
            print(f"ข้าม {sector_en} (ไม่พบหุ้น)")
            continue

        sector_stocks.append((sector_en, stock_list))
        target_bases.append(
            SECTOR_NAME_MAP.get(sector_en, f"{sector_en.lower()}_company.csv").replace(".csv", "")
        )

    if sector_stocks:
        process_all_sectors(sector_stocks)

    elapsed = time.time() - start_time
    print(f"\n✨ เสร็จสิ้นการดึงข้อมูลรอบแรกในเวลา: {elapsed / 60:.2f} นาที ✨")

    if args.skip_verify or not target_bases:
        return

    # ---- STEP 2: VERIFY & RETRY ----
    print("\n" + "=" * 50)
    print("STEP 2: ตรวจสอบและ Retry ข้อมูลที่ขาดหาย")
    print("=" * 50)

    print("\n[A] Check missing data...")
    script_a_check(save_summary=True, bases=target_bases)

    print("\n[B] Cascade delete incomplete data...")
    script_b_delete(apply_delete=True, bases=target_bases)

    print("\n[C] Refresh missing data...")
    script_c_refresh()

    print("\n[D] Verify after refresh...")
    script_a_check(save_summary=False, bases=target_bases)

    print("\n[E] Cleanup summary file...")
    script_e_cleanup()

    total = time.time() - start_time
    print(f"\n🎉 ทุกขั้นตอนเสร็จสิ้น รวมเวลา: {total / 60:.2f} นาที")


if __name__ == "__main__":
    main()
