import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin
import re
import time
from selenium import webdriver
import backoff
from aiohttp import ClientError
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException
import json
import os
import chromedriver_autoinstaller


class FastFacultyCrawlerV2:
    def __init__(self, base_urls, max_concurrent_requests=1):
        self.base_urls = base_urls if isinstance(base_urls, list) else [base_urls]
        self.max_concurrent_requests = max_concurrent_requests
        self.profiles_data = []
        self.processed_count = 0

        # Render hosting — write only to /tmp folder
        self.cache_file = os.path.join("/tmp", "faculty_data_cache.json")
        self.cache_expiration_seconds = 1 * 60 * 60  # 1 hour

    # ✅ FINAL WORKING SETUP DRIVER
    def setup_driver(self):
        chromedriver_autoinstaller.install()

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920x1080")

        # Render Chrome binary path
        options.binary_location = "/usr/bin/google-chrome"

        prefs = {
            'profile.managed_default_content_settings.images': 2,
            'profile.default_content_setting_values.notifications': 2
        }
        options.add_experimental_option('prefs', prefs)

        driver = webdriver.Chrome(service=Service(), options=options)
        driver.set_script_timeout(30)
        driver.implicitly_wait(10)
        return driver

    def _clean_href(self, href):
        return href.replace('&', '_').replace('(', '_').replace(')', '_')

    def get_institution_name(self, url):
        domain = url.split('//')[1].split('.')[0].upper()
        if domain == 'IISCPROFILES':
            return 'IISC'
        return domain

    async def get_all_profile_links(self, base_url):
        print(f"[INFO] Finding department and profile links for {base_url}...")
        profile_links = set()
        max_retries = 3

        for attempt in range(max_retries):
            driver = self.setup_driver()
            try:
                print(f"[INFO] Loading: {base_url} (Attempt {attempt+1})")

                driver.execute_script("""
                    window.alert = function() { return true; };
                    window.confirm = function() { return true; };
                    if (typeof Highcharts === 'undefined') {
                        window.Highcharts = { chart: function(){}, Chart: function(){} };
                    }
                """)

                driver.set_page_load_timeout(30)
                driver.get(base_url)

                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/faculty/index/']"))
                    )
                except Exception:
                    pass

                soup = BeautifulSoup(driver.page_source, 'lxml')
                initial_links = {
                    urljoin(base_url, self._clean_href(a['href']))
                    for a in soup.select("a[href*='/faculty/index/']")
                }

                if initial_links:
                    break

            except Exception as e:
                print("[ERROR]", e)
            finally:
                driver.quit()

        conn = aiohttp.TCPConnector(limit=self.max_concurrent_requests)

        async with aiohttp.ClientSession(connector=conn) as session:
            urls_to_process = set(initial_links)
            processed_urls = set()

            while urls_to_process:
                batch = list(urls_to_process)
                urls_to_process.clear()

                tasks = [self.fetch_html(session, u) for u in batch]
                html_contents = await asyncio.gather(*tasks)

                processed_urls.update(batch)

                for i, html in enumerate(html_contents):
                    if html:
                        current_url = batch[i]
                        soup = BeautifulSoup(html, 'lxml')

                        links = {
                            urljoin(current_url, a['href'])
                            for a in soup.select("a[href*='/profile/']")
                        }
                        profile_links.update(links)

                        for p in soup.select("ul.pagination li a"):
                            new_url = urljoin(current_url, self._clean_href(p['href']))
                            if new_url not in processed_urls:
                                urls_to_process.add(new_url)

        print(f"[INFO] Total profile links: {len(profile_links)}")
        return list(profile_links)

    def parse_profile(self, html_content, url, institution_name):
        soup = BeautifulSoup(html_content, 'lxml')
        name_el = soup.select_one('h1 strong, h1, div.col-md-9 h3')
        name = name_el.get_text(strip=True) if name_el else "N/A"

        name = re.sub(r'^(Dr|Prof|Mr|Mrs|Ms|Professor)\.?\s*', '', name)
        name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        department = "N/A"
        dept_el = soup.find(['div', 'p', 'span', 'li'], string=re.compile(r"Department of|School of", re.I))
        if dept_el:
            department = dept_el.get_text(strip=True)
        else:
            for sel in [
                'ul.name-location li:nth-of-type(2)',
                'div[style*="color:#666"]'
            ]:
                el = soup.select_one(sel)
                if el:
                    department = el.get_text(strip=True)
                    break

        # Vidwan ID
        vidwan_id = 'N/A'
        vidwan_match = re.search(r'vidwan.irins.org/profile/(\d+)', html_content)
        if vidwan_match:
            vidwan_id = vidwan_match.group(1)

        else:
            vlink = soup.find('a', href=re.compile(r'vidwan.*profile', re.I))
            if vlink:
                m = re.search(r'(\d+)', vlink['href'])
                if m:
                    vidwan_id = m.group(1)

        # Expertise
        expertise = 'N/A'
        head = soup.find(['h2', 'h3', 'h4', 'strong'], text=re.compile(r"Expertise|Research Interests", re.I))
        if head:
            next_el = head.find_next_sibling()
            if next_el:
                expertise = next_el.get_text(strip=True, separator=', ')

        # Image extraction
        image_url = "N/A"
        img_selectors = [
            '.profile-image img', '.faculty-image img', '.avatar img', '.user-image img',
            '.photo img', 'img.profile-photo', 'img.faculty-photo', '.profile-pic img',
            '#profile_image img', '.researcher-photo img'
        ]

        for s in img_selectors:
            im = soup.select_one(s)
            if im and im.get('src'):
                image_url = urljoin(url, im['src'])
                break

        if image_url == "N/A":
            for im in soup.find_all("img"):
                src = im.get("src", "").lower()
                alt = im.get("alt", "").lower()
                if any(k in src or k in alt for k in ["profile", "faculty", "photo", "avatar", "user"]):
                    image_url = urljoin(url, im.get("src"))
                    break

        if image_url == "N/A":
            prof_divs = soup.find_all(['div', 'section'], class_=lambda x: x and any(w in str(x).lower() for w in ['profile', 'faculty', 'photo', 'member']))
            for d in prof_divs:
                im = d.find("img")
                if im and im.get("src"):
                    s = im.get("src")
                    if not s.startswith("data:image") and not s.endswith(".ico"):
                        image_url = urljoin(url, s)
                        break

        if image_url != "N/A":
            if "placeholder" in image_url.lower() or image_url.startswith("data:image"):
                image_url = "N/A"

        profile = {
            'Institution': institution_name,
            'Name': name,
            'Department': department,
            'Vidwan-ID': vidwan_id,
            'Profile URL': url,
            'Image URL': image_url,
            'Expertise': expertise,
            'html_content': html_content
        }

        print(f"[SUCCESS] Processed: {name}")
        return profile

    @backoff.on_exception(backoff.expo,
                          (asyncio.TimeoutError, ClientError, aiohttp.ClientError),
                          max_tries=5, max_time=300)
    async def fetch_html(self, session, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.5'
        }

        try:
            timeout = aiohttp.ClientTimeout(total=120)
            async with session.get(url, timeout=timeout, headers=headers, ssl=False) as res:
                res.raise_for_status()
                content = await res.read()
                return content.decode("utf-8", errors="replace")
        except Exception as e:
            print(f"[ERROR] Failed URL {url}: {e}")
            await asyncio.sleep(1)
            return None

    async def fetch_and_process_profiles(self, urls, institution_name):
        conn = aiohttp.TCPConnector(limit=self.max_concurrent_requests)

        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self.fetch_html(session, u) for u in urls]
            pages = await asyncio.gather(*tasks)

            for i, html in enumerate(pages):
                if html:
                    try:
                        p = self.parse_profile(html, urls[i], institution_name)
                        if p:
                            self.profiles_data.append(p)
                    except Exception as e:
                        print(f"[ERROR Parsing] {urls[i]}: {e}")

    def save_to_excel(self, profiles):
        if not profiles:
            print("[INFO] No profiles to save.")
            return

        try:
            df = pd.DataFrame([{k: v for k, v in p.items() if k != 'html_content'} for p in profiles])
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            file = f"faculty_data_export_{ts}.xlsx"

            with pd.ExcelWriter(file, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Faculty Profiles")
                sheet = writer.sheets["Faculty Profiles"]

                for i, col in enumerate(df.columns):
                    width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                    sheet.column_dimensions[chr(65+i)].width = width

            print(f"[EXCEL SAVED] {file}")
        except Exception as e:
            print("[ERROR Saving Excel]", e)

    def _is_cache_valid(self):
        if not os.path.exists(self.cache_file):
            return False
        age = time.time() - os.path.getmtime(self.cache_file)
        return age < self.cache_expiration_seconds

    def _load_from_cache(self):
        print("[CACHE] Loading...")
        if not os.path.exists(self.cache_file):
            return []
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except:
            print("[CACHE] Corrupted, clearing...")
            return []

    def _save_to_cache(self, profiles):
        print("[CACHE] Saving...")
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(profiles, f, indent=4)

    async def crawl(self, keyword=None, save_excel=False):
        start = time.time()
        all_profiles = []

        # Load from cache
        if self._is_cache_valid():
            all_profiles = self._load_from_cache()

        else:
            all_profiles = self._load_from_cache()
            existing = {p['Profile URL']: p for p in all_profiles}

            print("[CRAWLER] Starting fresh crawl")

            for base_url in self.base_urls:
                inst = self.get_institution_name(base_url)
                print(f"[CRAWLER] Institution: {inst}")

                self.profiles_data = []
                urls = await self.get_all_profile_links(base_url)

                if urls:
                    await self.fetch_and_process_profiles(urls, inst)
                    for p in self.profiles_data:
                        existing[p['Profile URL']] = p

            all_profiles = list(existing.values())
            self._save_to_cache(all_profiles)

        # FILTERING LOGIC (unchanged)
        if keyword and keyword.lower().startswith("name:"):
            t = keyword.split(":", 1)[1].lower()
            filtered = [p for p in all_profiles if p.get("Name", "").lower().startswith(t)]

        elif keyword and keyword.lower().startswith("vidwan:"):
            t = keyword.split(":", 1)[1].lower()
            filtered = [p for p in all_profiles if p.get("Vidwan-ID", "").lower() == t]

        elif keyword:
            keys = [k.strip().lower() for k in keyword.split(",") if k.strip()]
            scored = []

            for p in all_profiles:
                expertise = p.get("Expertise", "").lower()
                html = p.get("html_content", "").lower()
                score = 0

                for k in keys:
                    if k in expertise:
                        score += 2
                    elif k in html:
                        score += 1

                if score > 0:
                    p["match_score"] = score
                    scored.append(p)

            scored.sort(key=lambda x: x["match_score"], reverse=True)
            filtered = scored

        else:
            filtered = all_profiles

        if save_excel:
            self.save_to_excel(filtered)

        print(f"[DONE] Total time: {time.time() - start:.2f} sec")

        return [{
            k: v for k, v in p.items() if k != "html_content"
        } for p in filtered]


def main():
    urls = [
        "https://iitm.irins.org", "https://iith.irins.org", "https://iiti.irins.org",
        "https://iitp.irins.org", "https://iiscprofiles.irins.org", "https://iitk.irins.org",
        "https://iitd.irins.org", "https://iitr.irins.org", "https://iiserb.irins.org",
        "https://iittp.irins.org", "https://iisermohali.irins.org", "https://iitjammu.irins.org"
    ]

    crawler = FastFacultyCrawlerV2(urls, max_concurrent_requests=5)
    try:
        asyncio.run(crawler.crawl(save_excel=True))
    except Exception as e:
        print("[ERROR]", e)
        if crawler.profiles_data:
            crawler.save_to_excel(crawler.profiles_data)


if __name__ == "__main__":
    main()
