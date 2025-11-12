import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin, quote
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

# ✅ Added for hosting (auto ChromeDriver install in Render / Railway)
from webdriver_manager.chrome import ChromeDriverManager


class FastFacultyCrawlerV2:
    def __init__(self, base_urls, max_concurrent_requests=1):
        self.base_urls = base_urls if isinstance(base_urls, list) else [base_urls]
        self.max_concurrent_requests = max_concurrent_requests
        self.profiles_data = []
        self.processed_count = 0

        # ✅ Use /tmp path for cache (Render / Railway only allows writing there)
        self.cache_file = os.path.join("/tmp", "faculty_data_cache.json")

        self.cache_expiration_seconds = 1 * 60 * 60  # 1 hour - shorter cache for testing

    def setup_driver(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-extensions")
        
        # Set page load strategy to eager for faster loading
        options.page_load_strategy = 'eager'
        
        # Create prefs dictionary to disable images and handle alerts
        prefs = {
            'profile.managed_default_content_settings.images': 2,  # Disable images
            'profile.default_content_settings.popups': 0,
            'profile.default_content_setting_values.notifications': 2,
            'profile.default_content_setting_values.automatic_downloads': 1
        }
        options.add_experimental_option('prefs', prefs)
        
        # ✅ Hosting-safe Chrome driver setup
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        
        # Set various timeouts
        driver.set_script_timeout(30)
        driver.implicitly_wait(10)
        
        return driver

    def _clean_href(self, href):
        return href.replace('&', '_').replace('(', '_').replace(')', '_')
        
    def get_institution_name(self, url):
        # Extract institution name from URL
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
                print(f"[INFO] Loading main page: {base_url} (Attempt {attempt + 1}/{max_retries})")
                
                # Inject scripts to handle alerts and undefined functions before loading page
                driver.execute_script("""
                    window.alert = function() { return true; };
                    window.confirm = function() { return true; };
                    if (typeof Highcharts === 'undefined') {
                        window.Highcharts = {
                            chart: function() { return {}; },
                            Chart: function() { return {}; }
                        };
                    }
                """)
                
                # Set page load timeout and navigate
                driver.set_page_load_timeout(30)
                driver.get(base_url)
                
                try:
                    # Wait for faculty links with a shorter timeout
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/faculty/index/']"))
                    )
                except Exception as e:
                    print(f"[WARNING] Timeout waiting for faculty links: {e}")
                    # Even if timeout occurs, try to process the page
                driver.execute_script("""
                    if (typeof Highcharts === 'undefined') {
                        window.Highcharts = {
                            chart: function() { return {}; },
                            Chart: function() { return {}; }
                        };
                    }
                """)
                
                # Get page source and parse links
                soup = BeautifulSoup(driver.page_source, 'lxml')
                initial_dept_links = {urljoin(base_url, self._clean_href(link['href'])) 
                                   for link in soup.select("a[href*='/faculty/index/']")}
                                   
                if initial_dept_links:
                    print(f"[INFO] Found {len(initial_dept_links)} unique initial department pages.")
                    break
                else:
                    print(f"[WARNING] No department links found on attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5)
                        
            except Exception as e:
                print(f"[ERROR] Failed attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
            finally:
                try:
                    driver.quit()
                except:
                    pass

        conn = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
        async with aiohttp.ClientSession(connector=conn) as session:
            urls_to_process = set(initial_dept_links)
            processed_urls = set()

            while urls_to_process:
                current_batch_urls = list(urls_to_process)
                urls_to_process.clear()
                
                print(f"[INFO] Fetching {len(current_batch_urls)} department pages...")
                tasks = [self.fetch_html(session, url) for url in current_batch_urls]
                html_contents = await asyncio.gather(*tasks)
                
                processed_urls.update(current_batch_urls)
                
                for i, html in enumerate(html_contents):
                    if html:
                        current_url = current_batch_urls[i]
                        soup = BeautifulSoup(html, 'lxml')
                        links = {urljoin(current_url, link['href']) for link in soup.select("a[href*='/profile/']")}
                        profile_links.update(links)
                        
                        for page_link in soup.select('ul.pagination li a'):
                            new_url = urljoin(current_url, self._clean_href(page_link['href']))
                            if new_url not in processed_urls and new_url not in urls_to_process:
                                urls_to_process.add(new_url)
            
            print(f"[INFO] Total department pages scanned: {len(processed_urls)}")

        print(f"\n[INFO] Total unique profile links found: {len(profile_links)}\n")
        return list(profile_links)

    def parse_profile(self, html_content, url, institution_name):
        soup = BeautifulSoup(html_content, 'lxml')
        name_element = soup.select_one('h1 strong, h1, div.col-md-9 h3')
        name = name_element.get_text(strip=True) if name_element else 'N/A'
        
        name = re.sub(r'^(Dr|Prof|Mr|Mrs|Ms|Professor)\.?\s*', '', name)
        name = re.sub(r'\s*\([^)]*\)', '', name).strip()

        department = 'N/A'
        dept_element = soup.find(['div', 'p', 'span', 'li'], string=re.compile(r'Department of|School of', re.I))
        if dept_element:
            department = dept_element.get_text(strip=True)
        else:
            dept_selectors = [
                'ul.name-location li:nth-of-type(2)',
                'div[style*="color:#666666"]'
            ]
            for selector in dept_selectors:
                elem = soup.select_one(selector)
                if elem and elem.get_text(strip=True):
                    department = elem.get_text(strip=True)
                    break

        vidwan_id = 'N/A'
        vidwan_match = re.search(r'vidwan.irins.org/profile/(\d+)', html_content)
        if vidwan_match:
            vidwan_id = vidwan_match.group(1)
        else:
            vidwan_link = soup.find('a', href=re.compile(r'vidwan.*profile', re.I))
            if vidwan_link:
                id_match = re.search(r'(\d+)', vidwan_link['href'])
                if id_match:
                    vidwan_id = id_match.group(1)

        expertise = 'N/A'
        expertise_heading = soup.find(['h2', 'h3', 'h4', 'strong'], text=re.compile(r'Expertise|Research Interests', re.I))
        if expertise_heading:
            next_element = expertise_heading.find_next_sibling()
            if next_element:
                expertise = next_element.get_text(strip=True, separator=', ')

        image_url = 'N/A'
        img_selectors = [
            '.profile-image img', '.faculty-image img', '.avatar img', '.user-image img',
            '.photo img', 'img.profile-photo', 'img.faculty-photo', '.profile-pic img',
            '#profile_image img', '.researcher-photo img'
        ]
        for selector in img_selectors:
            img_tag = soup.select_one(selector)
            if img_tag and img_tag.get('src'):
                image_url = urljoin(url, img_tag['src'])
                break
                
        if image_url == 'N/A':
            img_tags = soup.find_all('img')
            for img in img_tags:
                src = img.get('src', '').lower()
                alt = img.get('alt', '').lower()
                if any(keyword in src or keyword in alt for keyword in ['profile', 'faculty', 'photo', 'avatar', 'user']):
                    if img.get('src'):
                        image_url = urljoin(url, img['src'])
                        break

        if image_url == 'N/A':
            profile_divs = soup.find_all(['div', 'section'], class_=lambda x: x and any(word in str(x).lower() for word in ['profile', 'faculty', 'photo', 'member', 'person']))
            for div in profile_divs:
                img = div.find('img')
                if img and img.get('src'):
                    src = img['src']
                    if not (src.startswith('data:image') or src.endswith('.ico') or 'placeholder' in src.lower()):
                        if not (src.startswith('http://') or src.startswith('https://')):
                            src = urljoin(url, src)
                        image_url = src
                        break

        if image_url != 'N/A':
            if image_url.startswith('data:image') or image_url.endswith('.ico') or 'placeholder' in image_url.lower():
                image_url = 'N/A'

        profile_data = {
            'Institution': institution_name,
            'Name': name,
            'Department': department,
            'Vidwan-ID': vidwan_id,
            'Profile URL': url,
            'Image URL': image_url,
            'Expertise': expertise,
            'html_content': html_content
        }

        try:
            print(f"[SUCCESS] Processed: {name} | Vidwan-ID: {vidwan_id}")
        except UnicodeEncodeError:
            sanitized_name = name.encode('ascii', 'ignore').decode('ascii')
            print(f"[SUCCESS] Processed: {sanitized_name} | Vidwan-ID: {vidwan_id} (sanitized)")
            
        return profile_data

    @backoff.on_exception(backoff.expo, 
                          (asyncio.TimeoutError, ClientError, aiohttp.ClientError), 
                          max_tries=5,
                          max_time=300)
    async def fetch_html(self, session, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        try:
            timeout = aiohttp.ClientTimeout(total=120)
            async with session.get(url, timeout=timeout, headers=headers, ssl=False) as response:
                response.raise_for_status()
                content = await response.read()
                return content.decode('utf-8', errors='replace')
        except Exception as e:
            print(f"[ERROR] Failed to fetch {url}: {type(e).__name__} - {e}")
            await asyncio.sleep(1)
            return None

    async def fetch_and_process_profiles(self, urls, institution_name):
        conn = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self.fetch_html(session, url) for url in urls]
            html_contents = await asyncio.gather(*tasks)

            for i, content in enumerate(html_contents):
                if content:
                    try:
                        profile_data = self.parse_profile(content, urls[i], institution_name)
                        if profile_data:
                            self.profiles_data.append(profile_data)
                        self.processed_count += 1
                    except Exception as e:
                        print(f"[ERROR] Failed to parse profile {urls[i]}: {e}")

    def save_to_excel(self, profiles):
        if not profiles:
            print("[INFO] No data to save.")
            return
        try:
            df = pd.DataFrame([{k: v for k, v in p.items() if k != 'html_content'} for p in profiles])
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'faculty_data_export_{timestamp}.xlsx'
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Faculty Profiles', index=False)
                worksheet = writer.sheets['Faculty Profiles']
                for idx, col in enumerate(df.columns):
                    series = df[col]
                    max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 2
                    worksheet.column_dimensions[chr(65 + idx)].width = max_len
            print(f"\n[COMPLETE] Saved {len(profiles)} records to {filename}")
        except Exception as e:
            print(f"\n[ERROR] Failed to save data to Excel: {e}")

    def _is_cache_valid(self):
        if not os.path.exists(self.cache_file):
            return False
        cache_age = time.time() - os.path.getmtime(self.cache_file)
        return cache_age < self.cache_expiration_seconds

    def _load_from_cache(self):
        print("[INFO] Loading data from cache...")
        if not os.path.exists(self.cache_file):
            return []
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                else:
                    print("[WARNING] Cache file does not contain a list. Starting with an empty cache.")
                    return []
        except json.JSONDecodeError:
            print("[WARNING] Cache file is corrupted. Starting with an empty cache.")
            return []

    def _save_to_cache(self, profiles):
        print("[INFO] Saving data to cache...")
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(profiles, f, indent=4)

    async def crawl(self, keyword=None, save_excel=False):
        start_time = time.time()
        all_profiles = []

        if self._is_cache_valid():
            all_profiles = self._load_from_cache()
        else:
            print("[INFO] Cache is invalid or expired, starting crawler.")
            all_profiles = self._load_from_cache()
            existing_profiles = {profile['Profile URL']: profile for profile in all_profiles}
            print("[INFO] Starting Fast Faculty Crawler V2")
            for base_url in self.base_urls:
                institution_name = self.get_institution_name(base_url)
                print(f"[INFO] Crawling {institution_name}...")
                self.profiles_data = []
                profile_urls = await self.get_all_profile_links(base_url)
                if profile_urls:
                    await self.fetch_and_process_profiles(profile_urls, institution_name)
                    for new_profile in self.profiles_data:
                        profile_url = new_profile['Profile URL']
                        existing_profiles[profile_url] = new_profile
                else:
                    print(f"[ERROR] No profile links found for {institution_name}.")
            all_profiles = list(existing_profiles.values())
            self._save_to_cache(all_profiles)

        # Filtering logic unchanged
        if keyword and keyword.lower().startswith('name:'):
            search_term = keyword.split(':', 1)[1].lower()
            filtered_profiles = [p for p in all_profiles if p.get('Name', '').lower().startswith(search_term)]
        elif keyword and keyword.lower().startswith('vidwan:'):
            search_term = keyword.split(':', 1)[1].lower()
            filtered_profiles = [p for p in all_profiles if search_term == p.get('Vidwan-ID', '').lower()]
        elif keyword:
            keywords = [k.strip().lower() for k in keyword.split(',') if k.strip()]
            scored_profiles = []
            for p in all_profiles:
                expertise = p.get('Expertise', '').lower()
                html_content = p.get('html_content', '').lower()
                match_score = 0
                for kw in keywords:
                    if kw in expertise:
                        match_score += 2
                    elif kw in html_content:
                        match_score += 1
                if match_score > 0:
                    scored_profiles.append(p)
            scored_profiles.sort(key=lambda x: x.get('match_score', 0), reverse=True)
            filtered_profiles = scored_profiles
        else:
            filtered_profiles = all_profiles

        if save_excel:
            self.save_to_excel(filtered_profiles)
        print(f"Total execution time: {time.time() - start_time:.2f} seconds")
        return [{k: v for k, v in p.items() if k != 'html_content'} for p in filtered_profiles]


def main():
    urls = [
        "https://iitm.irins.org", "https://iith.irins.org", "https://iiti.irins.org",
        "https://iitp.irins.org", "https://iiscprofiles.irins.org", "https://iitk.irins.org",
        "https://iitd.irins.org", "https://iitr.irins.org", "https://iiserb.irins.org",
        "https://iittp.irins.org", "https://iisermohali.irins.org", "https://iitjammu.irins.org"
    ]
    crawler = FastFacultyCrawlerV2(base_urls=urls, max_concurrent_requests=5)
    try:
        asyncio.run(crawler.crawl(save_excel=True))
    except KeyboardInterrupt:
        print("\n[INFO] Crawling interrupted by user. Saving partial results...")
        if crawler.profiles_data:
            crawler.save_to_excel(crawler.profiles_data)
    except Exception as e:
        print(f"\n[ERROR] An error occurred: {e}")
        if crawler.profiles_data:
            print("[INFO] Attempting to save partial results...")
            crawler.save_to_excel(crawler.profiles_data)


if __name__ == "__main__":
    main()
