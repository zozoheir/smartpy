import asyncio
import time
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from rumorz_data.scraping.helpers import remove_html_tags
from rumorz_data.scraping.util.user_agents import browser_user_agents


class ChromiumPlaywrightContext:

    async def __aenter__(self,
                         screen_size=(1920, 1080)):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context(user_agent=random.choice(browser_user_agents),
                                                      viewport={'width': screen_size[0], 'height': screen_size[1]})
        self.page = await self.context.new_page()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.browser.close()
        await self.playwright.stop()

    async def get_url_content(self,
                              url,
                              wait_for_body=True,
                              wait_for_text=None,
                              timeout=30000):
        await self.page.goto(url)
        if wait_for_text:
            await self.page.wait_for_function(f"document.body.innerText.includes('{wait_for_text}')", timeout=timeout)
        elif wait_for_body:
            await self.page.wait_for_selector("body")
            await asyncio.sleep(2)

        source_code = await self.page.content()
        content = trafilatura.extract(source_code, include_comments=False)
        if content:
            content = remove_html_tags(content)
            return content


import random
from selenium import webdriver
import trafilatura


class SeleniumChromeDriverContext:
    def __enter__(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("--window-size=1920,1080")
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": random.choice(browser_user_agents)})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()

    def get_url_content(self, url):
        self.driver.get(url)
        content = trafilatura.extract(self.driver.page_source, include_comments=False)
        if content:
            content = remove_html_tags(content)
        return content



def extract_urls_from_source_code(source_code):
    soup = BeautifulSoup(source_code, 'html.parser')
    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and urlparse(href).scheme:
            links.append(href)
    return links
