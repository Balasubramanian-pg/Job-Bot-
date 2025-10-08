import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import random
from typing import List, Dict, Optional
import logging
import re
import os

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProxyRotator:
    """Rotates through a list of proxies to avoid getting blocked."""
    def __init__(self, proxies: List[str] = None):
        self.proxies = proxies or []
        self.current_index = 0
        self.failed_proxies = set()

    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxies:
            return None
        available = [p for p in self.proxies if p not in self.failed_proxies]
        if not available:
            logger.warning("All proxies have failed. Clearing failed list and retrying.")
            self.failed_proxies.clear()
            available = self.proxies
        if not available:
            logger.error("No available proxies to use.")
            return None
        proxy = available[self.current_index % len(available)]
        self.current_index += 1
        return {'http': proxy, 'https': proxy}

    def mark_failed(self, proxy: str):
        logger.warning(f"Marking proxy as failed: {proxy}")
        self.failed_proxies.add(proxy)

class LinkedInJobScraper:
    def __init__(self, proxies: List[str] = None, use_proxies: bool = False):
        self.base_search_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        self.base_job_url = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        self.proxy_rotator = ProxyRotator(proxies) if use_proxies and proxies else None
        self.max_retries = 3
        self.retry_delay = 5
        self.backoff_factor = 2

    def _rotate_user_agent(self):
        agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1'
        ]
        self.headers['User-Agent'] = random.choice(agents)

    def _make_request(self, url: str, params: Dict = None) -> Optional[requests.Response]:
        for attempt in range(self.max_retries):
            proxies = self.proxy_rotator.get_next_proxy() if self.proxy_rotator else None
            try:
                self._rotate_user_agent()
                if attempt > 0:
                    delay = self.retry_delay * (self.backoff_factor ** (attempt - 1)) + random.uniform(0, 1)
                    logger.info(f"Retrying request to {url} in {delay:.2f} seconds...")
                    time.sleep(delay)
                response = requests.get(url, params=params, headers=self.headers, proxies=proxies, timeout=20)
                if response.status_code == 429:
                    logger.warning("Rate limit hit (429). Waiting for 60 seconds.")
                    time.sleep(60)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.ProxyError as e:
                logger.error(f"Proxy error on attempt {attempt + 1}: {e}")
                if proxies and self.proxy_rotator: self.proxy_rotator.mark_failed(proxies.get('http', ''))
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {e}")
        logger.error(f"Failed to fetch URL after {self.max_retries} attempts: {url}")
        return None

    def _parse_time_to_minutes(self, time_str: str) -> int:
        if not time_str: return 999999
        time_str = time_str.lower()
        try:
            num = int(re.search(r'(\d+)', time_str).group(1))
            if 'minute' in time_str: return num
            if 'hour' in time_str: return num * 60
            if 'day' in time_str: return num * 1440
            if 'week' in time_str: return num * 10080
            if 'month' in time_str: return num * 43200
        except (AttributeError, ValueError):
            return 999999
        return 999999

    def _parse_applicant_count(self, count_str: str) -> int:
        if not count_str: return 0
        count_str = count_str.lower()
        if 'be among the first' in count_str:
            return int(re.search(r'(\d+)', count_str).group(1))
        match = re.search(r'(\d+)', count_str)
        return int(match.group(1)) if match else 0

    def _get_job_details(self, job_url: str) -> Dict:
        if not job_url: return {}
        response = self._make_request(job_url)
        if not response: return {}
        soup = BeautifulSoup(response.text, 'html.parser')
        details = {}
        desc_div = soup.find('div', class_='show-more-less-html__markup')
        details['description'] = desc_div.get_text(separator='\n', strip=True) if desc_div else ''
        criteria_list = soup.find('ul', class_='description__job-criteria-list')
        if criteria_list:
            for item in criteria_list.find_all('li'):
                header = item.find('h3', class_='description__job-criteria-subheader')
                value = item.find('span', class_='description__job-criteria-text')
                if header and value:
                    header_text = header.text.strip().lower()
                    value_text = value.text.strip()
                    if 'seniority level' in header_text: details['experienceLevel'] = value_text
                    elif 'employment type' in header_text: details['contractType'] = value_text
                    elif 'job function' in header_text: details['workType'] = value_text
                    elif 'industries' in header_text: details['sector'] = value_text
        apply_button = soup.find('button', class_='jobs-apply-button')
        if apply_button:
            details['applyType'] = 'EASY_APPLY' if 'jobs-apply-button--easy-apply' in apply_button.get('class', []) else 'EXTERNAL'
        else:
            top_card = soup.find('div', class_='top-card-layout__entity-info')
            apply_link = top_card.find('a', {'href': True}) if top_card else None
            if apply_link and 'linkedin.com' not in apply_link['href']:
                 details['applyType'] = 'EXTERNAL'
        salary_info = soup.find('div', class_='salary-main-rail-card__salary-info-container')
        details['salary'] = salary_info.get_text(strip=True) if salary_info else ''
        return details

    def _parse_job_list(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        jobs = []
        for card in soup.find_all('li'):
            base_card = card.find('div', class_='base-card')
            if not base_card: continue
            link_tag = base_card.find('a', class_='base-card__full-link')
            job_url = link_tag.get('href', '') if link_tag else ''
            job_id_match = re.search(r'-(\d+)\?', job_url)
            if not job_id_match: continue
            title_tag = base_card.find('h3', class_='base-search-card__title')
            company_tag = base_card.find('h4', class_='base-search-card__subtitle')
            company_link_tag = company_tag.find('a') if company_tag else None
            location_tag = base_card.find('span', class_='job-search-card__location')
            time_tag = base_card.find('time', class_='job-search-card__listdate')
            applicant_tag = card.find('span', class_='job-search-card__applicant-count')
            if not applicant_tag: applicant_tag = card.find('span', class_='job-search-card__listdate--new')
            company_id = ''
            if company_link_tag:
                company_id_match = re.search(r'/company/(\d+)', company_link_tag.get('href', ''))
                if company_id_match: company_id = company_id_match.group(1)
            job = {
                'title': title_tag.text.strip() if title_tag else '', 'location': location_tag.text.strip() if location_tag else '',
                'postedTime': time_tag.text.strip() if time_tag else '', 'publishedAt': time_tag.get('datetime', '') if time_tag else '',
                'jobUrl': job_url, 'companyName': company_tag.text.strip() if company_tag else '',
                'companyUrl': company_link_tag.get('href', '') if company_link_tag else '', 'companyId': company_id,
                'applicationsCount': applicant_tag.text.strip() if applicant_tag else '0 applicants', 'description': '',
                'contractType': '', 'experienceLevel': '', 'workType': '', 'sector': '', 'salary': '', 'posterFullName': '',
                'posterProfileUrl': '', 'applyUrl': job_url, 'applyType': '', 'benefits': ''
            }
            jobs.append(job)
        return jobs
    
    # --- THIS IS THE UPDATED METHOD ---
    def search_jobs(self, keywords: str = '', location: str = '', time_period: str = 'Any time', 
                    experience_level: str = '', job_type: str = '', limit: int = 10) -> List[Dict]:
        time_filters = {'Past 24 hours': 'r86400', 'Past week': 'r604800', 'Past month': 'r2592000', 'Any time': ''}
        exp_levels = {'Internship': '1', 'Entry level': '2', 'Associate': '3', 'Mid-Senior level': '4', 'Director': '5', 'Executive': '6'}
        job_types = {'Full-time': 'F', 'Part-time': 'P', 'Contract': 'C', 'Temporary': 'T', 'Internship': 'I'}
        
        base_params = {'keywords': keywords, 'location': location}
        if time_period in time_filters and time_filters[time_period]: base_params['f_TPR'] = time_filters[time_period]
        if experience_level in exp_levels: base_params['f_E'] = exp_levels[experience_level]
        if job_type in job_types: base_params['f_JT'] = job_types[job_type]
        
        detailed_jobs = []
        start_index = 0
        
        # <-- NEW: Loop to handle pagination
        while len(detailed_jobs) < limit:
            params = base_params.copy()
            params['start'] = start_index
            
            logger.info(f"Fetching jobs page starting at index {start_index}. ({len(detailed_jobs)}/{limit} jobs collected)")
            response = self._make_request(self.base_search_url, params=params)
            
            if not response:
                logger.error("Failed to get job list page. Aborting.")
                break
            
            # <-- NEW: Parse one page of jobs
            jobs_on_page = self._parse_job_list(response.text)
            
            # <-- NEW: Stop if a page is empty (we've reached the end)
            if not jobs_on_page:
                logger.info("No more job listings found. Ending search.")
                break
            
            for job in jobs_on_page:
                # <-- NEW: Check if we've hit the limit before scraping details
                if len(detailed_jobs) >= limit:
                    break
                    
                logger.info(f"Scraping details for job {len(detailed_jobs) + 1}/{limit}: '{job['title']}'")
                try:
                    details = self._get_job_details(job['jobUrl'])
                    job.update(details)
                    detailed_jobs.append(job)
                    time.sleep(random.uniform(1.5, 3.5)) # Politeness delay
                except Exception as e:
                    logger.error(f"Error scraping details for {job['jobUrl']}: {e}")
            
            # <-- NEW: Move to the next page for the next loop iteration
            start_index += len(jobs_on_page)

        logger.info(f"Finished scraping. Total jobs collected: {len(detailed_jobs)}")

        # Sorting Logic
        for job in detailed_jobs:
            job['_time_minutes'] = self._parse_time_to_minutes(job.get('postedTime', ''))
            job['_applicant_count'] = self._parse_applicant_count(job.get('applicationsCount', ''))
        
        detailed_jobs.sort(key=lambda x: (x['_time_minutes'], x['_applicant_count']))
        
        for job in detailed_jobs:
            del job['_time_minutes']
            del job['_applicant_count']
        
        # <-- NEW: Return only the number of jobs requested by the limit
        return detailed_jobs[:limit]

    def save_to_json(self, jobs: List[Dict], filename: str = 'linkedin_jobs.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully saved {len(jobs)} jobs to {filename}")

    def save_to_csv(self, jobs: List[Dict], filename: str = 'linkedin_jobs.csv'):
        if not jobs:
            logger.warning("No jobs to save to CSV.")
            return
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(jobs[0].keys()))
            writer.writeheader()
            writer.writerows(jobs)
        logger.info(f"Successfully saved {len(jobs)} jobs to {filename}")

# --- Main Execution ---
if __name__ == "__main__":
    scraper = LinkedInJobScraper(use_proxies=False)
    
    scrape_limit = 100 
    output_directory = r"P:\Reach TA's"
    
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Output directory is set to: {output_directory}")
    except OSError as e:
        logger.error(f"Failed to create directory {output_directory}. Please check drive P: access. Error: {e}")
        exit()

    json_output_path = os.path.join(output_directory, 'linkedin_jobs_detailed.json')
    csv_output_path = os.path.join(output_directory, 'linkedin_jobs_detailed.csv')

    # --- Search Parameters ---
    # NOTE: Very specific searches might not have 100+ results.
    # Try broader terms if you don't get enough jobs.
    search_keywords = 'Data Analyst'
    search_location = 'Pune, Maharashtra, India'
    search_time = 'Past week' # Changed to 'Past week' to get more results
    search_exp = '' # Removed experience level to broaden the search
    
    logger.info("--- LinkedIn Job Scraper Initialized ---")
    logger.info(f"Attempting to scrape up to {scrape_limit} jobs...")
    
    jobs = scraper.search_jobs(
        keywords=search_keywords,
        location=search_location,
        time_period=search_time,
        experience_level=search_exp,
        limit=scrape_limit
    )
    
    if jobs:
        scraper.save_to_json(jobs, json_output_path)
        scraper.save_to_csv(jobs, csv_output_path)
        
        print(f"\nSuccessfully scraped and saved {len(jobs)} jobs to '{output_directory}'")
        print("\n--- Top 5 Job Listings (sorted by time, then applicants) ---")
        for i, job in enumerate(jobs[:5], 1):
            print(f"\n{i}. {job['title']} at {job['companyName']}")
            print(f"   Location: {job['location']}")
            print(f"   Posted: {job['postedTime']} ({job['applicationsCount']})")
            print(f"   Experience: {job.get('experienceLevel', 'N/A')}")
            print(f"   URL: {job['jobUrl']}")
    else:
        print("\nNo jobs found or an error occurred during scraping.")