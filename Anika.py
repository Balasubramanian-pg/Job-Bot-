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
        
        # Determine applyType based on button existence and class
        apply_button = soup.find('button', class_='jobs-apply-button')
        if apply_button:
            details['applyType'] = 'EASY_APPLY' if 'jobs-apply-button--easy-apply' in apply_button.get('class', []) else 'EXTERNAL'
        else:
            # Fallback if the standard button is not found, check for an external link within the top card
            top_card = soup.find('div', class_='top-card-layout__entity-info')
            apply_link = top_card.find('a', {'href': True, 'data-tracking-control-name': 'public_jobs_apply_external'}) if top_card else None
            if apply_link: # If an external apply link is explicitly found
                details['applyType'] = 'EXTERNAL'
            else: # If no explicit easy apply or external link found, assume external or unknown
                details['applyType'] = 'UNKNOWN' 

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
            
            # Try to determine if it's Easy Apply from the list card (best effort)
            easy_apply_tag = card.find('span', class_='job-card-list__easy-apply-label')
            is_easy_apply = 'EASY_APPLY' if easy_apply_tag else 'UNKNOWN' # Will be confirmed in details

            job = {
                'title': title_tag.text.strip() if title_tag else '', 'location': location_tag.text.strip() if location_tag else '',
                'postedTime': time_tag.text.strip() if time_tag else '', 'publishedAt': time_tag.get('datetime', '') if time_tag else '',
                'jobUrl': job_url, 'companyName': company_tag.text.strip() if company_tag else '',
                'companyUrl': company_link_tag.get('href', '') if company_link_tag else '', 'companyId': company_id,
                'applicationsCount': applicant_tag.text.strip() if applicant_tag else '0 applicants', 'description': '',
                'contractType': '', 'experienceLevel': '', 'workType': '', 'sector': '', 'salary': '', 'posterFullName': '',
                'posterProfileUrl': '', 'applyUrl': job_url, 'applyType': is_easy_apply, # Initial guess for applyType
                'benefits': ''
            }
            jobs.append(job)
        return jobs
    
    def search_jobs(self, keywords: str = '', location: str = '', time_period: str = 'Any time', 
                    experience_level: str = '', job_type: str = '', limit: int = 10, easy_apply_only: bool = False) -> List[Dict]:
        time_filters = {'Past 24 hours': 'r86400', 'Past week': 'r604800', 'Past month': 'r2592000', 'Any time': ''}
        exp_levels = {'Internship': '1', 'Entry level': '2', 'Associate': '3', 'Mid-Senior level': '4', 'Director': '5', 'Executive': '6'}
        job_types = {'Full-time': 'F', 'Part-time': 'P', 'Contract': 'C', 'Temporary': 'T', 'Internship': 'I'}
        
        base_params = {'keywords': keywords, 'location': location}
        if time_period in time_filters and time_filters[time_period]: base_params['f_TPR'] = time_filters[time_period]
        if experience_level in exp_levels: base_params['f_E'] = exp_levels[experience_level]
        if job_type in job_types: base_params['f_JT'] = job_types[job_type]
        
        detailed_jobs = []
        start_index = 0
        
        # We need to fetch more raw jobs than the limit if we're filtering,
        # as many will be discarded. Let's aim to fetch 2-3x the limit initially.
        # This is an heuristic and might need adjustment.
        pages_to_fetch_per_search = max(1, (limit * 3) // 25) # LinkedIn shows 25 jobs per page.
        
        jobs_fetched_on_current_search = 0

        while jobs_fetched_on_current_search < limit:
            params = base_params.copy()
            params['start'] = start_index
            
            logger.info(f"Fetching jobs page starting at index {start_index} for location '{location}'. ({len(detailed_jobs)}/{limit} Easy Apply jobs collected so far)")
            response = self._make_request(self.base_search_url, params=params)
            
            if not response:
                logger.error(f"Failed to get job list page for location '{location}'. Aborting this search segment.")
                break
            
            jobs_on_page = self._parse_job_list(response.text)
            
            if not jobs_on_page:
                logger.info(f"No more job listings found on page {start_index // 25 + 1} for location '{location}'. Ending search segment.")
                break
            
            for job in jobs_on_page:
                if jobs_fetched_on_current_search >= limit:
                    break
                    
                logger.info(f"Checking details for: '{job['title']}' at '{job['companyName']}'")
                try:
                    details = self._get_job_details(job['jobUrl'])
                    job.update(details) # Update job with full details including accurate applyType

                    if easy_apply_only and job.get('applyType') == 'EASY_APPLY':
                        logger.info(f"✅ Easy Apply job found: '{job['title']}'")
                        detailed_jobs.append(job)
                        jobs_fetched_on_current_search += 1
                        time.sleep(random.uniform(1.5, 3.5)) # Politeness delay for successful scrapes
                    elif not easy_apply_only:
                        detailed_jobs.append(job)
                        jobs_fetched_on_current_search += 1
                        time.sleep(random.uniform(1.5, 3.5)) # Politeness delay for successful scrapes
                    else:
                        logger.debug(f"Skipping non-Easy Apply job: '{job['title']}' (Apply Type: {job.get('applyType')})")
                except Exception as e:
                    logger.error(f"Error scraping details for {job['jobUrl']}: {e}")
                
                # Small delay even for skipped jobs to avoid rapid requests to details pages
                time.sleep(random.uniform(0.5, 1.5)) 
            
            start_index += len(jobs_on_page)
            # Add a slight delay between page requests
            time.sleep(random.uniform(2, 4))


        logger.info(f"Finished scraping for location '{location}'. Total Easy Apply jobs collected: {len(detailed_jobs)}")

        # Sorting Logic
        for job in detailed_jobs:
            job['_time_minutes'] = self._parse_time_to_minutes(job.get('postedTime', ''))
            job['_applicant_count'] = self._parse_applicant_count(job.get('applicationsCount', ''))
        
        detailed_jobs.sort(key=lambda x: (x['_time_minutes'], x['_applicant_count']))
        
        for job in detailed_jobs:
            del job['_time_minutes']
            del job['_applicant_count']
        
        return detailed_jobs[:limit] # Return only up to the requested limit

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
    
    # We will try to get `total_jobs_limit` *across all locations and searches*.
    # Since we filter for Easy Apply, we might need to scrape more initially.
    total_jobs_target = 100 
    output_directory = r"P:\Reach TA's" 
    
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Output directory is set to: {output_directory}")
    except OSError as e:
        logger.error(f"Failed to create directory {output_directory}. Please check drive P: access. Error: {e}")
        exit()

    all_scraped_jobs = []
    
    # --- Search Parameters for your girlfriend's roles ---
    primary_keywords = "Financial planning and analysis, FP&A, Budgeting, Financial forecasting, Variance analysis, Opex capex, P&L, Balance sheet, Performance, Profitability"
    secondary_keywords = "Financial Analyst, Financial Consultant, Business Plan, Process improvement, Audit, Annual Operating Plan"

    search_locations = ['Pune, Maharashtra, India', 'Bangalore, Karnataka, India', 'Mumbai, Maharashtra, India']
    search_time = 'Past week' 
    
    search_configs = [
        {
            'keywords': primary_keywords,
            'experience_level': 'Associate',
            'label': 'Primary FP&A (Associate)'
        },
        {
            'keywords': primary_keywords,
            'experience_level': 'Mid-Senior level',
            'label': 'Primary FP&A (Mid-Senior)'
        },
        {
            'keywords': secondary_keywords,
            'experience_level': 'Associate',
            'label': 'Secondary Financial Roles (Associate)'
        },
        {
            'keywords': secondary_keywords,
            'experience_level': 'Mid-Senior level',
            'label': 'Secondary Financial Roles (Mid-Senior)'
        },
        {
            'keywords': 'FP&A Head',
            'experience_level': 'Mid-Senior level',
            'label': 'Specific FP&A Head (Mid-Senior)'
        },
        {
            'keywords': 'FP&A Head',
            'experience_level': 'Director',
            'label': 'Specific FP&A Head (Director)'
        }
    ]

    for location in search_locations:
        print(f"\nSearching in: {location}")
        for config in search_configs:
            logger.info(f"\n--- Starting search: {config['label']} in {location} ---")
            
            # Divide the total_jobs_target by the number of locations/configs for a more even spread
            # This is an approximate target per search combination
            jobs_to_fetch_per_specific_search = total_jobs_target // (len(search_locations) * len(search_configs)) + 5 
            if jobs_to_fetch_per_specific_search < 10: jobs_to_fetch_per_specific_search = 10 # ensure a minimum

            jobs_from_this_search = scraper.search_jobs(
                keywords=config['keywords'],
                location=location,
                time_period=search_time,
                experience_level=config['experience_level'],
                limit=jobs_to_fetch_per_specific_search, # Limit per specific location/config search
                easy_apply_only=True # <-- THIS IS THE NEW FILTER
            )
            
            for job in jobs_from_this_search:
                job['search_config'] = f"{config['label']} ({location})" # Add location to config label
                all_scraped_jobs.append(job)
            
            time.sleep(random.uniform(5, 10)) # Delay between different search queries

    # Remove duplicates based on jobUrl
    unique_jobs = {job['jobUrl']: job for job in all_scraped_jobs}.values()
    final_jobs = list(unique_jobs)

    # Re-sort the final list based on time and applicant count
    for job in final_jobs:
        job['_time_minutes'] = scraper._parse_time_to_minutes(job.get('postedTime', ''))
        job['_applicant_count'] = scraper._parse_applicant_count(job.get('applicationsCount', ''))
    
    final_jobs.sort(key=lambda x: (x['_time_minutes'], x['_applicant_count']))
    
    for job in final_jobs:
        if '_time_minutes' in job: del job['_time_minutes']
        if '_applicant_count' in job: del job['_applicant_count']

    json_output_path = os.path.join(output_directory, 'linkedin_fpna_easy_apply_jobs.json')
    csv_output_path = os.path.join(output_directory, 'linkedin_fpna_easy_apply_jobs.csv')
    
    if final_jobs:
        # Cap the final output to total_jobs_target if more were collected
        final_jobs_capped = final_jobs[:total_jobs_target]

        scraper.save_to_json(final_jobs_capped, json_output_path)
        scraper.save_to_csv(final_jobs_capped, csv_output_path)
        
        print(f"\nSuccessfully scraped and saved {len(final_jobs_capped)} unique Easy Apply jobs to '{output_directory}'")
        print("\n--- Top 10 Relevant Easy Apply Job Listings (sorted by time, then applicants) ---")
        for i, job in enumerate(final_jobs_capped[:10], 1):
            print(f"\n{i}. {job['title']} at {job['companyName']}")
            print(f"   Location: {job['location']}")
            print(f"   Posted: {job['postedTime']} ({job['applicationsCount']})")
            print(f"   Experience: {job.get('experienceLevel', 'N/A')}")
            print(f"   Apply Type: {job.get('applyType', 'N/A')}")
            print(f"   URL: {job['jobUrl']}")
    else:
        print("\nNo Easy Apply jobs found matching your criteria or an error occurred during scraping.")
