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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        self.proxy_rotator = ProxyRotator(proxies) if use_proxies and proxies else None
        self.max_retries = 5
        self.retry_delay = 10
        self.backoff_factor = 2
        self.session = requests.Session()

    def _rotate_user_agent(self):
        """Rotate user agents to appear more human-like"""
        agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.2277.83',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
        ]
        self.headers['User-Agent'] = random.choice(agents)

    def _make_request(self, url: str, params: Dict = None) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and proxy rotation"""
        for attempt in range(self.max_retries):
            proxies = self.proxy_rotator.get_next_proxy() if self.proxy_rotator else None
            current_proxy_str = proxies.get('http', '') if proxies else 'No Proxy'
            
            try:
                # Rotate user agent for every attempt
                self._rotate_user_agent()
                
                # Add random delay between retries
                if attempt > 0:
                    delay = self.retry_delay * (self.backoff_factor ** (attempt - 1)) + random.uniform(2, 5)
                    logger.info(f"Attempt {attempt + 1}/{self.max_retries}: Retrying in {delay:.2f}s (proxy: {current_proxy_str})")
                    time.sleep(delay)
                else:
                    logger.info(f"Attempt {attempt + 1}/{self.max_retries}: Making request to {url}")
                
                # Make the request
                response = self.session.get(
                    url, 
                    params=params, 
                    headers=self.headers, 
                    proxies=proxies, 
                    timeout=30,
                    allow_redirects=True
                )
                
                # Check for specific status codes
                if response.status_code == 429:
                    logger.warning(f"Rate limit hit (429). Waiting 120 seconds...")
                    time.sleep(120)
                    if proxies and self.proxy_rotator: 
                        self.proxy_rotator.mark_failed(proxies.get('http', ''))
                    continue
                
                if response.status_code == 403:
                    logger.warning(f"Access forbidden (403). Likely blocked.")
                    if proxies and self.proxy_rotator: 
                        self.proxy_rotator.mark_failed(proxies.get('http', ''))
                    continue
                
                if response.status_code == 999:
                    logger.warning(f"LinkedIn custom block (999). Need better proxies/headers.")
                    if proxies and self.proxy_rotator: 
                        self.proxy_rotator.mark_failed(proxies.get('http', ''))
                    continue
                
                # Check for LinkedIn auth walls or blocks
                if "authwall" in response.url or "checkpoint" in response.url:
                    logger.error(f"Hit LinkedIn auth wall. Need better proxies.")
                    if proxies and self.proxy_rotator: 
                        self.proxy_rotator.mark_failed(proxies.get('http', ''))
                    continue
                
                # Check if we got a valid response
                if response.status_code == 200:
                    return response
                else:
                    logger.warning(f"Unexpected status {response.status_code} for {url}")
                    continue
                    
            except requests.exceptions.ProxyError as e:
                logger.error(f"Proxy error on attempt {attempt + 1} (proxy: {current_proxy_str}): {e}")
                if proxies and self.proxy_rotator: 
                    self.proxy_rotator.mark_failed(proxies.get('http', ''))
            except requests.exceptions.Timeout as e:
                logger.error(f"Request timeout on attempt {attempt + 1} (proxy: {current_proxy_str}): {e}")
                if proxies and self.proxy_rotator: 
                    self.proxy_rotator.mark_failed(proxies.get('http', ''))
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error on attempt {attempt + 1} (proxy: {current_proxy_str}): {e}")
                if proxies and self.proxy_rotator: 
                    self.proxy_rotator.mark_failed(proxies.get('http', ''))
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1} (proxy: {current_proxy_str}): {e}")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
        
        logger.error(f"Failed to fetch URL after {self.max_retries} attempts: {url}")
        return None

    def _parse_time_to_minutes(self, time_str: str) -> int:
        """Convert time string to minutes for sorting"""
        if not time_str: 
            return 999999
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
        """Parse applicant count string to integer"""
        if not count_str: 
            return 0
        count_str = count_str.lower()
        if 'be among the first' in count_str:
            try:
                return int(re.search(r'(\d+)', count_str).group(1))
            except:
                return 0
        match = re.search(r'(\d+)', count_str)
        return int(match.group(1)) if match else 0

    def _get_job_details(self, job_url: str) -> Dict:
        """Get detailed job information from job posting page"""
        if not job_url: 
            return {}
        
        logger.debug(f"Fetching job details from: {job_url}")
        response = self._make_request(job_url)
        if not response: 
            return {}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        details = {}
        
        # Extract job description
        desc_div = soup.find('div', class_='show-more-less-html__markup')
        details['description'] = desc_div.get_text(separator='\n', strip=True) if desc_div else ''
        
        # Extract job criteria (experience level, employment type, etc.)
        criteria_list = soup.find('ul', class_='description__job-criteria-list')
        if criteria_list:
            for item in criteria_list.find_all('li'):
                header = item.find('h3', class_='description__job-criteria-subheader')
                value = item.find('span', class_='description__job-criteria-text')
                if header and value:
                    header_text = header.text.strip().lower()
                    value_text = value.text.strip()
                    if 'seniority level' in header_text: 
                        details['experienceLevel'] = value_text
                    elif 'employment type' in header_text: 
                        details['contractType'] = value_text
                    elif 'job function' in header_text: 
                        details['workType'] = value_text
                    elif 'industries' in header_text: 
                        details['sector'] = value_text
        
        # Determine apply type
        apply_button = soup.find('button', class_='jobs-apply-button')
        if apply_button:
            details['applyType'] = 'EASY_APPLY' if 'jobs-apply-button--easy-apply' in apply_button.get('class', []) else 'EXTERNAL'
        else:
            top_card = soup.find('div', class_='top-card-layout__entity-info')
            apply_link = top_card.find('a', {'href': True, 'data-tracking-control-name': 'public_jobs_apply_external'}) if top_card else None
            if apply_link:
                details['applyType'] = 'EXTERNAL'
            else:
                details['applyType'] = 'UNKNOWN'

        # Extract salary information
        salary_info = soup.find('div', class_='salary-main-rail-card__salary-info-container')
        details['salary'] = salary_info.get_text(strip=True) if salary_info else ''
        
        return details

    def _parse_job_list(self, html: str) -> List[Dict]:
        """Parse job listings from search results HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        jobs = []
        
        # Look for the main job list container
        job_list_container = soup.find('ul', class_='jobs-search__results-list')
        if not job_list_container:
            logger.warning("Could not find main job list container. LinkedIn structure may have changed.")
            # Try alternative selectors
            job_list_container = soup.find('ul', class_='jobs-search__results-list')
            if not job_list_container:
                return []

        for card in job_list_container.find_all('li'):
            base_card = card.find('div', class_='base-card')
            if not base_card: 
                continue
                
            link_tag = base_card.find('a', class_='base-card__full-link')
            job_url = link_tag.get('href', '') if link_tag else ''
            
            # Extract job ID with more flexible regex
            job_id_match = re.search(r'[^-]*-(\d+)(?:\?|$)', job_url)
            if not job_id_match: 
                continue
            
            # Extract job details
            title_tag = base_card.find('h3', class_='base-search-card__title')
            company_tag = base_card.find('h4', class_='base-search-card__subtitle')
            company_link_tag = company_tag.find('a') if company_tag else None
            location_tag = base_card.find('span', class_='job-search-card__location')
            time_tag = base_card.find('time', class_='job-search-card__listdate')
            applicant_tag = card.find('span', class_='job-search-card__applicant-count')
            
            if not applicant_tag: 
                applicant_tag = card.find('span', class_='job-search-card__listdate--new')
            
            # Extract company ID
            company_id = ''
            if company_link_tag:
                company_id_match = re.search(r'/company/(\d+)', company_link_tag.get('href', ''))
                if company_id_match: 
                    company_id = company_id_match.group(1)
            
            # Check for Easy Apply
            easy_apply_tag = card.find('span', class_='job-card-list__easy-apply-label')
            is_easy_apply = 'EASY_APPLY' if easy_apply_tag else 'UNKNOWN'

            job = {
                'title': title_tag.get_text(strip=True) if title_tag else '',
                'location': location_tag.get_text(strip=True) if location_tag else '',
                'postedTime': time_tag.get_text(strip=True) if time_tag else '',
                'publishedAt': time_tag.get('datetime', '') if time_tag else '',
                'jobUrl': job_url,
                'companyName': company_tag.get_text(strip=True) if company_tag else '',
                'companyUrl': company_link_tag.get('href', '') if company_link_tag else '',
                'companyId': company_id,
                'applicationsCount': applicant_tag.get_text(strip=True) if applicant_tag else '0 applicants',
                'description': '',  # Will be filled later
                'contractType': '', 'experienceLevel': '', 'workType': '', 'sector': '', 'salary': '',
                'posterFullName': '', 'posterProfileUrl': '',
                'applyUrl': job_url, 'applyType': is_easy_apply,
                'benefits': ''
            }
            jobs.append(job)
            
        logger.info(f"Parsed {len(jobs)} jobs from current page")
        return jobs
    
    def search_jobs(self, keywords: str = '', location: str = '', time_period: str = 'Any time', 
                    experience_level: str = '', job_type: str = '', limit: int = 10, easy_apply_only: bool = False) -> List[Dict]:
        """Search for jobs with given parameters"""
        # LinkedIn parameter mappings
        time_filters = {'Past 24 hours': 'r86400', 'Past week': 'r604800', 'Past month': 'r2592000', 'Any time': ''}
        exp_levels = {'Internship': '1', 'Entry level': '2', 'Associate': '3', 'Mid-Senior level': '4', 'Director': '5', 'Executive': '6'}
        job_types = {'Full-time': 'F', 'Part-time': 'P', 'Contract': 'C', 'Temporary': 'T', 'Internship': 'I'}
        
        base_params = {'keywords': keywords, 'location': location}
        if time_period in time_filters and time_filters[time_period]: 
            base_params['f_TPR'] = time_filters[time_period]
        if experience_level in exp_levels: 
            base_params['f_E'] = exp_levels[experience_level]
        if job_type in job_types: 
            base_params['f_JT'] = job_types[job_type]
        
        detailed_jobs = []
        start_index = 0
        max_raw_jobs_to_fetch = limit * 10  # Safety limit
        
        jobs_processed_from_raw_list = 0
        consecutive_empty_pages = 0

        while (jobs_processed_from_raw_list < max_raw_jobs_to_fetch and 
               len(detailed_jobs) < limit and 
               consecutive_empty_pages < 3):
            
            params = base_params.copy()
            params['start'] = start_index
            
            logger.info(f"Fetching page {start_index//25 + 1} for '{location}'. Collected {len(detailed_jobs)}/{limit} jobs.")
            
            response = self._make_request(self.base_search_url, params=params)
            
            if not response:
                logger.error(f"Failed to get job list page. Aborting this search.")
                break
            
            jobs_on_page = self._parse_job_list(response.text)
            
            if not jobs_on_page:
                consecutive_empty_pages += 1
                logger.info(f"No jobs found on page {start_index//25 + 1}. Consecutive empty pages: {consecutive_empty_pages}")
                if consecutive_empty_pages >= 3:
                    logger.info("Too many consecutive empty pages. Ending search.")
                    break
                start_index += 25
                continue
            
            consecutive_empty_pages = 0  # Reset counter
            
            for job in jobs_on_page:
                if len(detailed_jobs) >= limit:
                    break
                    
                jobs_processed_from_raw_list += 1
                
                # Skip if easy_apply_only is enabled and this isn't an easy apply job
                if easy_apply_only and job.get('applyType') != 'EASY_APPLY':
                    logger.debug(f"Skipping non-Easy Apply job: {job['title']}")
                    continue
                
                logger.info(f"Processing: {job['title']} at {job['companyName']}")
                
                try:
                    # Get detailed job information
                    details = self._get_job_details(job['jobUrl'])
                    job.update(details)
                    
                    # Double-check apply type after getting details
                    if easy_apply_only and job.get('applyType') != 'EASY_APPLY':
                        logger.debug(f"Skipping - not Easy Apply after details check: {job['title']}")
                        continue
                    
                    detailed_jobs.append(job)
                    logger.info(f"✅ Added job: {job['title']} (Total: {len(detailed_jobs)}/{limit})")
                    
                    # Random delay between detail requests
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    logger.error(f"Error scraping details for {job['jobUrl']}: {e}")
                    continue
            
            start_index += len(jobs_on_page)
            
            # Random delay between page requests
            time.sleep(random.uniform(3, 6))

        logger.info(f"Finished scraping for '{location}'. Total jobs collected: {len(detailed_jobs)}")
        
        # Sort by time and applicant count
        for job in detailed_jobs:
            job['_time_minutes'] = self._parse_time_to_minutes(job.get('postedTime', ''))
            job['_applicant_count'] = self._parse_applicant_count(job.get('applicationsCount', ''))
        
        detailed_jobs.sort(key=lambda x: (x['_time_minutes'], x['_applicant_count']))
        
        # Clean up temporary fields
        for job in detailed_jobs:
            if '_time_minutes' in job: 
                del job['_time_minutes']
            if '_applicant_count' in job: 
                del job['_applicant_count']
        
        return detailed_jobs[:limit]

    def save_to_json(self, jobs: List[Dict], filename: str = 'linkedin_jobs.json'):
        """Save jobs to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(jobs)} jobs to {filename}")

    def save_to_csv(self, jobs: List[Dict], filename: str = 'linkedin_jobs.csv'):
        """Save jobs to CSV file"""
        if not jobs:
            logger.warning("No jobs to save to CSV.")
            return
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(jobs[0].keys()))
            writer.writeheader()
            writer.writerows(jobs)
        logger.info(f"Saved {len(jobs)} jobs to {filename}")

# --- Main Execution ---
if __name__ == "__main__":
    # IMPORTANT: Add your actual proxies here for best results
    # Format: ['http://user:pass@proxy1:port', 'http://user:pass@proxy2:port']
    PROXIES = [
        # Add your proxy list here
        # 'http://username:password@proxy1:port',
        # 'http://username:password@proxy2:port',
    ]
    
    # Initialize scraper with proxies if available
    if PROXIES:
        scraper = LinkedInJobScraper(proxies=PROXIES, use_proxies=True)
        logger.info("Using proxies for scraping")
    else:
        scraper = LinkedInJobScraper(use_proxies=False)
        logger.warning("No proxies configured - using direct connection (may get blocked)")
    
    # Configuration
    total_jobs_target = 200 
    output_directory = r"P:\Reach TA's" 
    
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Output directory: {output_directory}")
    except OSError as e:
        logger.error(f"Failed to create directory: {e}")
        # Fallback to current directory
        output_directory = "."
        logger.info(f"Using current directory: {output_directory}")

    all_scraped_jobs = []
    
    # Search configurations
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

    num_search_combinations = len(search_locations) * len(search_configs)
    jobs_to_fetch_per_individual_search = max(20, (total_jobs_target // num_search_combinations) + 10)

    logger.info(f"Starting search with {num_search_combinations} search combinations")
    logger.info(f"Targeting {jobs_to_fetch_per_individual_search} jobs per search")

    # Perform searches
    for location in search_locations:
        print(f"\n{'='*60}")
        print(f"Searching in: {location}")
        print(f"{'='*60}")
        
        for config in search_configs:
            logger.info(f"--- Starting: {config['label']} in {location} ---")
            
            try:
                jobs_from_this_search = scraper.search_jobs(
                    keywords=config['keywords'],
                    location=location,
                    time_period=search_time,
                    experience_level=config['experience_level'],
                    limit=jobs_to_fetch_per_individual_search,
                    easy_apply_only=True
                )
                
                for job in jobs_from_this_search:
                    job['search_config'] = f"{config['label']} ({location})"
                    all_scraped_jobs.append(job)
                
                logger.info(f"Found {len(jobs_from_this_search)} jobs from {config['label']} in {location}")
                
                # Longer delay between different search configurations
                time.sleep(random.uniform(10, 15))
                
            except Exception as e:
                logger.error(f"Error in search {config['label']} in {location}: {e}")
                continue

    # Remove duplicates based on jobUrl
    unique_jobs = {}
    for job in all_scraped_jobs:
        if job['jobUrl'] in unique_jobs:
            # Keep the one with more complete information
            existing = unique_jobs[job['jobUrl']]
            if len(job.get('description', '')) > len(existing.get('description', '')):
                unique_jobs[job['jobUrl']] = job
        else:
            unique_jobs[job['jobUrl']] = job
    
    final_jobs = list(unique_jobs.values())

    # Sort final list
    for job in final_jobs:
        job['_time_minutes'] = scraper._parse_time_to_minutes(job.get('postedTime', ''))
        job['_applicant_count'] = scraper._parse_applicant_count(job.get('applicationsCount', ''))
    
    final_jobs.sort(key=lambda x: (x['_time_minutes'], x['_applicant_count']))
    
    for job in final_jobs:
        if '_time_minutes' in job: 
            del job['_time_minutes']
        if '_applicant_count' in job: 
            del job['_applicant_count']

    # Save results
    json_output_path = os.path.join(output_directory, 'linkedin_fpna_easy_apply_jobs_200.json') 
    csv_output_path = os.path.join(output_directory, 'linkedin_fpna_easy_apply_jobs_200.csv') 
    
    if final_jobs:
        final_jobs_capped = final_jobs[:total_jobs_target]

        scraper.save_to_json(final_jobs_capped, json_output_path)
        scraper.save_to_csv(final_jobs_capped, csv_output_path)
        
        print(f"\n{'='*60}")
        print(f"SUCCESS: Scraped and saved {len(final_jobs_capped)} unique Easy Apply jobs")
        print(f"JSON: {json_output_path}")
        print(f"CSV: {csv_output_path}")
        print(f"{'='*60}")
        
        print("\n--- Top 10 Most Relevant Easy Apply Jobs ---")
        for i, job in enumerate(final_jobs_capped[:10], 1):
            print(f"\n{i}. {job['title']} at {job['companyName']}")
            print(f"   Location: {job['location']}")
            print(f"   Posted: {job['postedTime']} ({job['applicationsCount']})")
            print(f"   Experience: {job.get('experienceLevel', 'N/A')}")
            print(f"   Apply Type: {job.get('applyType', 'N/A')}")
            print(f"   Search: {job.get('search_config', 'N/A')}")
            print(f"   URL: {job['jobUrl']}")
    else:
        print("\n❌ No Easy Apply jobs found matching your criteria.")
        print("Possible reasons:")
        print("1. LinkedIn is blocking requests (need better proxies)")
        print("2. No Easy Apply jobs match your search criteria")
        print("3. LinkedIn's HTML structure has changed")
        print("4. Network connectivity issues")