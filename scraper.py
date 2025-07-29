import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import collections
import time
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class GovMyCrawler:
    """
    A web crawler that reads a list of seed domains, then recursively
    scrapes them to find and list all unique .gov.my domains.
    """
    def __init__(self, input_file):
        self.input_file = input_file
        # A double-ended queue for URLs to visit
        self.queue = collections.deque()
        # A set to store URLs we have already visited to prevent re-crawling
        self.visited_urls = set()
        # A set to store the unique .gov.my domains we find
        self.found_domains = set()
        # A set to store domains from the seed file
        self.seed_domains = set()
        # A set to store domains that were discovered during crawling (not from seed file)
        self.discovered_domains = set()
        # A set to store domains that have been scraped to prevent re-scraping
        self.scraped_domains = set()
        # File to save discovered domains in real-time
        self.discovered_file = "discovered_domains_realtime.txt"
        # File to track scraped websites in real-time
        self.scraped_file = "websites_scraped.txt"
        # Thread locks for thread-safe operations
        self.queue_lock = threading.Lock()
        self.visited_lock = threading.Lock()
        self.found_domains_lock = threading.Lock()
        self.scraped_domains_lock = threading.Lock()
        self.file_lock = threading.Lock()
        # Set a user-agent to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(self.headers)
        
        # Initialize the discovered domains file
        self._initialize_discovered_file()
        # Initialize the scraped domains file
        self._initialize_scraped_file()

    def _initialize_discovered_file(self):
        """Initialize the real-time discovered domains file."""
        with open(self.discovered_file, 'w') as f:
            f.write(f"# Newly discovered .gov.my domains (real-time)\n")
            f.write(f"# Started crawling on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    def _initialize_scraped_file(self):
        """Initialize the real-time scraped websites file."""
        with open(self.scraped_file, 'w') as f:
            f.write(f"# Scraped .gov.my domains (real-time)\n")
            f.write(f"# Started crawling on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    def _save_discovered_domain(self, domain):
        """Append a newly discovered domain to the real-time file."""
        with self.file_lock:
            with open(self.discovered_file, 'a') as f:
                f.write(f"{domain}\n")

    def _save_scraped_domain(self, domain):
        """Append a scraped domain to the real-time file."""
        with self.file_lock:
            with open(self.scraped_file, 'a') as f:
                f.write(f"{domain}\n")

    def _initialize_queue(self):
        """Reads the input file and populates the initial crawl queue."""
        if not os.path.exists(self.input_file):
            print(f"❌ Error: Input file '{self.input_file}' not found.")
            print("Please create this file and populate it with your list of domains.")
            return False
            
        print(f"📖 Reading seed domains from '{self.input_file}'...")
        with open(self.input_file, 'r') as f:
            for line in f:
                domain = line.strip()
                if domain:
                    # Extract top-level domain to avoid duplicates from subdomains
                    top_level = self._extract_top_level_domain(domain)
                    if top_level:
                        # Add to seed domains set
                        self.seed_domains.add(top_level)
                        # Add both http and https versions to be safe
                        self.queue.append(f"http://{domain}")
                        self.queue.append(f"https://{domain}")
                        self.found_domains.add(top_level) # Add the top-level domain
        
        if not self.queue:
            print("⚠️  Warning: No domains found in the input file.")
            return False
            
        print(f"✅ Initialized queue with {len(self.queue)} URLs from {len(self.seed_domains)} seed domains.")
        return True

    def _extract_top_level_domain(self, domain):
        """
        Extracts the top-level domain from a potentially subdomain.
        Example: subdomain.example.gov.my -> example.gov.my
        """
        if not domain or not domain.endswith('.gov.my'):
            return None
            
        # Split the domain into parts
        parts = domain.split('.')
        
        # For .gov.my domains, we want the structure: [name].gov.my
        # If there are more parts, it's a subdomain
        if len(parts) >= 3:
            # Take the last 3 parts: [name].gov.my
            top_level = '.'.join(parts[-3:])
            return top_level
        
        return domain
    
    def _create_session(self):
        """Create a new session for thread-safe requests."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(self.headers)
        return session
    
    def _extract_links(self, current_url, soup):
        """Extracts top-level domains from all links on a page and adds them to the queue."""
        links_found = 0
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Create an absolute URL from a relative one (e.g., /contact-us)
            absolute_url = urljoin(current_url, href)
            
            try:
                parsed_url = urlparse(absolute_url)
                domain = parsed_url.netloc

                # We are only interested in .gov.my domains
                if domain and domain.endswith('.gov.my'):
                    # Extract top-level domain (remove subdomains)
                    top_level_domain = self._extract_top_level_domain(domain)
                    
                    if top_level_domain:
                        # Add the top-level domain to our set of findings (thread-safe)
                        with self.found_domains_lock:
                            if top_level_domain not in self.found_domains:
                                # Check if this is a truly new discovery (not in seed file)
                                if top_level_domain not in self.seed_domains:
                                    print(f"✅ Found new top-level domain: {top_level_domain}")
                                    # Save to discovered domains set and file
                                    self.discovered_domains.add(top_level_domain)
                                    self._save_discovered_domain(top_level_domain)
                                else:
                                    print(f"🔄 Found seed domain during crawl: {top_level_domain}")
                                
                                self.found_domains.add(top_level_domain)
                        
                        # Only add URL to queue if the domain hasn't been scraped yet and URL is new (thread-safe)
                        with self.visited_lock:
                            with self.scraped_domains_lock:
                                if (absolute_url not in self.visited_urls and 
                                    top_level_domain not in self.scraped_domains):
                                    with self.queue_lock:
                                        self.queue.append(absolute_url)
                                        links_found += 1
            except Exception as e:
                print(f"⚠️  Could not parse URL {absolute_url}: {e}")
        
        return links_found

    def _crawl_single_url(self, current_url):
        """Crawl a single URL - designed to be run in a thread."""
        # Create a session for this thread
        session = self._create_session()
        
        try:
            # Check if URL was already visited (thread-safe)
            with self.visited_lock:
                if current_url in self.visited_urls:
                    return None
                
                # Mark URL as visited
                self.visited_urls.add(current_url)
                
                # Also mark the http/https counterpart as visited
                if current_url.startswith("http://"):
                    https_version = "https://" + current_url[7:]
                    self.visited_urls.add(https_version)
                elif current_url.startswith("https://"):
                    http_version = "http://" + current_url[8:]
                    self.visited_urls.add(http_version)

            # Extract domain from current URL
            try:
                parsed_url = urlparse(current_url)
                current_domain = parsed_url.netloc
                current_top_level_domain = self._extract_top_level_domain(current_domain)
                
                # Skip if we've already scraped this domain (thread-safe)
                with self.scraped_domains_lock:
                    if current_top_level_domain and current_top_level_domain in self.scraped_domains:
                        print(f"⏭️  Skipping {current_url} - domain {current_top_level_domain} already scraped")
                        return None
                        
            except Exception as e:
                print(f"⚠️  Could not parse domain from {current_url}: {e}")
                return None

            print(f"🔎 Crawling: {current_url}")

            try:
                response = session.get(current_url, timeout=15, allow_redirects=True)
                
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in content_type:
                        # Mark this domain as scraped (thread-safe)
                        with self.scraped_domains_lock:
                            if current_top_level_domain and current_top_level_domain not in self.scraped_domains:
                                self.scraped_domains.add(current_top_level_domain)
                                self._save_scraped_domain(current_top_level_domain)
                                print(f"   ✅ Marked {current_top_level_domain} as scraped")
                        
                        soup = BeautifulSoup(response.content, 'html.parser')
                        links_found = self._extract_links(current_url, soup)
                        print(f"   -> Found {links_found} new links to crawl")
                        return links_found
                    else:
                        print(f"   -> Skipped (Not HTML content: {content_type})")
                elif response.status_code == 404:
                    print(f"   -> Website not found (404)")
                elif response.status_code == 403:
                    print(f"   -> Access forbidden (403)")
                elif response.status_code >= 500:
                    print(f"   -> Server error ({response.status_code})")
                else:
                    print(f"   -> Skipped (Status: {response.status_code})")

            except requests.exceptions.ConnectionError as e:
                print(f"   -> Connection failed: Website may not exist")
            except requests.exceptions.Timeout as e:
                print(f"   -> Request timed out")
            except requests.exceptions.TooManyRedirects as e:
                print(f"   -> Too many redirects")
            except requests.exceptions.SSLError as e:
                print(f"   -> SSL/Certificate error")
            except requests.exceptions.RequestException as e:
                print(f"   -> Request error: {type(e).__name__}")
            except Exception as e:
                print(f"   -> Unexpected error: {type(e).__name__}: {e}")
                
        finally:
            session.close()
            
        return None

    def crawl(self, crawl_limit=500, max_workers=50):
        """Starts the crawling process with threading."""
        if not self._initialize_queue():
            return

        print(f"\n🚀 Starting recursive crawl with {max_workers} threads...")
        crawled_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while crawled_count < crawl_limit:
                # Get URLs to process in this batch
                urls_to_process = []
                with self.queue_lock:
                    batch_size = min(max_workers, crawl_limit - crawled_count, len(self.queue))
                    if batch_size == 0:
                        break
                    
                    for _ in range(batch_size):
                        if self.queue:
                            urls_to_process.append(self.queue.popleft())
                        else:
                            break

                if not urls_to_process:
                    break

                # Submit all URLs in this batch to the thread pool
                future_to_url = {executor.submit(self._crawl_single_url, url): url for url in urls_to_process}
                
                # Process completed futures
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    crawled_count += 1
                    try:
                        links_found = future.result()
                        if links_found is not None:
                            print(f"✅ Completed crawling ({crawled_count}/{crawl_limit}): {url}")
                    except Exception as exc:
                        print(f"❌ Error crawling {url}: {exc}")
                
                # Small delay to be respectful to servers
                time.sleep(0.1)

        print("\n🏁 Crawl finished or limit reached.")
        self.save_results()

    def cleanup(self):
        """Clean up the session."""
        if hasattr(self, 'session'):
            self.session.close()

    def save_results(self):
        """Saves the found top-level domains to a text file."""
        output_file = "gov_my_top_level_domains.txt"
        print(f"\nFound {len(self.found_domains)} unique top-level .gov.my domains.")
        print(f"Seed domains from input file: {len(self.seed_domains)}")
        print(f"Newly discovered domains during crawling: {len(self.discovered_domains)}")
        print(f"Domains actually scraped: {len(self.scraped_domains)}")
        print(f"Crawled {len(self.visited_urls)} total URLs.")
        print(f"💾 Saving all top-level domains to {output_file}...")
        print(f"💾 Newly discovered domains were saved in real-time to {self.discovered_file}")
        print(f"💾 Scraped domains were tracked in real-time in {self.scraped_file}")
        
        sorted_domains = sorted(list(self.found_domains))
        
        with open(output_file, 'w') as f:
            f.write(f"# Top-level .gov.my domains found by crawler\n")
            f.write(f"# Total domains: {len(sorted_domains)}\n")
            f.write(f"# Seed domains: {len(self.seed_domains)}\n")
            f.write(f"# Newly discovered during crawling: {len(self.discovered_domains)}\n")
            f.write(f"# Domains actually scraped: {len(self.scraped_domains)}\n")
            f.write(f"# Crawled URLs: {len(self.visited_urls)}\n")
            f.write(f"# Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for domain in sorted_domains:
                f.write(domain + '\n')
        
        # Update the discovered domains file with final stats
        with open(self.discovered_file, 'a') as f:
            f.write(f"\n# Crawling completed on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total newly discovered domains: {len(self.discovered_domains)}\n")
            f.write(f"# Seed domains from input file: {len(self.seed_domains)}\n")
        
        # Update the scraped domains file with final stats
        with open(self.scraped_file, 'a') as f:
            f.write(f"\n# Crawling completed on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total domains scraped: {len(self.scraped_domains)}\n")
            f.write(f"# Total URLs crawled: {len(self.visited_urls)}\n")
        
        print("✅ Done.")


if __name__ == "__main__":
    # The input file containing the master list of domains
    INPUT_FILE = "domains.txt"
    
    # Set a limit for how many pages to crawl.
    # Increase this for a more comprehensive crawl.
    PAGE_CRAWL_LIMIT = 99999
    
    # Number of concurrent threads (optimized for t2.micro: 1 vCPU, 1GB RAM)
    MAX_THREADS = 10
    
    crawler = GovMyCrawler(INPUT_FILE)
    try:
        crawler.crawl(crawl_limit=PAGE_CRAWL_LIMIT, max_workers=MAX_THREADS)
    except KeyboardInterrupt:
        print("\n\n🛑 User interrupted crawl. Saving results...")
        crawler.save_results()
    finally:
        crawler.cleanup()
        print("🧹 Cleanup completed.")
