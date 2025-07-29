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
        # A stack (list) for URLs to visit - LIFO for better efficiency
        self.stack = []
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
        # A set to store all subdomains found (including top-level domains)
        self.all_subdomains = set()
        # A set to store scraped subdomains to prevent re-scraping
        self.scraped_subdomains = set()
        # File to save discovered domains in real-time
        self.discovered_file = "discovered_domains_realtime.txt"
        # File to track scraped websites in real-time
        self.scraped_file = "websites_scraped.txt"
        # Thread locks for thread-safe operations
        self.stack_lock = threading.Lock()
        self.visited_lock = threading.Lock()
        self.found_domains_lock = threading.Lock()
        self.scraped_domains_lock = threading.Lock()
        self.subdomains_lock = threading.Lock()
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
            f.write(f"# Newly discovered .gov.my domains and subdomains (real-time)\n")
            f.write(f"# Includes domains from links and email addresses\n")
            f.write(f"# Started crawling on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    def _initialize_scraped_file(self):
        """Initialize the real-time scraped websites file."""
        with open(self.scraped_file, 'w') as f:
            f.write(f"# Scraped .gov.my domains and subdomains (real-time)\n")
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

    def _should_skip_domain(self, domain):
        """Check if a domain should be skipped (e.g., www subdomains)."""
        if domain.startswith('www.'):
            return True
        return False

    def _initialize_queue(self):
        """Reads the input file and populates the initial crawl stack."""
        if not os.path.exists(self.input_file):
            print(f"❌ Error: Input file '{self.input_file}' not found.")
            print("Please create this file and populate it with your list of domains.")
            return False
            
        print(f"📖 Reading seed domains from '{self.input_file}'...")
        with open(self.input_file, 'r') as f:
            for line in f:
                domain = line.strip().lower()  # Convert to lowercase
                if domain:
                    # Extract top-level domain to avoid duplicates from subdomains
                    top_level = self._extract_top_level_domain(domain)
                    if top_level:
                        # Add to seed domains set
                        self.seed_domains.add(top_level)
                        # Add both http and https versions to be safe
                        self.stack.append(f"http://{domain}")
                        self.stack.append(f"https://{domain}")
                        self.found_domains.add(top_level) # Add the top-level domain
        
        if not self.stack:
            print("⚠️  Warning: No domains found in the input file.")
            return False
            
        print(f"✅ Initialized stack with {len(self.stack)} URLs from {len(self.seed_domains)} seed domains.")
        return True

    def _extract_top_level_domain(self, domain):
        """
        Extracts the top-level domain from a potentially subdomain.
        Example: subdomain.example.gov.my -> example.gov.my
        """
        if not domain:
            return None
        
        # Convert to lowercase for consistency
        domain = domain.lower()
        
        if not domain.endswith('.gov.my'):
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
        """Extracts top-level domains and subdomains from all links and emails on a page and adds them to the queue."""
        links_found = 0
        
        # Extract domains from regular links
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Create an absolute URL from a relative one (e.g., /contact-us)
            absolute_url = urljoin(current_url, href)
            
            try:
                parsed_url = urlparse(absolute_url)
                domain = parsed_url.netloc.lower()  # Convert to lowercase

                # We are only interested in .gov.my domains
                if domain and domain.endswith('.gov.my') and not self._should_skip_domain(domain):
                    links_found += self._process_domain(domain, absolute_url)
            except Exception as e:
                print(f"⚠️  Could not parse URL {absolute_url}: {e}")
        
        # Extract domains from email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b'
        page_text = soup.get_text()
        email_matches = re.findall(email_pattern, page_text)
        
        for email_domain in email_matches:
            email_domain = email_domain.lower()  # Convert to lowercase
            if email_domain.endswith('.gov.my') and not self._should_skip_domain(email_domain):
                # Create a dummy URL for the email domain to add to queue
                dummy_url = f"https://{email_domain}"
                links_found += self._process_domain(email_domain, dummy_url)
                print(f"📧 Found email domain: {email_domain}")
        
        return links_found

    def _process_domain(self, domain, url):
        """Process a discovered domain and add it to appropriate sets and queue."""
        links_added = 0
        
        # Extract top-level domain (remove subdomains)
        top_level_domain = self._extract_top_level_domain(domain)
        
        if top_level_domain:
            # Handle top-level domain discovery (thread-safe)
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
            
            # Handle subdomain discovery (thread-safe)
            with self.subdomains_lock:
                if domain not in self.all_subdomains:
                    self.all_subdomains.add(domain)
                    # Check if this is a new subdomain discovery (only log, don't save to discovered file)
                    if domain != top_level_domain:
                        print(f"🔍 Found new subdomain: {domain}")
            
            # Only add URL to stack if the subdomain hasn't been scraped yet and URL is new (thread-safe)
            with self.visited_lock:
                with self.subdomains_lock:
                    if (url not in self.visited_urls and 
                        domain not in self.scraped_subdomains):
                        with self.stack_lock:
                            self.stack.append(url)  # LIFO - add to end, pop from end
                            links_added = 1
        
        return links_added

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
                current_domain = parsed_url.netloc.lower()  # Convert to lowercase
                current_top_level_domain = self._extract_top_level_domain(current_domain)
                
                # Skip if this is a www domain or already scraped subdomain (thread-safe)
                if self._should_skip_domain(current_domain):
                    print(f"⏭️  Skipping {current_url} - www subdomain filtered out")
                    return None
                    
                with self.subdomains_lock:
                    if current_domain and current_domain in self.scraped_subdomains:
                        print(f"⏭️  Skipping {current_url} - subdomain {current_domain} already scraped")
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
                        # Check if this is a top-level domain or subdomain
                        is_top_level = (current_domain == current_top_level_domain)
                        
                        # Mark domain as scraped (thread-safe)
                        with self.subdomains_lock:
                            if current_domain and current_domain not in self.scraped_subdomains:
                                self.scraped_subdomains.add(current_domain)
                                self._save_scraped_domain(current_domain)
                                
                                if is_top_level:
                                    print(f"   ✅ Marked top-level domain {current_domain} as scraped")
                                else:
                                    print(f"   ✅ Marked subdomain {current_domain} as scraped")
                        
                        # Also add to top-level domains set if it's a top-level domain
                        if is_top_level:
                            with self.scraped_domains_lock:
                                if current_top_level_domain not in self.scraped_domains:
                                    self.scraped_domains.add(current_top_level_domain)
                        
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
        """Starts the crawling process with threading using a stack (LIFO)."""
        if not self._initialize_queue():
            return

        print(f"\n🚀 Starting recursive crawl with {max_workers} threads using stack (depth-first)...")
        crawled_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while crawled_count < crawl_limit:
                # Get URLs to process in this batch
                urls_to_process = []
                with self.stack_lock:
                    batch_size = min(max_workers, crawl_limit - crawled_count, len(self.stack))
                    if batch_size == 0:
                        break
                    
                    for _ in range(batch_size):
                        if self.stack:
                            urls_to_process.append(self.stack.pop())  # LIFO - pop from end
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
        """Saves the found top-level domains and subdomains to text files."""
        output_file = "gov_my_top_level_domains.txt"
        subdomains_file = "gov_my_all_subdomains.txt"
        
        print(f"\nFound {len(self.found_domains)} unique top-level .gov.my domains.")
        print(f"Found {len(self.all_subdomains)} total subdomains (including top-level).")
        print(f"Seed domains from input file: {len(self.seed_domains)}")
        print(f"Newly discovered domains during crawling: {len(self.discovered_domains)}")
        print(f"Top-level domains actually scraped: {len(self.scraped_domains)}")
        print(f"Subdomains actually scraped: {len(self.scraped_subdomains)}")
        print(f"Crawled {len(self.visited_urls)} total URLs.")
        print(f"💾 Saving all top-level domains to {output_file}...")
        print(f"💾 Saving all subdomains to {subdomains_file}...")
        print(f"💾 Newly discovered domains were saved in real-time to {self.discovered_file}")
        print(f"💾 Scraped domains were tracked in real-time in {self.scraped_file}")
        
        sorted_domains = sorted(list(self.found_domains))
        sorted_subdomains = sorted(list(self.all_subdomains))
        
        # Save top-level domains
        with open(output_file, 'w') as f:
            f.write(f"# Top-level .gov.my domains found by crawler\n")
            f.write(f"# Total top-level domains: {len(sorted_domains)}\n")
            f.write(f"# Total subdomains found: {len(sorted_subdomains)}\n")
            f.write(f"# Seed domains: {len(self.seed_domains)}\n")
            f.write(f"# Newly discovered during crawling: {len(self.discovered_domains)}\n")
            f.write(f"# Top-level domains actually scraped: {len(self.scraped_domains)}\n")
            f.write(f"# Subdomains actually scraped: {len(self.scraped_subdomains)}\n")
            f.write(f"# Crawled URLs: {len(self.visited_urls)}\n")
            f.write(f"# Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for domain in sorted_domains:
                f.write(domain + '\n')
        
        # Save all subdomains (including top-level domains)
        with open(subdomains_file, 'w') as f:
            f.write(f"# All .gov.my domains and subdomains found by crawler\n")
            f.write(f"# Total entries: {len(sorted_subdomains)}\n")
            f.write(f"# Top-level domains: {len(sorted_domains)}\n")
            f.write(f"# Subdomains scraped: {len(self.scraped_subdomains)}\n")
            f.write(f"# Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for subdomain in sorted_subdomains:
                # Mark if it's a top-level domain or subdomain
                top_level = self._extract_top_level_domain(subdomain)
                if subdomain == top_level:
                    f.write(f"{subdomain} (top-level)\n")
                else:
                    f.write(f"{subdomain} (subdomain of {top_level})\n")
        
        # Update the discovered domains file with final stats
        with open(self.discovered_file, 'a') as f:
            f.write(f"\n# Crawling completed on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total newly discovered top-level domains: {len(self.discovered_domains)}\n")
            f.write(f"# Total subdomains found: {len(self.all_subdomains)}\n")
            f.write(f"# Seed domains from input file: {len(self.seed_domains)}\n")
        
        # Update the scraped domains file with final stats
        with open(self.scraped_file, 'a') as f:
            f.write(f"\n# Crawling completed on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total top-level domains scraped: {len(self.scraped_domains)}\n")
            f.write(f"# Total subdomains scraped: {len(self.scraped_subdomains)}\n")
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
