import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import collections
import time
import os
import re
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
        # File to save discovered domains in real-time
        self.discovered_file = "discovered_domains_realtime.txt"
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

    def _initialize_discovered_file(self):
        """Initialize the real-time discovered domains file."""
        with open(self.discovered_file, 'w') as f:
            f.write(f"# Newly discovered .gov.my domains (real-time)\n")
            f.write(f"# Started crawling on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    def _save_discovered_domain(self, domain):
        """Append a newly discovered domain to the real-time file."""
        with open(self.discovered_file, 'a') as f:
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
                        # Add the top-level domain to our set of findings
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
                        
                        # If the URL is new and within scope, add it to our queue to crawl
                        if absolute_url not in self.visited_urls:
                            self.queue.append(absolute_url)
                            links_found += 1
            except Exception as e:
                print(f"⚠️  Could not parse URL {absolute_url}: {e}")
        
        return links_found

    def crawl(self, crawl_limit=500):
        """Starts the crawling process."""
        if not self._initialize_queue():
            return

        print(f"\n🚀 Starting recursive crawl...")
        crawled_count = 0

        while self.queue and crawled_count < crawl_limit:
            current_url = self.queue.popleft()

            if current_url in self.visited_urls:
                continue

            # Also mark the http/https counterpart as visited to avoid double-checking
            if current_url.startswith("http://"):
                https_version = "https://" + current_url[7:]
                self.visited_urls.add(https_version)
            elif current_url.startswith("https://"):
                http_version = "http://" + current_url[8:]
                self.visited_urls.add(http_version)
                
            self.visited_urls.add(current_url)
            crawled_count += 1
            
            print(f"🔎 Crawling ({crawled_count}/{crawl_limit}): {current_url}")

            try:
                response = self.session.get(current_url, timeout=15, allow_redirects=True)
                
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in content_type:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        links_found = self._extract_links(current_url, soup)
                        print(f"   -> Found {links_found} new links to crawl")
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
            
            # Be a good web citizen
            time.sleep(0.2)

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
        print(f"Crawled {len(self.visited_urls)} total URLs.")
        print(f"💾 Saving all top-level domains to {output_file}...")
        print(f"💾 Newly discovered domains were saved in real-time to {self.discovered_file}")
        
        sorted_domains = sorted(list(self.found_domains))
        
        with open(output_file, 'w') as f:
            f.write(f"# Top-level .gov.my domains found by crawler\n")
            f.write(f"# Total domains: {len(sorted_domains)}\n")
            f.write(f"# Seed domains: {len(self.seed_domains)}\n")
            f.write(f"# Newly discovered during crawling: {len(self.discovered_domains)}\n")
            f.write(f"# Crawled URLs: {len(self.visited_urls)}\n")
            f.write(f"# Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for domain in sorted_domains:
                f.write(domain + '\n')
        
        # Update the discovered domains file with final stats
        with open(self.discovered_file, 'a') as f:
            f.write(f"\n# Crawling completed on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total newly discovered domains: {len(self.discovered_domains)}\n")
            f.write(f"# Seed domains from input file: {len(self.seed_domains)}\n")
        
        print("✅ Done.")


if __name__ == "__main__":
    # The input file containing the master list of domains
    INPUT_FILE = "domains.txt"
    
    # Set a limit for how many pages to crawl.
    # Increase this for a more comprehensive crawl.
    PAGE_CRAWL_LIMIT = 99999
    
    crawler = GovMyCrawler(INPUT_FILE)
    try:
        crawler.crawl(crawl_limit=PAGE_CRAWL_LIMIT)
    except KeyboardInterrupt:
        print("\n\n🛑 User interrupted crawl. Saving results...")
        crawler.save_results()
    finally:
        crawler.cleanup()
        print("🧹 Cleanup completed.")
