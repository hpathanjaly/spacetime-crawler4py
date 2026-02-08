from collections import Counter, defaultdict
import os
import shelve

from threading import RLock, Lock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

SUB_COUNT = "subdomain_count"
TOKENS = "tokens"
TBD = "tbd"
LONGEST_PAGE_KEY = "longest_page"
EXACT_HASHES_KEY = "similarity_exact_hashes"
SIMHASH_LIST_KEY = "similarity_simhash_list"

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.domain_locks = dict()
        self.tbd_lock = Lock()
        self.count_lock = Lock()
        self.token_lock = Lock()
        self.longest_lock = Lock()
        self.locks_lock = Lock()
        self.simhash_lock = Lock()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        try:
            import similarity
            digests = self.save.get(EXACT_HASHES_KEY)
            fingerprints = self.save.get(SIMHASH_LIST_KEY)
            similarity.restore_state(digests, fingerprints)
        except ImportError:
            pass
        
        if restart:
            self.save[SUB_COUNT] = {}
            self.save[TOKENS] = Counter()
            self.save[TBD] = dict()
            self.save[LONGEST_PAGE_KEY] = (0, "")
            self.save[EXACT_HASHES_KEY] = []
            self.save[SIMHASH_LIST_KEY] = []
            self.save.sync()
            for url in self.config.seed_urls:
                self.add_url(url)
        else:   
            self._parse_save_file()
            if SUB_COUNT not in self.save:
                self.save[SUB_COUNT] = {}
            if TOKENS not in self.save:
                self.save[TOKENS] = Counter()
            if LONGEST_PAGE_KEY not in self.save:
                self.save[LONGEST_PAGE_KEY] = (0, "")
            if EXACT_HASHES_KEY not in self.save:
                self.save[EXACT_HASHES_KEY] = []
            if SIMHASH_LIST_KEY not in self.save:
                self.save[SIMHASH_LIST_KEY] = []
            self.save.sync()
            if not self.save[TBD]:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save[TBD])
        tbd_count = 0
        for url, completed in self.save[TBD].values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            with self.tbd_lock:
                return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.tbd_lock:
            if urlhash not in self.save[TBD]:
                tbd = self.save[TBD]
                tbd[urlhash] = (url, False)
                self.save[TBD] = tbd
                self.save.sync()
                self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.tbd_lock:
            if urlhash not in self.save[TBD]:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            tbd = self.save[TBD]
            tbd[urlhash] = (url, True)
            self.save[TBD] = tbd
            self.save.sync()
    
    def add_subdomain_count(self, domain):
        with self.count_lock:
            counts = self.save[SUB_COUNT]
            counts[domain] = counts.get(domain, 0) + 1
            self.save[SUB_COUNT] = counts
            self.save.sync()
    
    def get_subdomain_count(self):
        with self.count_lock:
            return self.save[SUB_COUNT]

    def add_tokens(self, tokens):
        with self.token_lock:
            self.save[TOKENS] += Counter(tokens)
            self.save.sync()
    
    def get_tokens(self):
        with self.token_lock:
            return self.save[TOKENS]

    def update_longest_page(self, url, tokens):
        """Update longest page if this page has more words. tokens is word -> count dict."""
        word_count = sum(tokens.values()) if tokens else 0
        if word_count <= 0:
            return
        with self.longest_lock:
            current = self.save.get(LONGEST_PAGE_KEY, (0, ""))
            if word_count > current[0]:
                self.save[LONGEST_PAGE_KEY] = (word_count, url)
                self.save.sync()

    def is_duplicate_page(self, tokens):
        try:
            import similarity
        except ImportError:
            return False
        if similarity.check_duplicate(tokens):
            return True
        with self.simhash_lock:
            digests, fingerprints = similarity.get_state_for_save()
            self.save[EXACT_HASHES_KEY] = digests
            self.save[SIMHASH_LIST_KEY] = fingerprints
            self.save.sync()
        return False

    def print_data(self):
        subdomain_counts: dict = self.get_subdomain_count()
        tokens: Counter = self.get_tokens()
        longest = self.save.get(LONGEST_PAGE_KEY, (0, ""))
        print(f"Total unique pages = {sum(subdomain_counts.values())}")
        print(f"Longest page: {longest[1]} ({longest[0]} words)")
        print("50 most common words:")
        print(tokens.most_common(50))
        print("Subdomains:")
        for subdomain, count in sorted(subdomain_counts.items(), key=lambda item: item[0]):
            print(f"{subdomain}, {count}")
    
    def get_domain_lock(self, domain):
        with self.locks_lock:
            if domain not in self.domain_locks:
                self.domain_locks[domain] = Lock()
            return self.domain_locks[domain]