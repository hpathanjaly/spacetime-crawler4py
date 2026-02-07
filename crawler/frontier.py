from collections import Counter, defaultdict
import os
import shelve

from threading import RLock, Lock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

SUB_COUNT = "subdomain_count"
TOKENS = "tokens"
EXACT_HASHES_KEY = "similarity_exact_hashes"
SIMHASH_LIST_KEY = "similarity_simhash_list"

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.tbd_lock = Lock()
        self.save_lock = Lock()
        
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
            self.save[SUB_COUNT] = defaultdict(int)
            self.save[TOKENS] = Counter()
            self.save[EXACT_HASHES_KEY] = []
            self.save[SIMHASH_LIST_KEY] = []
            self.save.sync()
            for url in self.config.seed_urls:
                self.add_url(url)
        else:   
            self._parse_save_file()
            # Cold start or old shelve: ensure report keys exist.
            if SUB_COUNT not in self.save:
                self.save[SUB_COUNT] = defaultdict(int)
            if TOKENS not in self.save:
                self.save[TOKENS] = Counter()
            if EXACT_HASHES_KEY not in self.save:
                self.save[EXACT_HASHES_KEY] = []
            if SIMHASH_LIST_KEY not in self.save:
                self.save[SIMHASH_LIST_KEY] = []
            self.save.sync()
            # No URLs to crawl: start from seed.
            if not self.to_be_downloaded:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        skip_keys = {SUB_COUNT, TOKENS, EXACT_HASHES_KEY, SIMHASH_LIST_KEY}
        total_count = 0
        tbd_count = 0
        for key in self.save:
            if key in skip_keys:
                continue
            val = self.save[key]
            try:
                url, completed = val
            except (TypeError, ValueError):
                continue
            total_count += 1
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
        with self.save_lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                with self.tbd_lock:
                    self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.save_lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            
            self.save[urlhash] = (url, True)
            self.save.sync()
    
    def add_subdomain_count(self, sub):
        with self.save_lock:
            self.save[SUB_COUNT][sub] = self.save[SUB_COUNT].get(sub, 0) + 1
            self.save[SUB_COUNT] = dict(self.save[SUB_COUNT])
            self.save.sync()
    
    def get_subdomain_count(self):
        with self.save_lock:
            return self.save[SUB_COUNT]

    def add_tokens(self, tokens):
        with self.save_lock:
            current = self.save.get(TOKENS, Counter())
            self.save[TOKENS] = current + Counter(tokens)
            self.save.sync()
    
    def get_tokens(self):
        with self.save_lock:
            return self.save[TOKENS]

    def is_duplicate_page(self, tokens):
        try:
            import similarity
        except ImportError:
            return False
        if similarity.check_duplicate(tokens):
            return True
        with self.save_lock:
            digests, fingerprints = similarity.get_state_for_save()
            self.save[EXACT_HASHES_KEY] = digests
            self.save[SIMHASH_LIST_KEY] = fingerprints
            self.save.sync()
        return False

    def print_data(self):
        subdomain_counts: dict = self.get_subdomain_count()
        tokens: Counter = self.get_tokens()
        print(f"Total unique pages = {sum(subdomain_counts.values())}")
        print("50 most common words:")
        print(tokens.most_common(50))
        print("Subdomains:")
        for subdomain, count in sorted(subdomain_counts.items(), key=lambda item: item[0]):
            print(f"{subdomain}, {count}")