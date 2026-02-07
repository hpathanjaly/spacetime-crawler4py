from collections import Counter, defaultdict
import os
import shelve

from threading import RLock, Lock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

SUB_COUNT = "subdomain_count"
TOKENS = "tokens"

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
        if restart:
            self.save[SUB_COUNT] = defaultdict(int)
            self.save[TOKENS] = Counter()
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
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
            self.save[SUB_COUNT][sub] += 1
            self.save.sync()
    
    def get_subdomain_count(self):
        with self.save_lock:
            return self.save[SUB_COUNT]

    def add_tokens(self, tokens):
        with self.save_lock:
            self.save[TOKENS] += Counter(tokens)
            self.save.sync()
    
    def get_tokens(self):
        with self.save_lock:
            return self.save[TOKENS]

    def print_data(self):
        subdomain_counts: dict = self.get_subdomain_count()
        tokens: Counter = self.get_tokens()
        print(f"Total unique pages = {sum(subdomain_counts.values())}")
        print("50 most common words:")
        print(tokens.most_common(50))
        print("Subdomains:")
        for subdomain, count in subdomain_counts.items():
            print(f"{subdomain}, {count}")