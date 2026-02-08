from datetime import datetime
import re
from urllib.parse import urlparse, urldefrag, urljoin
from bs4 import BeautifulSoup
from utils.tokenizer import tokenize

INVALID_SUBDOMAINS = {"doku.php"}
INVALID_QUERIES = {"ical", "share"}
ALLOWED_DOMAINS = ["ics", "cs", "informatics", "stat"]

def scraper(url, resp):
    return extract_next_links(url, resp)

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.error:
        print(f"Error in response: {resp.error}")
        return list(), dict()
    content_type = resp.raw_response.headers.get("Content-Type")
    if not content_type or 'text/html' not in content_type:
        return list(), dict()
    soup = BeautifulSoup(resp.raw_response.content, 'lxml')
    tokens = tokenize(soup.get_text())
    a_tags = soup.find_all('a')
    links = []
    for tag in a_tags:
        link = ""
        href = str(tag.get('href'))
        if href:
            link = urljoin(url, href)
        link, fragment = urldefrag(link)
        if is_valid(link):
            links.append(link)
    return links, tokens

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    # Added check for urls to be within ics, cs, informatics, or stats subdomain    
    try:
        date_re = r"\d{4}-\d{2}(?:-\d{2})?"
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        for path in INVALID_SUBDOMAINS:
            for sub in parsed.path.lower().split('/'):
                if path in sub:
                    return False
        
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()) and \
                re.match(r"(.*\.)?(ics|cs|informatics|stat)\.uci\.edu.*", parsed.netloc) and \
                not any(re.search(date_re, query) or query in INVALID_QUERIES for query in re.split(r'[&=]', parsed.query)) and \
                not re.search(date_re, parsed.path)
    
    except TypeError:
        print ("TypeError for ", parsed)
        raise
    except Exception:
        return False

if __name__ == '__main__':
    import code
    code.interact(local=globals())
