"""Link harvesting functionality for Avanza CLI."""

from __future__ import annotations

import logging
import re
from typing import List
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .http import get_html

# Module logger
logger = logging.getLogger(__name__)

# Module-level constants
SEED_URL = "https://www.avanza.se/aktier/hitta.html?s=numberOfOwners.desc&o=20000"
AVANZA_HOST = "www.avanza.se"
PATH_SUBSTR = "/aktier/om-aktien/"
RELATIVE_PATH_RE = re.compile(r"^/aktier/om-aktien/[^\s]+")


def harvest_links(session) -> list[str]:
    """Fetch the Avanza seed page and collect all stock detail links on that page only, deduped and absolute.
    
    Args:
        session: The requests.Session to use for fetching the seed page
        
    Returns:
        List of absolute URLs to stock detail pages on Avanza. Returns empty list on errors.
        
    Note:
        Only processes the initially loaded page without pagination (MVP approach).
        Links are filtered to only include https://www.avanza.se/aktier/om-aktien/ URLs.
    """
    # Validate session argument
    if session is None:
        logger.warning("No session provided to harvest_links")
        return []
    
    # Fetch HTML from seed URL
    try:
        html = get_html(SEED_URL, session)
    except Exception as e:
        logger.warning("Failed to fetch seed page %s: %s", SEED_URL, e)
        return []
    
    # Basic guardrails for HTML content
    if not html or len(html.strip()) < 100:
        logger.warning("Received empty or very short HTML content (%d chars)", len(html) if html else 0)
        return []
    
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html, 'lxml')
    
    # Collect candidate hrefs from all <a> tags
    raw_hrefs = []
    for a in soup.find_all('a'):
        href = (a.get('href') or '').strip()
        if not href:
            continue
        
        # Check if href matches our patterns for stock detail links
        if (PATH_SUBSTR in href) or RELATIVE_PATH_RE.match(href):
            raw_hrefs.append(href)
    
    # Normalize to absolute URLs and filter/deduplicate
    cleaned_urls = []
    for href in raw_hrefs:
        # Convert to absolute URL
        abs_url = urljoin(SEED_URL, href)
        
        # Parse URL components
        parts = urlsplit(abs_url)
        
        # Enforce domain and scheme constraints
        if parts.scheme != 'https' or parts.netloc != AVANZA_HOST:
            continue
        
        # Strip fragments to avoid duplicating same page with anchors
        cleaned = urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ''))
        
        # Defensive check: ensure PATH_SUBSTR is still in the path after join
        if PATH_SUBSTR not in parts.path:
            continue
        
        cleaned_urls.append(cleaned)
    
    # Deduplicate preserving original order
    seen = set()
    results = []
    for url in cleaned_urls:
        if url not in seen:
            seen.add(url)
            results.append(url)
    
    # Log summary before returning
    logger.info("Harvested %d stock links from seed", len(results))
    if results:
        logger.debug("Sample links: %s", results[:5])
    
    return results
