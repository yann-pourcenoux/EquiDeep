"""HTTP utilities for Avanza CLI."""

import logging
import os
import random
import threading
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from requests import Session, exceptions as req_exc
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

# Module logger
logger = logging.getLogger(__name__)

# Default timeout for HTTP requests (connect, read)
DEFAULT_TIMEOUT: Tuple[float, float] = (5.0, 20.0)

# Default headers for polite, browser-like requests
DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "sv,en;q=0.9",
    "Accept": "text/html",
}

# HTTP status codes that should trigger a retry
STATUS_FOR_RETRY = {429, 500, 502, 503, 504}

# Cache for robots.txt parsers per netloc (thread-safe)
_ROBOTS_CACHE: Dict[str, RobotFileParser] = {}
_robots_lock = threading.Lock()


def build_session() -> requests.Session:
    """Build a configured requests.Session with polite, browser-like defaults.
    
    Returns:
        A requests.Session with headers set for polite scraping including:
        - Modern browser User-Agent
        - Swedish/English language preference
        - HTML content acceptance
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    logger.debug("Built session with default headers: %s", DEFAULT_HEADERS)
    return session


def _polite_sleep(min_delay: float = 0.5, max_delay: float = 1.5) -> float:
    """Sleep for a randomized delay to be polite to servers.
    
    Args:
        min_delay: Minimum delay in seconds (default: 0.5)
        max_delay: Maximum delay in seconds (default: 1.5)
        
    Returns:
        The actual delay used (useful for testing)
        
    Note:
        Can be disabled by setting HTTP_DISABLE_DELAY=1 environment variable.
    """
    # Allow disabling delay for power users/tests
    if os.getenv("HTTP_DISABLE_DELAY") == "1":
        logger.debug("Polite sleep disabled via HTTP_DISABLE_DELAY=1")
        return 0.0
    
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)
    logger.debug("Polite sleep: %0.2fs", delay)
    return delay


def _retry_on_http_error(exc: Exception) -> bool:
    """Predicate to determine if an exception should trigger a retry.
    
    Args:
        exc: The exception that was raised
        
    Returns:
        True if the exception is an HTTPError with a status code in STATUS_FOR_RETRY
    """
    if isinstance(exc, req_exc.HTTPError) and getattr(exc, "response", None) is not None:
        return exc.response.status_code in STATUS_FOR_RETRY
    return False


def _get_robot_parser(netloc: str) -> Optional[RobotFileParser]:
    """Get a cached RobotFileParser for the given netloc.
    
    Args:
        netloc: The network location (domain:port) for robots.txt
        
    Returns:
        A RobotFileParser instance or None if not cached
    """
    with _robots_lock:
        return _ROBOTS_CACHE.get(netloc)


def check_robots_allowed(
    url: str, 
    session: Session, 
    user_agent: Optional[str] = None,
    timeout: Tuple[float, float] = DEFAULT_TIMEOUT
) -> bool:
    """Check if the URL is allowed by robots.txt.
    
    Args:
        url: The URL to check
        session: The requests.Session to use for fetching robots.txt
        user_agent: User agent to check (defaults to session's User-Agent)
        timeout: Timeout for robots.txt fetch
        
    Returns:
        True if the URL is allowed or if robots.txt is not accessible (fail-open)
        False if robots.txt explicitly disallows the URL
    """
    parsed = urlparse(url)
    netloc = parsed.netloc
    
    if not netloc:
        logger.warning("Invalid URL for robots.txt check: %s", url)
        return True
    
    # Use session's User-Agent header or provided one
    ua = user_agent or DEFAULT_HEADERS["User-Agent"]
    
    # Check cache first
    rp = _get_robot_parser(netloc)
    if rp is None:
        # Create and cache a new parser
        rp = RobotFileParser()
        robots_url = f"{parsed.scheme}://{netloc}/robots.txt"
        
        try:
            logger.debug("Fetching robots.txt from %s", robots_url)
            resp = session.get(robots_url, timeout=timeout)
            
            if resp.status_code == 200:
                # Parse the robots.txt content
                rp.set_url(robots_url)
                rp.parse(resp.text.splitlines())
                logger.debug("Successfully parsed robots.txt for %s", netloc)
            else:
                logger.info("robots.txt returned %d for %s, assuming allowed", resp.status_code, netloc)
                # Create an empty parser that allows everything
                rp.set_url(robots_url)
                rp.parse([])
                
        except Exception as e:
            logger.info("Failed to fetch robots.txt for %s (%s), assuming allowed", netloc, e)
            # Create an empty parser that allows everything on error (fail-open)
            rp.set_url(robots_url)
            rp.parse([])
        
        # Cache the parser
        with _robots_lock:
            _ROBOTS_CACHE[netloc] = rp
    
    # Check if the URL is allowed
    allowed = rp.can_fetch(ua, url)
    
    if not allowed:
        logger.info("robots.txt disallows %s for user-agent '%s'", url, ua)
    else:
        logger.debug("robots.txt allows %s for user-agent '%s'", url, ua)
    
    return allowed


# Configure retry policy for get_html
_retry_policy = {
    "reraise": True,
    "stop": stop_after_attempt(5),
    "wait": wait_exponential(multiplier=0.5, max=8) + wait_random(0, 0.5),
    "retry": retry_if_exception(_retry_on_http_error),
}


@retry(**_retry_policy)
def get_html(
    url: str, 
    session: Session, 
    timeout: Tuple[float, float] = DEFAULT_TIMEOUT,
    respect_robots: bool = False
) -> str:
    """GET the URL and return HTML text. Retries on specific HTTP status codes.
    
    Args:
        url: The URL to fetch
        session: The requests.Session to use for the request
        timeout: Timeout tuple (connect, read) in seconds
        respect_robots: Whether to check robots.txt (placeholder for subtask 3.4)
        
    Returns:
        The HTML text content from the response
        
    Raises:
        HTTPError: On non-retriable HTTP errors or after max retries
        RequestException: On connection/timeout errors (no retry)
        
    Note:
        - Retries only on HTTP status codes: {429, 500, 502, 503, 504}
        - Uses exponential backoff with jitter: start=0.5s, max=8s
        - Includes polite delay before each attempt
        - Other exceptions (timeouts, connection errors) propagate immediately
    """
    # Check robots.txt if requested
    if respect_robots:
        if not check_robots_allowed(url, session, timeout=timeout):
            raise PermissionError(f"robots.txt disallows fetching this URL: {url}")
    else:
        # Still check and log for awareness, but don't enforce
        if not check_robots_allowed(url, session, timeout=timeout):
            logger.info("Note: robots.txt disallows %s but respect_robots=False, proceeding anyway. Please respect site TOS.", url)
    
    _polite_sleep()  # polite delay before each attempt
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text
