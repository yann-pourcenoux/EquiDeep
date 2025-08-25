"""Tests for the HTTP utilities module."""

import os
import time
from unittest.mock import Mock, patch, MagicMock
from urllib.robotparser import RobotFileParser

import pytest
import requests
from requests.exceptions import HTTPError, ConnectTimeout

from avanza_cli.http import (
    build_session,
    get_html,
    check_robots_allowed,
    DEFAULT_TIMEOUT,
    DEFAULT_HEADERS,
    _polite_sleep,
)


class TestBuildSession:
    """Tests for build_session function."""

    def test_build_session_returns_session(self):
        """Test that build_session returns a requests.Session."""
        session = build_session()
        assert isinstance(session, requests.Session)

    def test_build_session_sets_headers(self):
        """Test that build_session sets the expected headers."""
        session = build_session()
        
        # Check that all default headers are present
        for key, expected_value in DEFAULT_HEADERS.items():
            assert session.headers[key] == expected_value

    def test_build_session_headers_content(self):
        """Test specific header values are correct."""
        session = build_session()
        
        assert "Mozilla/5.0" in session.headers["User-Agent"]
        assert "Chrome" in session.headers["User-Agent"]
        assert session.headers["Accept-Language"] == "sv,en;q=0.9"
        assert session.headers["Accept"] == "text/html"


class TestPoliteDelay:
    """Tests for _polite_sleep function."""

    @patch("avanza_cli.http.time.sleep")
    @patch("avanza_cli.http.random.uniform")
    def test_polite_sleep_uses_correct_range(self, mock_uniform, mock_sleep):
        """Test that polite sleep uses the specified delay range."""
        mock_uniform.return_value = 1.0
        
        result = _polite_sleep(0.5, 1.5)
        
        mock_uniform.assert_called_once_with(0.5, 1.5)
        mock_sleep.assert_called_once_with(1.0)
        assert result == 1.0

    @patch("avanza_cli.http.time.sleep")
    @patch("avanza_cli.http.random.uniform")
    def test_polite_sleep_default_range(self, mock_uniform, mock_sleep):
        """Test that polite sleep uses default range when no args provided."""
        mock_uniform.return_value = 0.8
        
        result = _polite_sleep()
        
        mock_uniform.assert_called_once_with(0.5, 1.5)
        mock_sleep.assert_called_once_with(0.8)
        assert result == 0.8

    @patch("avanza_cli.http.time.sleep")
    def test_polite_sleep_disabled_by_env(self, mock_sleep):
        """Test that polite sleep can be disabled via environment variable."""
        with patch.dict(os.environ, {"HTTP_DISABLE_DELAY": "1"}):
            result = _polite_sleep()
            
            mock_sleep.assert_not_called()
            assert result == 0.0

    @patch("avanza_cli.http.time.sleep")
    def test_polite_sleep_not_disabled_by_other_env_values(self, mock_sleep):
        """Test that only exact value '1' disables the delay."""
        with patch.dict(os.environ, {"HTTP_DISABLE_DELAY": "0"}):
            with patch("avanza_cli.http.random.uniform", return_value=0.7):
                result = _polite_sleep()
                
                mock_sleep.assert_called_once_with(0.7)
                assert result == 0.7


class TestGetHtml:
    """Tests for get_html function."""

    @patch("avanza_cli.http._polite_sleep")
    @patch("avanza_cli.http.check_robots_allowed")
    def test_get_html_success(self, mock_robots, mock_sleep):
        """Test successful HTML retrieval."""
        mock_robots.return_value = True
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "<html>Test content</html>"
        mock_session.get.return_value = mock_response
        
        result = get_html("https://example.com", mock_session)
        
        assert result == "<html>Test content</html>"
        mock_session.get.assert_called_once_with("https://example.com", timeout=DEFAULT_TIMEOUT)
        mock_response.raise_for_status.assert_called_once()
        mock_sleep.assert_called_once()

    @patch("avanza_cli.http._polite_sleep")
    @patch("avanza_cli.http.check_robots_allowed")
    def test_get_html_custom_timeout(self, mock_robots, mock_sleep):
        """Test that custom timeout is passed through."""
        mock_robots.return_value = True
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "content"
        mock_session.get.return_value = mock_response
        custom_timeout = (10.0, 30.0)
        
        get_html("https://example.com", mock_session, timeout=custom_timeout)
        
        mock_session.get.assert_called_once_with("https://example.com", timeout=custom_timeout)

    @patch("avanza_cli.http._polite_sleep")
    @patch("avanza_cli.http.check_robots_allowed")
    def test_get_html_retries_on_500_error(self, mock_robots, mock_sleep):
        """Test that get_html retries on 500 errors."""
        mock_robots.return_value = True
        mock_session = Mock()
        
        # First call raises 500 error, second succeeds
        error_response = Mock()
        error_response.status_code = 500
        http_error = HTTPError(response=error_response)
        
        success_response = Mock()
        success_response.text = "success"
        
        mock_session.get.side_effect = [http_error, success_response]
        
        # Mock raise_for_status to raise on first call only
        def mock_raise_for_status():
            if mock_session.get.call_count == 1:
                raise http_error
        
        error_response.raise_for_status = mock_raise_for_status
        success_response.raise_for_status = Mock()
        
        result = get_html("https://example.com", mock_session)
        
        assert result == "success"
        assert mock_session.get.call_count == 2
        assert mock_sleep.call_count == 2  # Called before each attempt

    @patch("avanza_cli.http._polite_sleep")
    @patch("avanza_cli.http.check_robots_allowed")
    def test_get_html_no_retry_on_timeout(self, mock_robots, mock_sleep):
        """Test that connection timeouts are not retried."""
        mock_robots.return_value = True
        mock_session = Mock()
        mock_session.get.side_effect = ConnectTimeout("Connection timed out")
        
        with pytest.raises(ConnectTimeout):
            get_html("https://example.com", mock_session)
        
        # Should only be called once (no retry)
        mock_session.get.assert_called_once()
        mock_sleep.assert_called_once()

    @patch("avanza_cli.http._polite_sleep")
    @patch("avanza_cli.http.check_robots_allowed")
    def test_get_html_no_retry_on_404(self, mock_robots, mock_sleep):
        """Test that 404 errors are not retried."""
        mock_robots.return_value = True
        mock_session = Mock()
        
        error_response = Mock()
        error_response.status_code = 404
        http_error = HTTPError(response=error_response)
        
        mock_session.get.return_value.raise_for_status.side_effect = http_error
        
        with pytest.raises(HTTPError):
            get_html("https://example.com", mock_session)
        
        # Should only be called once (no retry for 404)
        mock_session.get.assert_called_once()
        mock_sleep.assert_called_once()


class TestRobotsAwareness:
    """Tests for robots.txt functionality."""

    @patch("avanza_cli.http._polite_sleep")
    def test_robots_enforcement_raises_permission_error(self, mock_sleep):
        """Test that robots.txt enforcement raises PermissionError when disallowed."""
        mock_session = Mock()
        
        with patch("avanza_cli.http.check_robots_allowed", return_value=False):
            with pytest.raises(PermissionError, match="robots.txt disallows fetching this URL"):
                get_html("https://example.com", mock_session, respect_robots=True)
        
        # Should not make the actual HTTP request
        mock_session.get.assert_not_called()
        mock_sleep.assert_not_called()

    @patch("avanza_cli.http._polite_sleep")
    @patch("avanza_cli.http.check_robots_allowed")
    def test_robots_logging_when_not_enforced(self, mock_robots, mock_sleep, caplog):
        """Test that robots.txt violations are logged when not enforced."""
        import logging
        
        # Set log level to capture INFO messages
        caplog.set_level(logging.INFO, logger="avanza_cli.http")
        
        mock_robots.return_value = False
        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = "content"
        mock_session.get.return_value = mock_response
        
        get_html("https://example.com", mock_session, respect_robots=False)
        
        # Should proceed with request despite robots.txt
        mock_session.get.assert_called_once()
        
        # Should log a warning about TOS
        assert "robots.txt disallows" in caplog.text
        assert "respect site TOS" in caplog.text

    def test_check_robots_allowed_with_disallow_rule(self):
        """Test robots.txt parsing with disallow rule."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nDisallow: /private/"
        mock_session.get.return_value = mock_response
        
        # Test disallowed path
        result = check_robots_allowed("https://example.com/private/page", mock_session)
        assert result is False
        
        # Test allowed path
        result = check_robots_allowed("https://example.com/public/page", mock_session)
        assert result is True

    def test_check_robots_allowed_fail_open_on_error(self):
        """Test that robots.txt errors default to allowing access."""
        mock_session = Mock()
        mock_session.get.side_effect = ConnectTimeout("Failed to connect")
        
        result = check_robots_allowed("https://example.com/anything", mock_session)
        assert result is True

    def test_check_robots_allowed_fail_open_on_404(self):
        """Test that missing robots.txt defaults to allowing access."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response
        
        result = check_robots_allowed("https://example.com/anything", mock_session)
        assert result is True

    def test_check_robots_allowed_caching(self):
        """Test that robots.txt responses are cached per netloc."""
        # Clear the cache first to ensure clean test
        from avanza_cli.http import _ROBOTS_CACHE
        _ROBOTS_CACHE.clear()
        
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "User-agent: *\nDisallow: /private/"
        mock_session.get.return_value = mock_response
        
        # First call
        check_robots_allowed("https://example.com/page1", mock_session)
        
        # Second call to same domain
        check_robots_allowed("https://example.com/page2", mock_session)
        
        # Should only fetch robots.txt once
        mock_session.get.assert_called_once()

    def test_check_robots_allowed_invalid_url(self):
        """Test handling of invalid URLs."""
        mock_session = Mock()
        
        result = check_robots_allowed("not-a-url", mock_session)
        assert result is True  # Fail open for invalid URLs
