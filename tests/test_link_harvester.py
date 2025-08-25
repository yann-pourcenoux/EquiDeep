"""Tests for link_harvester module."""

import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

from src.avanza_cli.link_harvester import harvest_links


class TestHarvestLinks:
    """Test cases for the harvest_links function."""

    def test_harvest_links_with_fixture(self, monkeypatch, caplog):
        """Test harvest_links with a realistic HTML fixture."""
        # Load the fixture HTML
        fixture_path = Path(__file__).parent / "fixtures" / "avanza_seed.html"
        fixture_html = fixture_path.read_text()
        
        # Mock get_html to return our fixture
        mock_get_html = Mock(return_value=fixture_html)
        monkeypatch.setattr("src.avanza_cli.link_harvester.get_html", mock_get_html)
        
        # Create a mock session
        mock_session = Mock()
        
        # Call the function
        with caplog.at_level(logging.INFO):
            result = harvest_links(mock_session)
        
        # Verify get_html was called once with correct arguments
        mock_get_html.assert_called_once_with(
            "https://www.avanza.se/aktier/hitta.html?s=numberOfOwners.desc&o=20000",
            mock_session
        )
        
        # Verify results
        assert len(result) > 0, "Should harvest at least some links"
        assert all(url.startswith("https://www.avanza.se/aktier/om-aktien/") for url in result), \
            "All harvested links should match expected pattern"
        
        # Verify deduplication (no duplicates)
        assert len(set(result)) == len(result), "Results should be deduplicated"
        
        # Check specific expected links (normalized)
        expected_links = {
            "https://www.avanza.se/aktier/om-aktien/abb-ltd",
            "https://www.avanza.se/aktier/om-aktien/ericsson-b",
            "https://www.avanza.se/aktier/om-aktien/volvo-b?some=query",
            "https://www.avanza.se/aktier/om-aktien/atlas-copco-a",  # fragment removed
            "https://www.avanza.se/aktier/om-aktien/whitespace",
            "https://www.avanza.se/aktier/om-aktien/"
        }
        result_set = set(result)
        
        # Most expected links should be present (allowing for some variation)
        overlap = expected_links.intersection(result_set)
        assert len(overlap) >= 4, f"Should find most expected links. Found: {result_set}, Expected: {expected_links}"
        
        # Verify logging
        log_messages = [record.message for record in caplog.records if record.levelno >= logging.INFO]
        harvest_log = [msg for msg in log_messages if "Harvested" in msg and "stock links" in msg]
        assert len(harvest_log) > 0, "Should log harvested count"
        assert str(len(result)) in harvest_log[0], "Log should include actual count"

    def test_harvest_links_robustness(self, monkeypatch, caplog):
        """Test harvest_links with synthetic HTML containing edge cases."""
        # Create a minimal HTML snippet with various edge cases
        test_html = """
        <html>
        <body>
            <!-- Valid relative links -->
            <a href="/aktier/om-aktien/test-stock">Test Stock</a>
            <a href="/aktier/om-aktien/another-stock">Another Stock</a>
            
            <!-- Valid absolute links -->
            <a href="https://www.avanza.se/aktier/om-aktien/abs-stock">Abs Stock</a>
            
            <!-- Duplicates -->
            <a href="/aktier/om-aktien/test-stock">Test Stock (dup)</a>
            <a href="https://www.avanza.se/aktier/om-aktien/test-stock">Test Stock (abs dup)</a>
            
            <!-- Wrong host -->
            <a href="https://m.avanza.se/aktier/om-aktien/mobile">Mobile</a>
            <a href="https://other.com/aktier/om-aktien/other">Other site</a>
            
            <!-- Wrong scheme -->
            <a href="http://www.avanza.se/aktier/om-aktien/insecure">HTTP</a>
            
            <!-- Fragment URLs -->
            <a href="/aktier/om-aktien/frag-stock#section">Fragment</a>
            <a href="/aktier/om-aktien/frag-stock#different">Same with different fragment</a>
            
            <!-- Non-matching links -->
            <a href="/other/page">Other</a>
            <a href="/aktier/not-om-aktien/wrong">Wrong path</a>
            
            <!-- Empty/missing href -->
            <a href="">Empty</a>
            <a>No href</a>
            <a href="   ">Whitespace only</a>
        </body>
        </html>
        """
        
        # Mock get_html to return our test HTML
        mock_get_html = Mock(return_value=test_html)
        monkeypatch.setattr("src.avanza_cli.link_harvester.get_html", mock_get_html)
        
        # Create a mock session
        mock_session = Mock()
        
        # Call the function
        with caplog.at_level(logging.INFO):
            result = harvest_links(mock_session)
        
        # Verify results
        expected_results = [
            "https://www.avanza.se/aktier/om-aktien/test-stock",  # first occurrence
            "https://www.avanza.se/aktier/om-aktien/another-stock",
            "https://www.avanza.se/aktier/om-aktien/abs-stock",
            "https://www.avanza.se/aktier/om-aktien/frag-stock"  # fragment removed, deduped
        ]
        
        assert result == expected_results, f"Expected {expected_results}, got {result}"
        
        # Verify all results follow pattern
        for url in result:
            assert url.startswith("https://www.avanza.se/aktier/om-aktien/"), \
                f"URL {url} doesn't match expected pattern"
            assert "#" not in url, f"URL {url} should not contain fragments"
        
        # Verify deduplication
        assert len(set(result)) == len(result), "Results should be deduplicated"
        
        # Verify logging
        log_messages = [record.message for record in caplog.records if record.levelno >= logging.INFO]
        harvest_log = [msg for msg in log_messages if "Harvested" in msg]
        assert len(harvest_log) > 0, "Should log harvested count"

    def test_harvest_links_no_session(self, caplog):
        """Test harvest_links handles None session gracefully."""
        with caplog.at_level(logging.WARNING):
            result = harvest_links(None)
        
        assert result == [], "Should return empty list when no session provided"
        
        warning_logs = [record.message for record in caplog.records if record.levelno >= logging.WARNING]
        session_warnings = [msg for msg in warning_logs if "No session provided" in msg]
        assert len(session_warnings) > 0, "Should log warning about missing session"

    def test_harvest_links_http_error(self, monkeypatch, caplog):
        """Test harvest_links handles HTTP errors gracefully."""
        # Mock get_html to raise an exception
        mock_get_html = Mock(side_effect=Exception("HTTP error"))
        monkeypatch.setattr("src.avanza_cli.link_harvester.get_html", mock_get_html)
        
        mock_session = Mock()
        
        with caplog.at_level(logging.WARNING):
            result = harvest_links(mock_session)
        
        assert result == [], "Should return empty list on HTTP error"
        
        warning_logs = [record.message for record in caplog.records if record.levelno >= logging.WARNING]
        http_warnings = [msg for msg in warning_logs if "Failed to fetch seed page" in msg]
        assert len(http_warnings) > 0, "Should log warning about HTTP error"

    def test_harvest_links_empty_html(self, monkeypatch, caplog):
        """Test harvest_links handles empty/short HTML gracefully."""
        # Test empty HTML
        mock_get_html = Mock(return_value="")
        monkeypatch.setattr("src.avanza_cli.link_harvester.get_html", mock_get_html)
        
        mock_session = Mock()
        
        with caplog.at_level(logging.WARNING):
            result = harvest_links(mock_session)
        
        assert result == [], "Should return empty list for empty HTML"
        
        # Test very short HTML
        mock_get_html.return_value = "<html></html>"  # Less than 100 chars
        
        with caplog.at_level(logging.WARNING):
            result = harvest_links(mock_session)
        
        assert result == [], "Should return empty list for very short HTML"
        
        warning_logs = [record.message for record in caplog.records if record.levelno >= logging.WARNING]
        short_html_warnings = [msg for msg in warning_logs if "empty or very short HTML" in msg]
        assert len(short_html_warnings) >= 1, "Should log warning about short HTML"

    def test_harvest_links_no_matching_links(self, monkeypatch, caplog):
        """Test harvest_links when HTML contains no matching stock links."""
        test_html = """
        <html>
        <body>
            <a href="/other/page">Other page</a>
            <a href="/news/article">News</a>
            <a href="https://other-site.com/stocks">External</a>
        </body>
        </html>
        """ * 10  # Make it long enough to pass length check
        
        mock_get_html = Mock(return_value=test_html)
        monkeypatch.setattr("src.avanza_cli.link_harvester.get_html", mock_get_html)
        
        mock_session = Mock()
        
        with caplog.at_level(logging.INFO):
            result = harvest_links(mock_session)
        
        assert result == [], "Should return empty list when no matching links found"
        
        # Should still log (even if 0 links)
        log_messages = [record.message for record in caplog.records if record.levelno >= logging.INFO]
        harvest_log = [msg for msg in log_messages if "Harvested" in msg and "0" in msg]
        assert len(harvest_log) > 0, "Should log even when 0 links harvested"
