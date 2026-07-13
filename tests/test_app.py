import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "api"))
os.environ.setdefault("LLM_BASE_URL", "https://example.invalid/v1")
os.environ.setdefault("LLM_API_KEY", "test")

import app  # noqa: E402


class AppHelpersTest(unittest.TestCase):
    def test_normalize_query_collapses_whitespace(self):
        self.assertEqual(app._normalize_query("  dream\n heist  "), "dream heist")

    def test_top_k_is_bounded(self):
        self.assertEqual(app._resolve_top_k(0), 1)
        self.assertEqual(app._resolve_top_k(500), 200)

    def test_untrusted_peer_cannot_spoof_forwarded_ip(self):
        request = SimpleNamespace(
            client=SimpleNamespace(host="203.0.113.10"),
            headers={"x-forwarded-for": "198.51.100.20"},
        )
        original_headers = app.TRUST_PROXY_HEADERS
        original_ips = app.TRUSTED_PROXY_IPS
        try:
            app.TRUST_PROXY_HEADERS = True
            app.TRUSTED_PROXY_IPS = {"192.0.2.10"}
            self.assertEqual(app._resolve_client_ip(request), "203.0.113.10")
        finally:
            app.TRUST_PROXY_HEADERS = original_headers
            app.TRUSTED_PROXY_IPS = original_ips

    def test_format_movie_details_matches_frontend_contract(self):
        details = app._format_movie_details(
            ("tt1234567", "Example", 2026, ["Drama", "Sci-Fi"], 7.5, None)
        )
        self.assertEqual(details["imdbID"], "tt1234567")
        self.assertEqual(details["Genre"], "Drama, Sci-Fi")
        self.assertEqual(details["Poster"], "N/A")
        self.assertEqual(details["source"], "cinemattr")


if __name__ == "__main__":
    unittest.main()
