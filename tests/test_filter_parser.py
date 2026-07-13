import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "api"))

import filter_parser  # noqa: E402


class FilterParserProvidersTest(unittest.TestCase):
    def test_local_provider_precedes_cloud(self):
        values = {
            "LOCAL_LLM_BASE_URL": "http://local.test/v1",
            "LOCAL_LLM_MODEL": "local-model",
            "LLM_BASE_URL": "https://cloud.test/v1",
            "LLM_API_KEY": "cloud-key",
            "LLM_MODEL": "cloud-model",
        }
        with patch.dict(os.environ, values, clear=True):
            providers = filter_parser._build_providers()
        self.assertEqual([provider.name for provider in providers], ["local", "cloud"])

    def test_parse_query_falls_back_after_local_failure(self):
        local_create = Mock(side_effect=ConnectionError("offline"))
        cloud_create = Mock(
            return_value=SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            content='{"semantic_query":"dream heist","year_from":2010,"year_to":null,"genres":["thriller"],"min_rating":7}'
                        )
                    )
                ]
            )
        )
        providers = [
            filter_parser.LLMProvider(
                "local", SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=local_create))), "local-model"
            ),
            filter_parser.LLMProvider(
                "cloud", SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=cloud_create))), "cloud-model"
            ),
        ]
        with patch.object(filter_parser, "_build_providers", return_value=providers):
            parsed = filter_parser.parse_query("dream heist after 2010")
        self.assertEqual(parsed.semantic_query, "dream heist")
        self.assertEqual(parsed.year_from, 2010)
        self.assertEqual(parsed.genres, ("thriller",))
        self.assertEqual(local_create.call_count, 1)
        self.assertEqual(cloud_create.call_count, 1)


if __name__ == "__main__":
    unittest.main()
