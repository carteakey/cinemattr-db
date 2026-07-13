import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "api"))

import embeddings  # noqa: E402
import load_embeddings  # noqa: E402


class EmbeddingsTest(unittest.TestCase):
    def test_config_loads_task_prefixes(self):
        values = {
            "EMBEDDING_BASE_URL": "http://embeddings.test/v1",
            "EMBEDDING_API_KEY": "test",
            "EMBEDDING_MODEL": "nomic-embed-text-v1.5",
            "EMBEDDING_DIM": "768",
            "EMBEDDING_QUERY_PREFIX": "search_query: ",
            "EMBEDDING_DOCUMENT_PREFIX": "search_document: ",
        }
        with patch.dict(os.environ, values, clear=True):
            config = embeddings.load_embedding_config()
        self.assertEqual(config.query_prefix, "search_query: ")
        self.assertEqual(config.document_prefix, "search_document: ")

    def test_query_prefix_is_sent_to_provider(self):
        create = Mock(
            return_value=SimpleNamespace(data=[SimpleNamespace(embedding=[0.1, 0.2])])
        )
        client = SimpleNamespace(embeddings=SimpleNamespace(create=create))
        config = embeddings.EmbeddingConfig(
            base_url="http://embeddings.test/v1",
            api_key="test",
            model="nomic-embed-text-v1.5",
            dim=768,
            query_prefix="search_query: ",
            document_prefix="search_document: ",
        )
        vector = embeddings.embed_query(client, config, "dream heist")
        self.assertEqual(vector, [0.1, 0.2])
        create.assert_called_once_with(
            model="nomic-embed-text-v1.5",
            input=["search_query: dream heist"],
            dimensions=768,
        )

    def test_movie_document_contains_search_fields(self):
        text = load_embeddings._movie_embedding_text(
            {
                "title": "Arrival",
                "year": 2016,
                "genres": ["Drama", "Sci-Fi"],
                "plot": "A linguist works with an alien language.",
            }
        )
        self.assertEqual(
            text,
            "Arrival. 2016. Drama, Sci-Fi. A linguist works with an alien language.",
        )

    def test_movie_document_omits_missing_plot(self):
        text = load_embeddings._movie_embedding_text(
            {
                "title": "Example Film",
                "year": 2026,
                "genres": ["Drama"],
                "plot": None,
            }
        )
        self.assertEqual(text, "Example Film. 2026. Drama")


if __name__ == "__main__":
    unittest.main()
