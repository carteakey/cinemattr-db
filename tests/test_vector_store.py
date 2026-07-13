import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "api"))

import vector_store  # noqa: E402
from filter_parser import ParsedQuery  # noqa: E402


class VectorStoreTest(unittest.TestCase):
    def test_science_fiction_alias_matches_imdb_genre(self):
        parsed = ParsedQuery(
            semantic_query="space adventure",
            genres=("science fiction",),
        )
        where, params = vector_store._build_where(parsed)
        self.assertIn("list_has_any", where)
        self.assertEqual(params, [["sci-fi"]])


if __name__ == "__main__":
    unittest.main()
