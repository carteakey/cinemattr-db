import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_CONFIG_FILES = (ROOT / ".env.example", ROOT / "deploy/.env.example")
PLACEHOLDER_VALUES = {"", "...", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
TOKEN_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
    re.compile(r"\b(?:nvapi|ghp|github_pat|xox[baprs])-[A-Za-z0-9_-]{16,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b"),
)


class PublicConfigTest(unittest.TestCase):
    def test_public_config_has_placeholders_not_credentials(self):
        for path in PUBLIC_CONFIG_FILES:
            text = path.read_text(encoding="utf-8")
            for pattern in TOKEN_PATTERNS:
                self.assertIsNone(pattern.search(text), f"credential-shaped value in {path}")

            for line in text.splitlines():
                key, separator, value = line.partition("=")
                if separator and ("API_KEY" in key or "TOKEN" in key):
                    self.assertIn(
                        value.strip(),
                        PLACEHOLDER_VALUES,
                        f"non-placeholder credential value in {path}: {key}",
                    )


if __name__ == "__main__":
    unittest.main()
