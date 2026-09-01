"""
test_connection.py — Verify connectivity to the Docling for IBM watsonx managed service.

Converts docs/regulatory_guidelines_2025.epub and prints the first 500 characters of the
resulting Markdown. A successful run confirms that:
  • the service URL is reachable
  • the API key is valid
  • document conversion is working end-to-end

Usage (from the lab-1499/ directory):
    python scripts/test_connection.py

Credentials are read from the .env file in the same directory.
"""

import os
import sys
from pathlib import Path

# ── Load .env ──────────────────────────────────────────────────────────────────
env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# ── Read credentials ───────────────────────────────────────────────────────────
service_url = os.environ.get("DOCLING_SERVICE_URL", "").rstrip("/")
api_key     = os.environ.get("DOCLING_SERVICE_API_KEY", "")

if not service_url or not api_key:
    print("✗ Missing credentials.")
    print("  Set DOCLING_SERVICE_URL and DOCLING_SERVICE_API_KEY in lab-1499/.env")
    sys.exit(1)

# ── Convert ────────────────────────────────────────────────────────────────────
doc_path = Path(__file__).parent.parent / "docs" / "regulatory_guidelines_2025.epub"
if not doc_path.exists():
    print(f"✗ Document not found: {doc_path}")
    sys.exit(1)

print(f"Connecting to: {service_url}")
print(f"Converting:    {doc_path.name}")
print()

try:
    from docling.service_client import DoclingServiceClient

    with DoclingServiceClient(url=service_url, api_key=api_key) as client:
        result = client.convert(source=doc_path)

    markdown = result.document.export_to_markdown()
    print("✓ Connection successful!\n")
    print("── First 500 characters of converted Markdown ──────────────────────")
    print(markdown[:500])
    print("────────────────────────────────────────────────────────────────────")

except ImportError:
    print("✗ docling-client is not installed.")
    print("  Run:  uv pip install docling-client")
    sys.exit(1)
except (OSError, RuntimeError) as e:
    print(f"✗ Conversion failed: {e}")
    sys.exit(1)
