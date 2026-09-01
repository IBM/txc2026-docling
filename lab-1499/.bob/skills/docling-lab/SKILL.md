---
name: docling-lab
description: >
  Use when the user wants to convert documents with the Docling for IBM watsonx managed service
  from the command line. Covers converting a single file, a URL, or a directory of documents,
  choosing output formats, enabling enrichments (OCR, table extraction, picture classification,
  chart data), and filtering by input format. Trigger phrases: "convert", "docling convert",
  "convert this document", "convert to markdown", "extract from PDF", "convert with docling",
  "convert a directory", "docling remote", "docling-lab".
---

# Docling Lab — Command-Line Conversion Skill

This skill teaches Bob how to convert documents using the `docling convert-remote` CLI, which
sends conversion requests to the **Docling for IBM watsonx** managed service. No local ML models
or GPU are required — all processing happens on the service.

## Prerequisites

- Virtual environment from lab section A.1 is active (`.venv`)
- `.env` file in `lab-1499/` is populated with `DOCLING_SERVICE_URL` and `DOCLING_SERVICE_API_KEY`
- `docling-client` is installed (`uv pip install docling-client`)

Credentials are loaded automatically from the `.env` file — no need to export them manually.

## When to use each reference

| Task | Reference |
|---|---|
| Convert a file, URL, or directory | [convert.md](convert.md) |
| Customize the pipeline (OCR, enrichments, output formats, page range) | [options.md](options.md) |

Always read the relevant reference before running a command.
When the user's request involves a conversion task, activate this skill and route to the
appropriate reference file.
