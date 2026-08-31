# Docling Lab — Converting Documents

All commands use `docling convert-remote`. Credentials are read automatically from the `.env`
file in the working directory.

---

## Convert a single local file

```bash
docling convert-remote my_file.pdf
```

Output is written to the current directory as `my_file.md` (Markdown by default).
To write to a specific folder:

```bash
docling convert-remote --output ./output my_file.pdf
```

---

## Convert a file from a URL

```bash
docling convert-remote https://arxiv.org/pdf/2408.09869
```

---

## Convert multiple files at once

Pass multiple paths or URLs as separate arguments:

```bash
docling convert-remote my_file.pdf \
                       api_reference.docx \
                       annual_report.epub
```

---

## Convert all documents in a directory

```bash
docling convert-remote docs/
```

By default this walks the entire directory tree and converts every supported format it finds.
To restrict to specific input formats (e.g. only PDFs and DOCX files):

```bash
docling convert-remote --from pdf --from docx docs/
```

---

## Check conversion output

After conversion, the output files appear in the directory specified by `--output` (default: `.`).
Each source document produces one output file per requested format, named after the source file.

Example — after converting `my_file.pdf` to Markdown and JSON:

```
./my_file.md
./my_file.json
```

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All documents converted successfully |
| `1` | Runtime or connection failure (service unreachable, conversion error) |
| `2` | Configuration error (no service URL resolved from flag, env, or `.env`) |
