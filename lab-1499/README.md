# Hands-On with Docling for IBM watsonx
## Convert, Extract, and Build Your Own Document-Powered App

> **IBM TechXchange 2026 · Hands-on Lab · 90 minutes**

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Lab Architecture](#2-lab-architecture)
3. [Part 1 — Get Ready with Docling for IBM watsonx](#3-part-1--get-ready-with-docling-for-ibm-watsonx) *(20 min)*
4. [Part 2 — Choose Your Track](#4-part-2--choose-your-track) *(35 min)*
   - [Track A — Document Intelligence with IBM Bob](#track-a--document-intelligence-with-ibm-bob)
   - [Track B — Document Intelligence with Python](#track-b--document-intelligence-with-python)
5. [Use Case — Fill the Compliance Assessment Form](#5-use-case--fill-the-compliance-assessment-form) *(20 min)*
6. [Continue Learning](#6-continue-learning)

---

## 1. Introduction

### The scenario

You are a compliance analyst at **Vantara Bank**, a mid-sized bank operating across multiple regulatory jurisdictions. Every quarter you receive a bundle of regulatory submissions, audit reports, and product disclosure sheets — all arriving as PDFs, Word documents, and EPUBs — and must populate a standardised **Compliance Assessment Form** with the key obligations, risk indicators, and financial metrics they contain.

Today you will use **Docling for IBM watsonx** to automate that pipeline: from raw documents to a filled-in, audit-ready assessment — without standing up a single GPU.

### About this lab

This lab introduces **Docling for IBM watsonx**, the enterprise managed-service edition of the open-source [Docling](https://github.com/docling-project/docling) document AI toolkit. You will:

- Use the **Workbench** (no-code UI) to convert, inspect, and export compliance documents
- Export documents to the **DocLang** open ISO standard and explore them in the **DocLang Viewer**
- Choose a hands-on track: **IBM Bob** (AI-assistant, no coding) or **Python notebook** (developer)
- Extract structured data from multiple documents and auto-populate a Compliance Assessment Form

### Lab timing

| Part | Activity | Time |
|------|----------|------|
| Part 1 | Get Ready with Docling for IBM watsonx | 20 min |
| Part 2 | Choose Your Track (Bob *or* Python) | 35 min |
| Use Case | Fill the Compliance Assessment Form | 20 min |
| **Total** | | **75 min** |

> The remaining 15 minutes of the 90-minute session are reserved for setup, Q&A, and the optional extension steps.

### Prerequisites

- A Linux virtual machine (**VM**) to run Docling and all the lab exercises
- A **Docling for IBM watsonx** trial account — you will create this in Part 1
- **IBM Bob** access — provided by your instructor
- **watsonx.ai** access - provided by your instructor

---

## 2. Lab Architecture

### How Docling for IBM watsonx works

<!-- TODO: add architecture diagram showing: browser/SDK → Docling for IBM watsonx API (IBM Cloud) → DoclingDocument → export formats / downstream tools -->

Docling for IBM watsonx exposes the Docling document AI stack as a **fully managed REST API**. You send a document (PDF, DOCX, EPUB, image, …); the service runs layout analysis, OCR, table structure detection, and enrichment on IBM Cloud; and returns a structured **DoclingDocument** you can export to Markdown, HTML, JSON, or DocLang.

| Component | Role in this lab |
|-----------|-----------------|
| **Docling for IBM watsonx** | Managed conversion service — no local GPU or model needed |
| **Workbench** | No-code drag-and-drop UI for conversion and export |
| **DocLang Viewer** | Online viewer for DocLang-format documents |
| **IBM Bob** (Track A) | AI assistant connected to Docling via **Docling Skills** (packaged in the `docling` library) and/or the **Docling MCP server** |
| **Docling Service Client** (Track B) | Use the Docling [Python SDK](https://developer.dcls.saas.ibm.com/examples/basic/#python-sdk-usage) to make API calls to Docling as a service |
| **watsonx.ai** (Track B) | LLM for RAG generation and compliance summarization |

### Lab documents

The `lab-1499/docs` folder of the **Lab Repository** contains all the necessary documents to run this lab assignments:

| File | Format | Key fields it contains |
|------|--------|----------------------|
| `vantara_risk_disclosure.pdf` | PDF (4 pages) | Entity name, reporting period, reference number, total risk exposure, risk category table, capital ratios, **logo**, **3 data charts** |
| `q3_audit_summary.docx` | DOCX (6 sections) | Material findings, sign-off date, confirms risk exposure and reference number |
| `regulatory_guidelines_2025.epub` | EPUB (7 chapters) | Key regulatory obligations, AML requirements, reporting deadlines |
| `compliance_assessment_form.json` | JSON | Empty form template — to be populated during the lab |
| `docling_lab.ipynb` | IPYNB | Notebook for Track B

---

## 3. Part 1 — Get Ready with Docling for IBM watsonx
### *(20 minutes — everyone)*

### 3.1 Get the Lab Repository

Open a terminal on the VM and run:

```bash
git clone https://github.com/IBM/txc2026-docling.git
cd txc2026-docling/lab-1499
```

The lab material for this lab is included in the subfolder `docs`.

<!-- TODO: run a start.sh bash script to set up the environment -->

### 3.2 Sign Up for a Free Trial

1. In the browser on the lab VM, go to [ibm.biz/docling-for-watsonx-trial-txc26](https://ibm.biz/docling-for-watsonx-trial-txc26).
2. Sign in with your own IBMid, or create one if you do not have one. This is your personal
account and it is **not** the workshop student login.
3. Follow the prompts to start the trial. Provisioning takes about a minute; wait until the instance is shown as active (_Your trial is ready!_).
4. Click _Access your trial now_ and you will land on the **Docling for IBM watsonx** dashboard.

<!-- TODO: add screenshot of dashboard landing page -->

---

### 3.3 Explore the Workbench

The **Workbench** converts documents with drag-and-drop — no code needed.

<!-- TODO: add screenshot of the Workbench UI with callouts -->

1. In the Workbench, click **Upload a document** and select `vantara_risk_disclosure.pdf` from the `docs/` folder.
2. When conversion completes, you will be redirected to the **Tasks** section where you can inspect the result:
   - The **left column** displays the document structure as a tree of nodes — sections, paragraphs, tables, and figures. Each node shows its structural role (heading, paragraph, table, …) in the upper-left corner and its unique identifier in the upper-right corner. Note that text extracted from images appears as dedicated nodes, and that headers and footers are shown on a shaded background to distinguish them from the body content.
   - The **right column** renders the document as it appears in the original file. Click on any element to highlight the corresponding node in the left column.
   - Click any table node to inspect how individual cells and column headers were reconstructed.
3. Repeat with `q3_audit_summary.docx` or with any URL of your choice, and observe that it produces the same Docling document structure regardless of the input format.
4. Try a conversion with custom options: in the **Tasks** section, click **Create task**, select **Single**, and click **Next**. Select `vantara_risk_disclosure.pdf` again and configure the pipeline options before submitting:
   - **Output formats:** check the **DCLX** box to also produce a DocLang archive.
   - **Text:** uncheck **Enable OCR**, since this is a born-digital PDF.
   - **Images:** check **Picture classification** to label each image by type.

   After clicking **Create Task**, compare the new result with the first conversion and note the differences.
5. Click **Download** in the upper-right corner to save the selected conversion output before moving on. You will use it in the next step.

---

### 3.4 Inspect the DocLang File — the ISO Standard

**DocLang** is an open XML-based ISO standard for structured documents. It defines a machine-readable format for documents of any type — like JSON for data or HTML for the web — that any tool can implement and any pipeline can consume. Docling exports the document conversion output to DocLang and packages it in a `.dclx` archive containing the structural data alongside page images, ready for any downstream tool that reads the standard.

1. Open **<https://doclang.ai/viewer/>** and drag the `.dclx` file you downloaded in [section 3.3, step 5](#33-explore-the-workbench) onto it.
2. Browse the document in the central panel. Observe the structural nodes in the left panel and how they are rendered in the right panel.
3. Click the **Layers** drop-down in the upper-right corner and uncheck **Furniture**. Observe that headers and footers are no longer displayed.

<!-- TODO: add screenshot of the DocLang Viewer with tree panel callout -->

---

### 3.5 Get an API Key

You will need an API key and a service URL to authenticate the tools in Part 2 against the managed service.

<!-- TODO: add screenshot of the API key creation screen with callouts -->

1. In your browser, go back to the **Docling for IBM watsonx** dashboard.

   > [!TIP]
   > If you accidentally closed the browser tab, you can return to your **Docling for IBM watsonx Trial** through the **My IBM** page at [myibm.ibm.com/dashboard](https://myibm.ibm.com/dashboard/).

2. Go to the **API examples** section. You will find the **Service URL** to connect to the Docling service.
3. Click **Get API key** and then click **Generate an API key**. In the dialogue, enter a name for your key (for example, **MyDoclingKey**) and confirm.
4. From the confirmation screen, copy the **API key** and save it somewhere accessible — you will paste it into your tool configuration in Part 2.

   > [!WARNING]
   > Never commit your API key to a git repository. The `.env` file used in this lab is already listed in `.gitignore`.

---

### 3.6 Set your Connection Details in the Environment

The best place to save your credentials is in the environment file of the lab workspace in **IBM Bob**. This prepares the connections needed for Part 2.

1. In **IBM Bob**, use the **Explorer** to locate the `lab-1499/.env.example` file.
2. Copy the file and rename the copy to `.env` in the same folder.
3. In the `.env` file, replace the `DOCLING_SERVICE_URL` value with your **Service URL** and the `DOCLING_SERVICE_API_KEY` value with your **API key** from section [3.5](#35-get-an-api-key).

## 4. Part 2 — Choose Your Track
### *(35 minutes — pick Track A or Track B)*

**Track A — IBM Bob:** no code, conversational, requires Bob access. Bob connects to Docling via **Skills** (installed from the `docling` package) and/or the **Docling MCP server**.
**Track B — Python notebook:** requires Python 3.10+ and `uv`.

Rejoin at [Section 5 (Use Case)](#5-use-case--fill-the-compliance-assessment-form) when your 35 minutes are up.

---

### Track A — Document Intelligence with IBM Bob

#### A.1 Connect to the Managed Service

In **IBM Bob**, go back to the terminal you opened in section [3.1](#31-get-the-lab-repository).

Install `uv` if it is not already present on the VM (requires only `curl`):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env   # add uv to PATH for the current session
```

Create a virtual environment with Python 3.14. `uv` will automatically download it
if needed — no manual Python installation required.

> [!NOTE]
> Docling's lightweight service client requires Python 3.10 or later.

```bash
uv venv --python 3.14
source .venv/bin/activate
```

Install the lightweight service-client:

```bash
uv pip install "docling-client"
```

Test the connection by running the provided script. It converts `docs/regulatory_guidelines_2025.epub`
and prints the first lines of the resulting Markdown — confirming that the service URL is
reachable, the API key is valid, and conversion is working end-to-end:

```bash
python scripts/test_connection.py
```

A successful run looks like:

```
Connecting to: https://<your-service-url>
Converting:    regulatory_guidelines_2025.epub

✓ Connection successful!

── First 500 characters of converted Markdown ──────────────────────
## Q3 2026 Internal Audit Summary
...
```

If you see an error, check:
- `✗ Missing credentials` → `.env` is missing or the variable names are wrong (section [3.6](#36-set-your-connection-details-in-the-environment))
- `401` / `403` → API key is incorrect or expired (section [3.5](#35-get-an-api-key))
- Connection error → Service URL is wrong or the network blocks outbound HTTPS

---

#### A.2 Load the Lab Skills in Bob

The lab repository already includes a custom skill set under `.bob/skills/docling-lab/`. This skill teaches Bob how to convert documents using the `docling convert-remote` CLI — a no-code, command-line approach that is easy to follow regardless of Python experience.

Bob scans `.bob/skills/` automatically at workspace load time, so the skill is available from your first conversation. The first time Bob uses it, it will prompt **Tools awaiting approval: Use Skill docling-lab** — click **Approve skill tools for task**.

Verify the skill is in place:

```bash
ls .bob/skills/docling-lab/SKILL.md
```

> [!NOTE]
> The Docling package also ships a bundled usage skill covering the Python SDK (conversion, extraction, chunking, and the service client). If you want to make it available to Bob as well, you can install it with [`library-skills`](https://library-skills.io):
>
> ```bash
> uvx library-skills install --skill docling --all -y
> mkdir -p .bob/skills
> ln -sf ../../.agents/skills/docling .bob/skills/docling
> ```
>
> Full reference: [agent skills docs](https://docling-project.github.io/docling/usage/agent_skills/).

---


#### A.3 Convert, Explore, and Extract

Try these prompts in Bob:

```
Convert docs/vantara_risk_disclosure.pdf and return the document key.
```
```
Show me the heading outline of the document.
```
```
What are the main risk categories described in this document?
```
```
From docs/vantara_risk_disclosure.pdf, extract these fields as JSON:
entity_name, reporting_period, total_risk_exposure_usd,
highest_risk_category, regulator_reference_number.
```

> When done, proceed to [Section 5 (Use Case)](#5-use-case--fill-the-compliance-assessment-form).

---

### Track B — Document Intelligence with Python

#### B.1 Environment Setup

```bash
uv venv && source .venv/bin/activate
uv pip install notebook ipywidgets ipykernel
cp env.example .env    # then edit .env with your credentials
jupyter notebook docling_lab.ipynb
```

Install the service-client package (no local ML models needed):

```bash
uv pip install "docling-slim[service-client,feat-chunking]"
```

---

#### B.2 Work Through the Notebook

Open `docling_lab.ipynb` and run sections **1 through 7** in order:

| Section | What you do |
|---------|------------|
| 1 | Connect to the managed service (`DoclingServiceClient`) |
| 2 | Convert PDF, DOCX, and EPUB with `client.convert_all()` |
| 3 | Inspect the document structure (sections, tables, figures) |
| 4 | Export to DocLang and query with `dclq` shell commands |
| 5 | Chunk a document with `client.chunk(ChunkerKind.HYBRID)` |
| 6 | RAG pipeline: `DoclingLoader` → Milvus → watsonx.ai LLM |
| 7 | Extract typed fields with `DocumentExtractor` + Pydantic |

Key `dclq` commands you will run in Section 4:

```bash
dclq inspect  vantara_risk_disclosure.dclx   # inventory
dclq outline  vantara_risk_disclosure.dclx   # heading tree + XPaths
dclq grep -i  'risk exposure' vantara_risk_disclosure.dclx
dclq show     vantara_risk_disclosure.dclx '/heading[2]' --section
```

> When done with Section 7, proceed to [Section 5 (Use Case)](#5-use-case--fill-the-compliance-assessment-form).

---

## 5. Use Case — Fill the Compliance Assessment Form
### *(20 minutes — everyone)*

The **Compliance Assessment Form** (`docs/compliance_assessment_form.json`) defines these fields — spread across the three documents:

```json
{
  "entity_name": null,
  "reporting_period": null,
  "total_risk_exposure_usd": null,
  "highest_risk_category": null,
  "regulator_reference_number": null,
  "key_obligations": [],
  "material_findings": [],
  "sign_off_date": null
}
```

### Step 1 — Convert All Three Documents *(2 min)*

**Track A (Bob):**
```
Convert all three documents in the docs/ folder and return their document keys.
```

**Track B (notebook — Section 2):** already done. Skip to Step 2.

---

### Step 2 — Get a Document Outline *(3 min)*

**Track A (Bob):**
```
Show me the heading outline of q3_audit_summary.docx.
```

**Track B — `dclq`:**
```bash
dclq outline q3_audit_summary.dclx
```

---

### Step 3 — Extract Compliance Metrics *(5 min)*

**Track A (Bob):**
```
From all three compliance documents, extract the following fields and return JSON:
entity_name, reporting_period, total_risk_exposure_usd, highest_risk_category,
regulator_reference_number, key_obligations (list), material_findings (list), sign_off_date.
```

**Track B (notebook — Section 7):** the `ComplianceForm` Pydantic model and multi-document merge loop are already in the notebook. Run the extraction cells.

---

### Step 4 — Generate a Summary and Fill the Form *(10 min)*

**Track A (Bob):**
```
Based on the extracted data, write a 3-sentence compliance executive summary
covering total risk exposure, the highest risk category, and the critical obligations.
Then create a new DoclingDocument with the filled form as a structured table
and export it to Markdown.
```

**Track B (notebook — Section 8):** Run the "Putting It All Together" cells — they assemble the extracted fields, call the watsonx.ai LLM for the narrative summary, and render an HTML compliance report.

---

### ✅ What you achieved

By the end of this use case you have processed three documents in three different formats, extracted eight structured fields without manual copy-paste, generated an executive summary, and produced an audit-ready HTML report — all powered by Docling for IBM watsonx.

---

### Extension steps (if time permits)

**Track A — Auto-fill the Word document (requires Bob DOCX tool):**
```
Using the extracted values, update docs/compliance_assessment_form.docx
and save the result as docs/compliance_assessment_form_filled.docx.
```

**Track B — Edit the form with Docling Agent (Section 9 in the notebook):**
The notebook includes commented-out code for `DoclingEditingAgent` that can apply natural-language edits to a DoclingDocument template.
> ℹ️ `docling-agent` is not yet on PyPI — install from source:  
> `pip install git+https://github.com/docling-project/docling-agent`

---

## 6. Continue Learning

The lab materials remain available after the event:
**<https://github.com/docling-project/docling-workshops>** → folder `workshops/2026_10_26/`

The Docling for IBM watsonx **free trial** stays active — continue at:
**<https://www.ibm.com/products/docling>**

### Resources

| Resource | URL |
|----------|-----|
| Docling for IBM watsonx | <https://www.ibm.com/products/docling> |
| Free trial sign-up | <https://www.ibm.com/account/reg/us-en/signup?formid=urx-54322> |
| Docling documentation | <https://docling-project.github.io/docling/> |
| Managed service docs | <https://docling-project.github.io/docling/usage/api_server/managed/> |
| Agent skills docs | <https://docling-project.github.io/docling/usage/agent_skills/> |
| Docling MCP | <https://github.com/docling-project/docling-mcp> |
| Docling Agent | <https://github.com/docling-project/docling-agent> |
| DocLang Viewer | <https://doclang.ai/viewer/> |
| `dclq` CLI | <https://github.com/docling-project/docling-core/tree/main/packages/dclq> |
| Docling GitHub | <https://github.com/docling-project/docling> |
| Docling Discord | <https://docling.ai/discord> |

### Quick reference — service client

```python
from docling.service_client import DoclingServiceClient, ChunkerKind

client = DoclingServiceClient(url=..., api_key=...)

result = client.convert(source="report.pdf")          # single document
print(result.document.export_to_markdown())

for r in client.convert_all(source=["a.pdf", "b.docx"], max_concurrency=4):
    print(r.input.file.name, r.status)                # many documents

response = client.chunk(source="report.pdf", chunker=ChunkerKind.HYBRID)
```

### Quick reference — `dclq`

```bash
dclq inspect  doc.dclx                            # structural inventory
dclq outline  doc.dclx                            # heading tree + XPaths
dclq grep -i  'PATTERN' doc.dclx                  # search semantic units
dclq show     doc.dclx '/heading[N]' --section    # retrieve a section
dclq list     doc.dclx --type table_cell --page 1 # list units by type
```

### Quick reference — Docling MCP (remote mode)

```bash
DOCLING_MCP_CONVERSION_MODE=remote \
DOCLING_MCP_SERVICE_URL=https://<url> \
DOCLING_MCP_SERVICE_API_KEY=<key> \
uvx --from docling-mcp docling-mcp-server \
  --transport streamable-http conversion generation manipulation
```

### Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `DOCLING_SERVICE_URL not set` | `.env` not loaded | Run `cp env.example .env` and fill in your credentials |
| Conversion returns empty Markdown | Scanned PDF, no text layer | Enable **OCR** in the Workbench options |
| `dclq` command not found | Package not installed | `pip install dclq` |
| Notebook kernel crash on import | Missing dependency | Re-run the install cell, then **Kernel → Restart** |
| MCP server not responding | Server not started | Re-run the `uvx --from docling-mcp ...` command; check port 8000 is free |
| watsonx.ai returns 401 | Expired API key | Regenerate the key at cloud.ibm.com and update `.env` |
