# Hands-On with Docling for IBM watsonx
## Convert, Extract, and Build Your Own Document-Powered App

> **IBM TechXchange 2026 · Hands-on Lab · 90 minutes**

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Lab Architecture](#2-lab-architecture)
3. [Part 1 — Get Ready with Docling for IBM watsonx](#3-part-1--get-ready-with-docling-for-ibm-watsonx) *(20 min)*
4. [Part 2 — Choose Your Track](#4-part-2--choose-your-track) *(50 min)*
   - [Where both tracks are going: the form](#where-both-tracks-are-going-the-form)
   - [Track A — Document Intelligence with IBM Bob](#track-a--document-intelligence-with-ibm-bob)
   - [Track B — Document Intelligence with Python](#track-b--document-intelligence-with-python)
5. [Compare the Two Routes](#5-compare-the-two-routes) *(5 min)*
6. [Continue Learning](#6-continue-learning)

---

## 1. Introduction

### The scenario

You are a compliance analyst at **Vantara Bank**, a mid-sized bank operating across multiple regulatory jurisdictions. Every quarter you receive a bundle of regulatory submissions, audit reports, and product disclosure sheets — all arriving as PDFs, Word documents, and EPUBs — and must populate a standardised **Compliance Assessment Form** with the key obligations, risk indicators, and financial metrics they contain.

Today you will use **Docling for IBM watsonx** to automate that pipeline: from raw documents to a filled-in, audit-ready assessment — without standing up a single GPU.

### About this lab

This lab introduces **Docling for IBM watsonx**, the enterprise managed-service edition of the open-source [Docling](https://github.com/docling-project/docling) document AI toolkit. You will:

- Use the **Workbench** (no-code UI) to convert, inspect, and export compliance documents
- Export documents to **DocLang**, the open ISO standard for documents, and explore them in the **DocLang Viewer**
- Choose a hands-on track — **IBM Bob** (AI assistant, no coding) or a **Python notebook** (developer) — that takes you all the way to the filled Compliance Assessment Form
- Compare the two routes to the same eight fields

### Lab timing

| Part | Activity | Time |
|------|----------|------|
| Part 1 | Get Ready with Docling for IBM watsonx | 20 min |
| Part 2 | Your track (Bob *or* Python), ending with the filled form | 50 min |
| Compare | Two routes to the same eight fields | 5 min |
| **Total** | | **75 min** |

> The remaining 15 minutes of the 90-minute session are reserved for setup, Q&A, and the optional extension steps.

### Prerequisites

- A Linux virtual machine (**VM**) to run Docling and all the lab exercises
- An **IBMid** — the same IBMid signs you in everywhere today: the lab, the Docling trial and IBM Bob
- A **Docling for IBM watsonx** trial account — you will create this in Part 1
- **IBM Bob** — already installed on the VM. Track A talks to its assistant; Track B uses it as an IDE
- **watsonx.ai** access *(Track B)* — an API key and project ID shared by the class, put on your VM by the instructors (nothing to type)

---

## 2. Lab Architecture

### How Docling for IBM watsonx works

Docling for IBM watsonx exposes the Docling document AI stack as a **fully managed REST API**. You send a document (PDF, DOCX, EPUB, image, …); the service runs layout analysis, OCR, table structure detection, and enrichment on IBM Cloud; and returns a structured **DoclingDocument** you can export to Markdown, HTML, JSON, or DocLang.

Nothing on the VM runs a model: Docling converts on your trial instance, and in Track B **watsonx.ai** serves the embedding model and the language model.

| Component | Role in this lab |
|-----------|-----------------|
| **Docling for IBM watsonx** | Managed conversion service — no local GPU or model needed |
| **Workbench** | No-code drag-and-drop UI for conversion and export |
| **DocLang Viewer** | Online viewer for DocLang-format documents |
| **IBM Bob** (Track A) | AI assistant connected to Docling via the **docling-lab skill** in `.bob/skills/` |
| **IBM Bob as an IDE** (Track B) | Runs the notebook: editor, Explorer and terminal |
| **Docling Service Client** (Track B) | The Docling [Python SDK](https://developer.dcls.saas.ibm.com/examples/basic/#python-sdk-usage) for calling Docling as a service |
| **watsonx.ai** (Track B) | Embeddings and LLM for RAG, extraction, the summary, and the editing agent |

### Lab documents

The `lab-1499/docs` folder of the **Lab Repository** contains all the documents for this lab:

| File | Format | Key fields it contains |
|------|--------|----------------------|
| `vantara_risk_disclosure.pdf` | PDF (4 pages) | Entity name, reporting period, reference number, total risk exposure, risk category table, capital ratios, **logo**, **3 data charts** |
| `q3_audit_summary.docx` | DOCX (6 sections) | Material findings, sign-off date, confirms risk exposure and reference number |
| `regulatory_guidelines_2025.epub` | EPUB (7 chapters) | Key regulatory obligations, AML requirements, reporting deadlines |
| `compliance_assessment_form.json` | JSON | Empty form template — filled at the end of both tracks |
| `docling_lab.ipynb` | IPYNB | Notebook for Track B |

---

## 3. Part 1 — Get Ready with Docling for IBM watsonx
### *(20 minutes — everyone)*

### 3.1 Set up the lab

Open a terminal on the VM and run:

```bash
curl -L ibm.biz/txc26-1499-bootstrap | bash -
```

It clones the lab into `~/txc2026-docling/lab-1499`, installs `uv`, creates the Python environment with the Docling client, and writes your `.env` from `.env.example`. It is safe to run again. Then:

```bash
cd ~/txc2026-docling/lab-1499
```

### 3.2 Sign Up for a Free Trial

1. In the browser on the lab VM, go to [ibm.biz/docling-for-watsonx-trial-txc26](https://ibm.biz/docling-for-watsonx-trial-txc26).
2. Sign in with your IBMid, or create one if you do not have one.
3. Follow the prompts to start the trial. Provisioning takes about a minute; wait until the instance is shown as active (_Your trial is ready!_).
4. Click _Access your trial now_ and you will land on the **Docling for IBM watsonx** dashboard.

---

### 3.3 Explore the Workbench

The **Workbench** converts documents with drag-and-drop — no code needed.

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
5. Click **Actions**, then **Download**, to save the selected conversion output before moving on. You will use it in the next step.

---

### 3.4 Inspect the DocLang File — the ISO Standard

**DocLang** is an open XML-based ISO standard for structured documents. It defines a machine-readable format for documents of any type — like JSON for data or HTML for the web — that any tool can implement and any pipeline can consume. Docling exports the document conversion output to DocLang and packages it in a `.dclx` archive containing the structural data alongside page images, ready for any downstream tool that reads the standard. The format and its tooling are described at **<https://doclang.ai>**; the **DocLang Viewer** at **<https://doclang.ai/viewer/>** opens a `.dclx` file right in your browser.

1. Open **<https://doclang.ai/viewer/>** and drag the `.dclx` file you downloaded in [section 3.3, step 5](#33-explore-the-workbench) onto it.
2. Browse the document in the central panel. Observe the structural nodes in the left panel and how they are rendered in the right panel.
3. Click the **Layers** drop-down in the upper-right corner and uncheck **Furniture**. Observe that headers and footers are no longer displayed.

---

### 3.5 Get an API Key

You will need an API key and a service URL to authenticate the tools in Part 2 against the managed service.

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

Both tracks read the same `.env` file in the lab folder. The bootstrap already created it from `.env.example`.

1. In **IBM Bob**, use the **Explorer** to open `lab-1499/.env` (if it is missing, copy `.env.example` to `.env`).
2. Replace the `DOCLING_SERVICE_URL` value with your **Service URL** and the `DOCLING_SERVICE_API_KEY` value with your **API key** from section [3.5](#35-get-an-api-key).
3. Leave the three `WX_` lines as they are: they are the **watsonx.ai** credentials Track B uses, shared by the whole class. The instructors put them on your VM in `~/lab-1499-watsonx.txt` before the lab, and the bootstrap copied them into `.env`.

   > [!TIP]
   > If the `WX_` lines still show `your-…` placeholders, the bootstrap ran before the file arrived: run it again (it keeps what you typed), or copy the lines from `~/lab-1499-watsonx.txt`. The notebook also reads that file on its own.

---

## 4. Part 2 — Choose Your Track
### *(50 minutes — pick Track A or Track B)*

Both tracks use the same service, the same three documents and the same credentials, and both end with the same thing: the Compliance Assessment Form, filled in. What differs is what you drive it from — a conversation, or code you can read.

| | Track A — IBM Bob | Track B — Python notebook |
|---|---|---|
| **Best for** | anyone; no coding required | developers, and anyone who wants the API |
| **You drive it from** | a conversation with Bob's assistant | notebook cells, in Bob's editor |
| **You finish with** | the form as JSON, a summary, and the form as a Markdown table | the form as a typed object, an HTML report, and the form as a DocLang file |
| **Needs** | IBM Bob, already on the VM | the shared watsonx.ai credentials, already on your VM (section [3.6](#36-set-your-connection-details-in-the-environment)) |

Go to [Section 5 (Compare)](#5-compare-the-two-routes) when your 50 minutes are up, even if you have not finished.

### Where both tracks are going: the form

The **Compliance Assessment Form** (`docs/compliance_assessment_form.json`) defines eight fields — spread across the three documents:

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

| Field | Lives in |
|-------|----------|
| `entity_name`, `reporting_period`, `total_risk_exposure_usd`, `highest_risk_category`, `regulator_reference_number` | `vantara_risk_disclosure.pdf` |
| `material_findings` (list), `sign_off_date` | `q3_audit_summary.docx` |
| `key_obligations` (list) | `regulatory_guidelines_2025.epub` |

The spread is deliberate. Two of the fields — the risk exposure and the regulator's reference number — appear in **two** documents, which is what lets you check one against the other.

---

### Track A — Document Intelligence with IBM Bob

#### A.1 Check the Connection

In the terminal from section [3.1](#31-set-up-the-lab), activate the environment the bootstrap created and run the provided check. It converts `docs/regulatory_guidelines_2025.epub` and prints the first lines of the resulting Markdown — confirming that the service URL is reachable, the API key is valid, and conversion is working end-to-end:

```bash
cd ~/txc2026-docling/lab-1499
source .venv/bin/activate
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

#### A.2 Log in to Bob and Load the Lab Skill

In the panel on the right of IBM Bob, click **Log in to Bob** and sign in with your IBMid.

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

#### A.4 Fill the Form from All Three Documents

```
Convert all three documents in the docs/ folder and return their document keys.
```
```
Show me the heading outline of q3_audit_summary.docx.
```
```
From all three compliance documents, extract the following fields and return JSON:
entity_name, reporting_period, total_risk_exposure_usd, highest_risk_category,
regulator_reference_number, key_obligations (list), material_findings (list), sign_off_date.
```

Look at the two fields that appear in two documents, and ask Bob which document each value came from.

#### A.5 Write the Summary and the Filled Form

```
Based on the extracted data, write a 3-sentence compliance executive summary
covering total risk exposure, the highest risk category, and the critical obligations.
Then create a new DoclingDocument with the filled form as a structured table
and export it to Markdown.
```

**If time permits:** save the same document as DocLang and open it in the [DocLang Viewer](https://doclang.ai/viewer/):

```
Save the filled form DoclingDocument as a DocLang archive,
output/compliance_assessment_form_bob.dclx.
```

> When done, proceed to [Section 5 (Compare)](#5-compare-the-two-routes).

---

### Track B — Document Intelligence with Python

In this track **IBM Bob is your IDE**: you use its editor, its file Explorer and its terminal to run the lab's notebook, as you would in any other IDE. You do not need Bob's AI assistant here. Nothing runs a model on the VM: Docling converts on your trial instance, and watsonx.ai serves the embedding model and the language model.

#### B.1 Open the Notebook in Bob

1. Open a terminal inside Bob (**Terminal → New Terminal**). It opens in the lab directory. Activate the environment the bootstrap created and add the package that lets the IDE run notebook cells in it:

   ```bash
   source .venv/bin/activate
   uv pip install ipykernel
   ```

2. In the **Explorer**, open `docs/docling_lab.ipynb`.
3. Click **Select Kernel** and choose the lab's `.venv`.
4. Run the two **Setup** cells. The first installs the notebook's packages (thin clients only — no ML framework, no model weights); the second finds the lab folder, loads `.env` (and `~/lab-1499-watsonx.txt` for any watsonx.ai value `.env` is missing), and names any missing variable.

#### B.2 Work Through the Notebook

Run the sections in order:

| Section | What you do |
|---------|------------|
| 1 | Connect to the managed service (`DoclingServiceClient`) |
| 2 | Convert PDF, DOCX, and EPUB with `client.convert_all()` |
| 3 | Inspect the document structure (sections, tables, figures) |
| 4 | Export all three documents to DocLang (`output/*.dclx`) and query them with `dclq` |
| 5 | Chunk the documents with `HybridChunker`, sized for the watsonx.ai embedding model |
| 6 | RAG pipeline: watsonx.ai embeddings → Milvus Lite → watsonx.ai LLM, with sources for every answer |
| 7 | Extract the eight fields into a Pydantic `ComplianceForm` with `with_structured_output` |
| 8 | Write the executive summary and the HTML report (`output/compliance_assessment_form_filled.html`) |
| 9 | *(optional)* Fill the form with `DoclingEditingAgent` on watsonx.ai |

> [!NOTE]
> Every Docling chunk keeps the page and bounding box it came from — the basis for **visual grounding** in a RAG application. Building that is out of scope here.
>
> Docling for IBM watsonx will soon support schema-based extraction as a **native** function of the service: send the schema with the documents and get the typed fields back.

Key `dclq` commands from section 4, which also work in Bob's terminal:

```bash
dclq inspect  output/vantara_risk_disclosure.dclx   # inventory
dclq outline  output/vantara_risk_disclosure.dclx   # heading tree + XPaths
dclq grep -i  'risk exposure' output/vantara_risk_disclosure.dclx
dclq outline  output/q3_audit_summary.dclx          # a DOCX, same kind of tree
```

**If time permits:** section 9 saves the empty and the agent-filled form as `output/compliance_assessment_form_empty.dclx` and `output/compliance_assessment_form_agent_filled.dclx`. Drag both onto the [DocLang Viewer](https://doclang.ai/viewer/) and compare them.

> When done, proceed to [Section 5 (Compare)](#5-compare-the-two-routes).

---

## 5. Compare the Two Routes
### *(5 minutes — everyone)*

Both tracks filled the same eight fields from the same three documents. Compare with a neighbour who took the other track.

| | Track A — IBM Bob | Track B — Python notebook |
|---|---|---|
| **Where the form is defined** | in your prompt, as a list of field names | in code, as a Pydantic model with types and descriptions |
| **What you can check** | the trace: which skill, which command, which file | every cell: the input, the prompt, the schema, the output |
| **Running it again** | ask again — the answer may be worded differently | run the cells again — same code, same schema |
| **What it is good for** | exploring a bundle you have not seen, quickly | a pipeline you run every quarter |

Two questions to answer:

- **Did the two fields that appear twice agree?** `total_risk_exposure_usd` and `regulator_reference_number` are each in two documents. Did your route tell you that, or did it pick one silently?
- **Where did the structure matter?** Both routes read a table cell as a table cell. Find one field that would have been a guess if the table had collapsed into text.

### ✅ What you achieved

You processed three documents in three different formats, extracted eight structured fields without manual copy-paste, generated an executive summary, and produced an audit-ready form — all powered by Docling for IBM watsonx.

---

## 6. Continue Learning

The lab materials remain available after the event:
**<https://github.com/IBM/txc2026-docling>** → folder `lab-1499/`

The Docling for IBM watsonx **free trial** stays active — continue at:
**<https://www.ibm.com/products/docling>**

### Resources

| Resource | URL |
|----------|-----|
| Docling for IBM watsonx | <https://www.ibm.com/products/docling> |
| Free trial sign-up | <https://ibm.biz/docling-for-watsonx-trial-txc26> |
| Docling documentation | <https://docling-project.github.io/docling/> |
| Managed service docs | <https://developer.dcls.saas.ibm.com/> |
| Agent skills docs | <https://docling-project.github.io/docling/usage/agent_skills/> |
| Docling MCP | <https://github.com/docling-project/docling-mcp> |
| Docling Agent | <https://github.com/docling-project/docling-agent> |
| Visual grounding example | <https://docling-project.github.io/docling/examples/visual_grounding/> |
| DocLang Viewer | <https://doclang.ai/viewer/> |
| `dclq` CLI | <https://github.com/docling-project/docling-core/tree/main/packages/dclq> |
| Docling GitHub | <https://github.com/docling-project/docling> |
| Docling Discord | <https://docling.ai/discord> |

### Quick reference — service client

```python
from docling.service_client import DoclingServiceClient

client = DoclingServiceClient(url=..., api_key=...)

result = client.convert(source="report.pdf")          # single document
print(result.document.export_to_markdown())

for r in client.convert_all(source=["a.pdf", "b.docx"], max_concurrency=4):
    print(r.input.file.name, r.status)                # many documents
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
| `DOCLING_SERVICE_URL not set` / `✗ Missing credentials` | `.env` not filled in | `cp .env.example .env` and fill in your credentials |
| Notebook setup cell says *Set these variables* for `WX_` | The shared watsonx.ai file is not on the VM | Check that `~/lab-1499-watsonx.txt` exists; if not, ask the instructor |
| Notebook cannot import `docling` | Wrong kernel, or setup skipped | **Select Kernel** → `.venv`, then run the **Setup** cells |
| Conversion returns empty Markdown | Scanned PDF, no text layer | Enable **OCR** in the Workbench options |
| `dclq` command not found | Environment not active | `source .venv/bin/activate`, or `uv pip install dclq` |
| MCP server not responding | Server not started | Re-run the `uvx --from docling-mcp ...` command; check port 8000 is free |
| watsonx.ai returns 401 | Shared key revoked or expired | Ask the instructor |
