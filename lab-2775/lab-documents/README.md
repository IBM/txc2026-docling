# Lab documents

The documents you upload to your bucket during the lab. They are fictional, and
so is every person, company, card number and bank account in them.

## The use case

**Halvorsen Fluid Systems** makes industrial pumps. Its field service team wants
an assistant that answers engineers' questions from the company's own
documents: technical notes from engineering, the maintenance manuals, and the
case files the service desk keeps for every customer call-out. Those documents
are produced by different teams and land in one bucket on their own schedule.
Nobody runs a batch job; a pipeline has to pick each one up as it arrives,
convert and chunk it, enrich and embed it, and index it for retrieval.

Three things make that harder than "convert and index":

- **Case files contain personal and payment data.** Customer e-mail addresses,
  phone numbers, the card a repair was paid with, the account a refund went to.
  None of it may reach the index the assistant searches. But the service desk
  still needs to be able to see what was there.
- **The same document is filed more than once.** A technical note is published
  by engineering and then filed again by field service as a "controlled copy":
  a different file, the same words. Indexing both makes every answer cite the
  same passage twice.
- **The rules change while the pipeline runs.** Compliance decides that phone
  numbers count as personal data after all, or that short fragments should be
  dropped. That has to take effect on the next document, not after a redeploy.

## The documents

| File | What it is | What it shows in the lab |
|---|---|---|
| `01-technical-note.pdf` | TN-2026-014, *Bearing temperature drift on the HX-400 pump series*: headings, a figure, two tables, part and serial numbers | The ordinary case. Its chunks go straight through, and it contains no personal data, so the guard finds nothing |
| `02-customer-record.pdf` | Service case SC-88213, a bearing failure at a brewery: contacts, site details, billing and a refund | E-mail addresses, a card number and an IBAN in ordinary sentences are redacted by default. The phone numbers and the IP address of the telemetry gateway are redacted only once you switch those detectors on |
| `03-technical-note-reissued.pdf` | The same note as 01, filed again by field service: US Letter instead of A4, a serif typeface, a different title | Docling converts all of it. De-duplication on the text fingerprint then drops every chunk, because the words are the same as 01's |
| `04-maintenance-manual.pdf` | OM-HX4, the series' operation and maintenance manual: specifications, alignment tolerances, maintenance schedule, fault finding, spare parts | More material for retrieval, and safe to upload before the PII guard exists. Its re-greasing interval for the HX-420 is the one the technical note later shortened |

## Questions to ask

Once 01 and 04 are indexed, the **Ask** tab can answer questions like these:

- *What is the revised trip temperature for the HX-420?* An answer from a table row.
- *Which bearing replaces BRG-6207-ZZ, and from which serial number?* Turn
  **Keyword boost** off and ask again. Part numbers embed into nothing in
  particular, and the plain vector search finds them less reliably.
- *Why does the drive-end bearing overheat on these pumps?* An answer from the
  explanation in the root-cause section, not from a single sentence.
- *How often should the HX-420 drive-end bearing be re-greased?* The manual
  says 4,000 hours and the technical note shortens it. A good answer cites both
  and says which one applies.
- *What does a new HX-460 cost?* Neither document says. The answer should say
  so rather than invent a price.

After 02 has gone through the full pipeline:

- *How can I contact the plant manager at Brauerei Sonnenberg?* The passage
  the answer is built from says `[REDACTED:EMAIL]`. Redaction happened before
  indexing, so retrieval never had the address to give away.
