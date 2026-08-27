# assets

Logos and conference branding, in one place so the dashboard, the lab guide and
the architecture diagrams all draw from the same files. Nothing here is
generated at run time and nothing here needs the network: the two generators
that produced them are how the files got here, not a step anyone has to
run before using them.

    logos/   the products the lab is built from
    brand/   IBM TechXchange 2026 conference identity

## Which file to use

Every logo comes in two treatments, and the choice is not taste — each of these
brand guides specifies a reversed version for dark backgrounds:

| system     | on a light page          | on the black header             |
|------------|--------------------------|---------------------------------|
| IBM Cloud  | `ibm-cloud.png`          | `ibm-cloud-white.png`           |
| Docling    | `docling-banner.svg`     | `docling-banner-white.svg`      |
| Confluent  | `confluent.svg`          | `confluent-white.svg`           |
| Apache Flink | —                      | `flink-white.svg`               |
| OpenSearch | `opensearch.svg`         | `opensearch-white.svg`          |
| IBM        | `ibm.svg`                | `ibm-white.svg`                 |
| TechXchange| `brand/techxchange-black.png` | `brand/techxchange-white.png` |

`docling.svg` / `docling.png` are the mark on its own, for a place too narrow
for the name — a diagram node, a favicon. Everywhere else use the lockup.

`brand/techxchange-geometry.svg` is the template's shape vocabulary as a
two-row block, on a transparent background. It is an accent, not a picture:
anchor it to an edge and let it crop, the way the hero slides do. Never put it
behind type.

The palette, from the template's own theme (IBM Carbon):

| role                | value     |
|---------------------|-----------|
| hero background     | `#000000` |
| interactive / links | `#0F62FE` |
| gradient, coral end | `#FA4D56` |
| gradient, middle    | `#EE5396` |
| gradient, purple end| `#8A3FFC` |
| outline marks       | `#FF8389` |

Typeface: IBM Plex Sans. Used when the machine has it and quietly skipped when
it does not — nothing here fetches a webfont, and the one place the type is
load-bearing (Docling's wordmark) is drawn as outlines rather than as text.

## Where they came from

| file(s) | source |
|---|---|
| `docling.svg`, `docling.png` | [`docling-project/docling`](https://github.com/docling-project/docling/tree/main/docs/assets) |
| `docling-banner*.svg` | assembled — see below |
| `confluent.svg` | Confluent's own CDN, via confluent.io |
| `flink-white.svg` | the [Apache Flink logo sheet](https://flink.apache.org/img/logo/svg/white_filled.svg), cropped by viewBox to the horizontal lockup |
| `opensearch.svg` | the [OpenSearch brand kit](https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg) |
| `ibm.svg`, `ibm-cloud.png` | Wikimedia Commons — IBM does not publish these for download |
| `brand/techxchange-white.png` | the IBM TechXchange 2026 speaker template (`ppt/media/image1.png`), cropped to its ink |
| `brand/techxchange-black.png` | the LAB-2775 lab-guide template |
| `brand/techxchange-geometry.svg` | drawn here, from the template's hero slides |
| `*-white.*`, `*-black.*` | recolours of the single-colour originals, by the scripts |

`fetch-logos.sh` re-downloads the first group and re-derives the recolours. It
lives with the workshop's own tooling rather than here, because refreshing a
logo is a maintenance job and the results are committed.

`make-docling-banner.py`, beside it, builds `docling-banner*.svg` — the one file
Docling does not publish: the project ships a colour mark and a white-on-black
banner, and the lab needs the colour mark with the name beside it. The script
takes the placement from the published banner and sets the name in IBM Plex
Sans, converted to outlines. It needs the font and `fonttools`, which is why it
is separate from `fetch-logos.sh` — the output is committed and regenerating it
is rare.

## Trademarks

IBM, the IBM logo, IBM Cloud and IBM TechXchange are trademarks of
International Business Machines Corporation. Confluent is a trademark of
Confluent, Inc. Apache, Apache Flink and Apache Kafka are trademarks of the
Apache Software Foundation. OpenSearch is a trademark of the OpenSearch
Software Foundation. Docling is a project of the Linux Foundation's LF AI &
Data. These marks are used here to identify the systems the lab runs on; the
logos are reproduced unmodified except for the reversed recolours described
above, which each brand's own guidelines provide for.
