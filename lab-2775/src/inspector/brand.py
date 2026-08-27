"""The lab's identity on screen: the TechXchange header, and the product logos.

Everything here reads from ``assets/`` at the top of the repo rather than from
files beside the dashboard, because the same logos are wanted by the lab guide
and by the architecture diagrams — one folder, one provenance, one place to fix
a logo that a vendor has changed. See ``assets/README.md``.

Images are inlined as ``data:`` URIs. Streamlit serves static files only when
the app is started with ``--server.enableStaticServing``, and asking a student
to remember a flag in order to see a logo is a bad trade; a base64 string in the
page has no such condition and the whole set is under half a megabyte.

Nothing here is load-bearing. A missing ``assets/`` folder — someone copied
``dashboard/`` on its own — degrades to text, and the dashboard still tells you
what the pipeline is doing. The one rule the helpers exist to enforce is that
the *reversed* logo goes on the black header and the default one goes on the
light page, which is what each of these brand guides asks for.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

ASSETS = Path(__file__).resolve().parents[2] / "assets"

# IBM Carbon, by way of the TechXchange 2026 template's theme: the hero is black
# with white type, the accents are the coral-magenta-purple gradient, and the
# one interactive colour is Blue 60.
BLUE_60 = "#0F62FE"
CORAL = "#FA4D56"
MAGENTA = "#EE5396"
PURPLE = "#8A3FFC"
GRADIENT = f"linear-gradient(90deg, {PURPLE} 0%, {MAGENTA} 55%, {CORAL} 100%)"
_MIME = {".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg"}


@dataclass(frozen=True)
class Logo:
    """One product logo, in the two treatments a page actually needs."""
    default: str          # for the light page
    reversed: str         # for the black header; "" when the mark works on both
    alt: str
    role: str = ""        # what this system does in *this* lab
    # Four wordmarks drawn to the same pixel height do not read as the same
    # size: Docling's lockup is a tall mark with small lettering beside it,
    # while the other three are lettering and almost nothing else. The
    # multiplier is what puts their *names* at a comparable size.
    scale: float = 1.0

    def file(self, dark: bool) -> str:
        return (self.reversed or self.default) if dark else self.default


# The four systems this lab wires together, in the order a document passes
# through them. Docling ships a mark and a black-background banner but not a
# colour lockup, so assets/make-docling-banner.py builds one from both; the
# duck stays full colour in either treatment and only the lettering reverses.
LOGOS = {
    "ibm-cloud": Logo("logos/ibm-cloud.png", "logos/ibm-cloud-white.png", "IBM Cloud",
                      "Object Storage — where a document lands"),
    "docling": Logo("logos/docling-banner.svg", "logos/docling-banner-white.svg", "Docling",
                    "converts and chunks, outside Flink", scale=1.35),
    "confluent": Logo("logos/confluent.svg", "logos/confluent-white.svg", "Confluent",
                      "Kafka and Flink — where the pipeline runs"),
    "opensearch": Logo("logos/opensearch.svg", "logos/opensearch-white.svg", "OpenSearch",
                       "the index the chunks and vectors land in"),
    "techxchange": Logo("brand/techxchange-black.png", "brand/techxchange-white.png",
                        "IBM TechXchange"),
    "ibm": Logo("logos/ibm.svg", "logos/ibm-white.svg", "IBM"),
}


@lru_cache(maxsize=64)
def data_uri(relative: str) -> str:
    """``assets/<relative>`` as a data URI, or "" when it is not there."""
    path = ASSETS / relative
    try:
        payload = base64.b64encode(path.read_bytes()).decode("ascii")
    except OSError:
        return ""
    return f"data:{_MIME.get(path.suffix, 'application/octet-stream')};base64,{payload}"


def img(name: str, height: int = 22, dark: bool = False, style: str = "") -> str:
    """One logo as an ``<img>``, sized so a row of them reads evenly."""
    logo = LOGOS.get(name)
    if logo is None:
        return ""
    uri = data_uri(logo.file(dark))
    if not uri:
        return f"<span style='font-weight:600;font-size:{height * 0.7:.0f}px'>{logo.alt}</span>"
    return (f'<img src="{uri}" alt="{logo.alt}" title="{logo.alt}" '
            f'style="height:{height * logo.scale:.0f}px;width:auto;display:block;{style}">')


def header_html(lab_id: str, title: str, subtitle: str, note: str = "") -> str:
    """The black TechXchange header: conference mark, lab number, lab title.

    Laid out the way the template's hero slide is — mark and type on the left,
    the geometric motif holding the right edge, a gradient rule closing the
    block — so that a screenshot of this dashboard in the deck does not look
    like it came from somewhere else.
    """
    logo = img("techxchange", height=26, dark=True)
    motif = data_uri("brand/techxchange-geometry.svg")
    # The motif is a *column*, never a background behind the type. Anchored
    # right and cropped by the column's own overflow, so the shapes run off the
    # edge the way they do on the template's hero slide — and so a narrow
    # window takes room away from the shapes instead of putting them under the
    # lab title.
    motif_col = (
        f'<div style="flex:0 1 340px;min-width:0;background-image:url(\'{motif}\');'
        'background-repeat:no-repeat;background-position:right center;'
        'background-size:auto 100%;"></div>'
    ) if motif else ""
    return f"""
<div style="background:#000;border-radius:6px;margin-bottom:16px;overflow:hidden;">
  <div style="display:flex;align-items:stretch;gap:20px;min-height:132px;padding-left:24px;">
    <div style="flex:1 1 auto;min-width:0;display:flex;flex-direction:column;
                justify-content:center;padding-top:20px;padding-bottom:20px;">
      <div style="display:flex;align-items:center;gap:14px;margin-bottom:14px;">
        {logo}
        <span style="color:#8d8d8d;font-size:0.72rem;letter-spacing:0.12em;
                     text-transform:uppercase;border-left:1px solid #393939;padding-left:14px;">
          {lab_id}
        </span>
      </div>
      <div style="color:#fff;font-size:2.1rem;font-weight:300;line-height:1.15;">{title}</div>
      <div style="color:#c6c6c6;font-size:0.95rem;font-weight:300;margin-top:6px;">{subtitle}</div>
      {f'<div style="color:#8d8d8d;font-size:0.8rem;margin-top:10px;">{note}</div>' if note else ''}
    </div>
    {motif_col}
  </div>
  <div style="height:4px;background:{GRADIENT};"></div>
</div>
"""


def rail_html(names: tuple[str, ...] = ("ibm-cloud", "docling", "confluent", "opensearch")) -> str:
    """The systems the lab is built from, with what each one does here.

    Under the header rather than in it: the header says which lab this is, and
    this says what the lab is made of. The order is the order a document
    travels, so the rail reads as the pipeline before the diagram draws it.
    """
    cells = []
    for name in names:
        logo = LOGOS.get(name)
        if logo is None:
            continue
        # A fixed-height box around the mark rather than around the whole cell:
        # it is what puts four logos of four different proportions on one line.
        cells.append(
            '<div style="display:flex;flex-direction:column;gap:8px;">'
            f'<div style="height:38px;display:flex;align-items:center;">{img(name, height=22)}</div>'
            f'<span style="color:#6f6f6f;font-size:0.72rem;line-height:1.3;">{logo.role}</span>'
            "</div>"
        )
    # The bottom padding is the gap between the branding and the first real
    # measurement on the page. It wants to be wide: what follows is the job's
    # state, and a role caption sitting a few pixels above a metric reads as a
    # label for it.
    return (
        '<div style="display:flex;gap:44px;flex-wrap:wrap;align-items:flex-start;'
        'padding-bottom:40px;">' + "".join(cells) + "</div>"
    )


def inline(name: str, text: str, height: int = 20) -> str:
    """A logo and a line of text on one baseline — for a section heading."""
    mark = img(name, height=height, style="margin:0;")
    return (f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:6px;">{mark}'
            f'<span style="font-size:0.85rem;color:#525252;">{text}</span></div>')
