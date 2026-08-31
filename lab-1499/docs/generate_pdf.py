"""
Generate vantara_risk_disclosure.pdf — Quarterly Risk Disclosure Statement
for the fictional "Vantara Bank" (replaces "Meridian Financial Group").

Includes:
  • A simple SVG-derived logo drawn with ReportLab primitives
  • Two data charts (grouped bar, horizontal bar) using ReportLab/Matplotlib
  • All original tables and text, company name replaced throughout

Run:
    uv run --no-project --python 3.12 \
        --with reportlab --with matplotlib \
        generate_pdf.py
"""

import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, HRFlowable, KeepTogether, ListFlowable, ListItem
)
from reportlab.platypus.flowables import LIIndenter
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import Flowable
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Circle, Polygon
from reportlab.graphics import renderPDF
import os

# ── Brand colours ────────────────────────────────────────────────────────────
BRAND_DARK   = colors.HexColor("#1A3A5C")   # deep navy
BRAND_MID    = colors.HexColor("#2E6DA4")   # mid blue
BRAND_ACCENT = colors.HexColor("#F5A623")   # amber
BRAND_LIGHT  = colors.HexColor("#EAF2FB")   # pale blue tint
BLACK        = colors.black
WHITE        = colors.white
GREY         = colors.HexColor("#555555")
LIGHT_GREY   = colors.HexColor("#CCCCCC")

BANK_NAME   = "Vantara Bank"
BANK_SHORT  = "the Bank"
BANK_ABR    = "VB"
BANK_ADDR   = "One Vantara Plaza, Chicago, IL 60601"
BANK_WEB    = "www.vantarabank.example.com"
BANK_PHONE  = "+1 (312) 555-0190"
BANK_LEI    = "9X4TBM7QR2KS8PH6YN03"
DOC_ID      = "VB-RD-Q3-2026-001"
REF_NO      = "FRB-2026-0847"

OUT_FILE = os.path.join(os.path.dirname(__file__), "vantara_risk_disclosure.pdf")

# ── Logo Flowable ─────────────────────────────────────────────────────────────
class BankLogo(Flowable):
    """Draw a simple geometric bank logo using ReportLab shapes."""
    def __init__(self, width=110, height=38):
        Flowable.__init__(self)
        self.width  = width
        self.height = height

    def draw(self):
        c = self.canv
        w, h = self.width, self.height

        # Shield background
        shield_w, shield_h = 32, 34
        sx, sy = 0, h - shield_h - 2
        c.setFillColor(BRAND_DARK)
        c.setStrokeColor(BRAND_DARK)
        path = c.beginPath()
        path.moveTo(sx + shield_w / 2, sy + shield_h)          # top-centre
        path.lineTo(sx + shield_w, sy + shield_h * 0.65)        # top-right
        path.lineTo(sx + shield_w, sy + shield_h * 0.30)        # mid-right
        path.lineTo(sx + shield_w / 2, sy)                      # bottom-centre
        path.lineTo(sx, sy + shield_h * 0.30)                   # mid-left
        path.lineTo(sx, sy + shield_h * 0.65)                   # top-left
        path.close()
        c.drawPath(path, fill=1, stroke=0)

        # "V" letter inside shield in amber
        c.setFillColor(BRAND_ACCENT)
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(sx + shield_w / 2, sy + shield_h * 0.25, "V")

        # Bank name text
        c.setFillColor(BRAND_DARK)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(shield_w + 6, sy + shield_h * 0.55, BANK_NAME)

        # Tagline
        c.setFillColor(GREY)
        c.setFont("Helvetica", 7)
        c.drawString(shield_w + 6, sy + shield_h * 0.30, "Strength. Clarity. Trust.")


# ── Matplotlib chart helpers ──────────────────────────────────────────────────

def fig_to_image(fig, width_pt=400, height_pt=200):
    """Return a ReportLab Image built from a Matplotlib figure."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return Image(buf, width=width_pt, height=height_pt)


def chart_rwa_by_category():
    """Grouped bar chart: RWA by risk category Q2 vs Q3 2026."""
    categories = ["Credit Risk", "Market Risk", "Operational\nRisk",
                  "Liquidity Risk", "Counterparty\nRisk"]
    q2 = [2591, 987, 609, 262, 86]
    q3 = [2748, 1054, 621, 284, 113]

    x = np.arange(len(categories))
    bar_w = 0.35

    fig, ax = plt.subplots(figsize=(7, 3.2))
    bars_q2 = ax.bar(x - bar_w / 2, q2, bar_w, label="Q2 2026",
                     color="#2E6DA4", alpha=0.85)
    bars_q3 = ax.bar(x + bar_w / 2, q3, bar_w, label="Q3 2026",
                     color="#F5A623", alpha=0.9)

    ax.set_title("Risk-Weighted Assets by Category (USD M)", fontsize=10, pad=8)
    ax.set_ylabel("USD Millions", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=7.5)
    ax.legend(fontsize=8)
    ax.yaxis.set_tick_params(labelsize=8)
    ax.set_ylim(0, 3200)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Value labels on Q3 bars
    for bar in bars_q3:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 25,
                f"{int(bar.get_height())}", ha="center", va="bottom", fontsize=6.5)

    fig.tight_layout()
    return fig_to_image(fig, width_pt=430, height_pt=200)


def chart_credit_sub_portfolio():
    """Horizontal bar chart: credit sub-portfolio exposure."""
    labels  = ["Residential\nMortgage", "Commercial\nReal Estate",
               "Corporate\nLending", "Leveraged\nLoans", "Consumer\nCredit"]
    values  = [841, 712, 634, 321, 240]
    npl     = [0.9, 3.2, 2.6, 4.1, 1.4]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7, 3.0))

    # Left: exposure
    y = np.arange(len(labels))
    bars = ax1.barh(y, values, color="#1A3A5C", alpha=0.85)
    ax1.set_yticks(y)
    ax1.set_yticklabels(labels, fontsize=7.5)
    ax1.set_xlabel("Exposure (USD M)", fontsize=8)
    ax1.set_title("Credit Sub-portfolio Exposure", fontsize=9)
    ax1.set_xlim(0, 1050)
    ax1.grid(axis="x", linestyle="--", alpha=0.4)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    for bar, val in zip(bars, values):
        ax1.text(val + 10, bar.get_y() + bar.get_height() / 2,
                 f"{val}", va="center", fontsize=7)

    # Right: NPL ratio
    colours = ["#2E6DA4" if n < 2.0 else "#F5A623" if n < 3.5 else "#C0392B"
               for n in npl]
    bars2 = ax2.barh(y, npl, color=colours, alpha=0.9)
    ax2.set_yticks(y)
    ax2.set_yticklabels(labels, fontsize=7.5)
    ax2.set_xlabel("NPL Ratio (%)", fontsize=8)
    ax2.set_title("Non-Performing Loan Ratio (%)", fontsize=9)
    ax2.set_xlim(0, 5.5)
    ax2.axvline(x=2.4, color="grey", linestyle="--", linewidth=0.8,
                label="Peer median 2.4%")
    ax2.legend(fontsize=7)
    ax2.grid(axis="x", linestyle="--", alpha=0.4)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    for bar, val in zip(bars2, npl):
        ax2.text(val + 0.05, bar.get_y() + bar.get_height() / 2,
                 f"{val}%", va="center", fontsize=7)

    fig.tight_layout()
    return fig_to_image(fig, width_pt=430, height_pt=195)


# ── Styles ────────────────────────────────────────────────────────────────────

def make_styles():
    base = getSampleStyleSheet()

    styles = {}

    styles["body"] = ParagraphStyle(
        "body", parent=base["Normal"],
        fontSize=9.5, leading=14, spaceAfter=6, alignment=TA_JUSTIFY,
        textColor=BLACK
    )
    styles["h1"] = ParagraphStyle(
        "h1", parent=base["Heading1"],
        fontSize=13, leading=16, spaceBefore=14, spaceAfter=4,
        textColor=BRAND_DARK, fontName="Helvetica-Bold"
    )
    styles["h2"] = ParagraphStyle(
        "h2", parent=base["Heading2"],
        fontSize=10.5, leading=13, spaceBefore=10, spaceAfter=3,
        textColor=BRAND_MID, fontName="Helvetica-Bold"
    )
    styles["caption"] = ParagraphStyle(
        "caption", parent=base["Normal"],
        fontSize=7.5, leading=10, spaceAfter=4,
        textColor=GREY, alignment=TA_CENTER, fontName="Helvetica-Oblique"
    )
    styles["disclaimer"] = ParagraphStyle(
        "disclaimer", parent=base["Normal"],
        fontSize=7.5, leading=10, spaceAfter=4,
        textColor=GREY, alignment=TA_JUSTIFY
    )
    styles["footer"] = ParagraphStyle(
        "footer", parent=base["Normal"],
        fontSize=7, leading=9, textColor=GREY, alignment=TA_CENTER
    )
    styles["meta_label"] = ParagraphStyle(
        "meta_label", parent=base["Normal"],
        fontSize=8.5, leading=12, textColor=GREY, fontName="Helvetica-Bold"
    )
    styles["meta_value"] = ParagraphStyle(
        "meta_value", parent=base["Normal"],
        fontSize=8.5, leading=12, textColor=BLACK
    )
    return styles


# ── Table styles ──────────────────────────────────────────────────────────────

def hdr_table_style(col_widths=None):
    return TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0),  BRAND_DARK),
        ("TEXTCOLOR",    (0, 0), (-1, 0),  WHITE),
        ("FONTNAME",     (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, -1), 8.5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, BRAND_LIGHT]),
        ("GRID",         (0, 0), (-1, -1), 0.4, LIGHT_GREY),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",   (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ("LEFTPADDING",  (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ])


# ── Cover / metadata block ────────────────────────────────────────────────────

def cover_block(styles):
    """Title block that appears at the top of page 1."""
    elements = []

    # Logo
    elements.append(BankLogo(width=140, height=46))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=2,
                               color=BRAND_ACCENT, spaceAfter=6))

    # Document title
    title_style = ParagraphStyle(
        "title", fontSize=17, leading=21, textColor=BRAND_DARK,
        fontName="Helvetica-Bold", spaceAfter=2
    )
    elements.append(Paragraph("Quarterly Risk Disclosure Statement", title_style))
    sub_style = ParagraphStyle(
        "sub", fontSize=9, leading=12, textColor=BRAND_MID,
        fontName="Helvetica", spaceAfter=10
    )
    elements.append(Paragraph(
        "Reporting Period: Q3 2026 (1 July – 30 September 2026) &nbsp;&nbsp;|&nbsp;&nbsp; "
        f"Regulatory Reference: {REF_NO}", sub_style))

    # Metadata table — 2 columns (label | value), fits within 170 mm printable width
    meta_data = [
        ["Entity",          BANK_NAME],
        ["Registered in",   "Illinois, USA  ·  LEI: " + BANK_LEI],
        ["Regulator",       "Federal Reserve Board  ·  OCC"],
        ["Document class",  "Regulatory Disclosure — Public"],
        ["Prepared by",     "Chief Risk Officer, Risk Management Division"],
        ["Distribution",    "Federal Reserve Board, Board of Directors, Public"],
    ]
    col_w = [35*mm, 135*mm]   # total = 170 mm = A4 − 2×20 mm margins
    meta_table = Table(meta_data, colWidths=col_w)
    meta_table.setStyle(TableStyle([
        ("FONTNAME",     (0, 0), (-1, -1), "Helvetica"),
        ("FONTNAME",     (0, 0), (0, -1),  "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, -1), 8),
        ("TEXTCOLOR",    (0, 0), (0, -1),  GREY),
        ("BACKGROUND",   (0, 0), (-1, -1), BRAND_LIGHT),
        ("GRID",         (0, 0), (-1, -1), 0.3, LIGHT_GREY),
        ("TOPPADDING",   (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ("LEFTPADDING",  (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    elements.append(meta_table)
    elements.append(Spacer(1, 8))

    # Disclaimer
    disclaimer_text = (
        f"This document is prepared pursuant to Federal Reserve Regulation Q and the Basel III "
        f"Capital Adequacy Framework (Pillar 3). It contains forward-looking statements subject to "
        f"material risks and uncertainties. Past performance is not indicative of future results. "
        f"{BANK_NAME} accepts no liability for decisions taken on the basis of this disclosure alone."
    )
    elements.append(Paragraph(disclaimer_text, styles["disclaimer"]))
    elements.append(HRFlowable(width="100%", thickness=0.5,
                               color=LIGHT_GREY, spaceAfter=10))
    return elements


# ── Section 1 — Executive Summary ─────────────────────────────────────────────

def section_executive_summary(styles):
    s = styles
    elements = []
    elements.append(Paragraph("1. Executive Summary", s["h1"]))
    elements.append(Paragraph(
        f'{BANK_NAME} ("the Bank") presents its Quarterly Risk Disclosure for the period '
        f'1 July 2026 to 30 September 2026, submitted in accordance with Federal Reserve Board '
        f'Regulation Q and the requirements of the Dodd–Frank Wall Street Reform and Consumer '
        f'Protection Act. The Bank operates across retail banking, corporate lending, capital '
        f'markets, and asset management divisions in 14 jurisdictions.',
        s["body"]))
    elements.append(Paragraph(
        f'Total risk-weighted exposure for Q3 2026 stands at <b>USD 4,820 million</b>, '
        f'representing a 6.3% increase over Q2 2026 (USD 4,535 million), driven primarily by '
        f'growth in the corporate lending portfolio and elevated market volatility observed in '
        f'August 2026. The Bank\'s Common Equity Tier 1 (CET1) ratio remains above the regulatory '
        f'minimum at <b>12.4%</b>, providing a capital buffer of 440 basis points above the 8.0% '
        f'requirement.',
        s["body"]))
    elements.append(Paragraph(
        f'The highest-ranked risk category for the reporting period is <b>Credit Risk</b>, '
        f'accounting for 57% of total risk-weighted assets. The Bank has implemented targeted '
        f'remediation actions across its commercial real estate and leveraged loan sub-portfolios '
        f'in response to deteriorating credit conditions in the North American market.',
        s["body"]))
    return elements


# ── Section 2 — Risk Summary ───────────────────────────────────────────────────

def section_risk_summary(styles):
    s = styles
    elements = []
    elements.append(Paragraph("2. Risk Summary", s["h1"]))
    elements.append(Paragraph("2.1 Risk Exposure by Category — Q3 2026", s["h2"]))
    elements.append(Paragraph(
        f'The table below presents the Bank\'s risk-weighted assets (RWA) broken down by risk '
        f'category as at 30 September 2026, compared with the prior quarter. All figures are '
        f'expressed in USD millions.',
        s["body"]))
    elements.append(Paragraph(
        f'The <b>6.3% quarter-on-quarter increase</b> in total RWA reflects a confluence of '
        f'factors that emerged during the July–September 2026 period. Corporate lending volumes '
        f'expanded as several pipeline transactions closed ahead of anticipated interest rate '
        f'adjustments in Q4 2026, adding approximately USD 180 million to credit RWA. '
        f'Simultaneously, a sharp rise in implied volatility across fixed income and equity '
        f'markets during August 2026 drove mark-to-market losses in the trading book, '
        f'increasing market risk RWA by USD 67 million.',
        s["body"]))
    elements.append(Paragraph(
        f'The most notable outlier is <b>Counterparty Risk</b>, which grew by 31.4% (USD 27 '
        f'million) quarter-on-quarter. This increase is directly attributable to a single '
        f'cross-currency swap portfolio maturing on 15 October 2026; the associated credit '
        f'valuation adjustment (CVA) charge is expected to reverse in Q4 2026 once the '
        f'position matures. Excluding this item, underlying RWA growth would have been '
        f'approximately 5.7% — broadly in line with management\'s annual guidance of 5–6%.',
        s["body"]))

    # Table 1
    t1_data = [
        ["Risk Category", "RWA Q3 2026\n(USD M)", "RWA Q2 2026\n(USD M)",
         "Change (%)", "Status"],
        ["Credit Risk",       "2,748", "2,591", "+6.1%", "HIGH"],
        ["Market Risk",       "1,054",   "987", "+6.8%", "ELEVATED"],
        ["Operational Risk",    "621",   "609", "+2.0%", "MODERATE"],
        ["Liquidity Risk",      "284",   "262", "+8.4%", "MODERATE"],
        ["Counterparty Risk",   "113",    "86", "+31.4%","ELEVATED"],
        ["TOTAL",             "4,820", "4,535", "+6.3%", "—"],
    ]
    col_w = [46*mm, 28*mm, 28*mm, 22*mm, 22*mm]
    t1 = Table(t1_data, colWidths=col_w)
    t1.setStyle(hdr_table_style())
    # Highlight totals row
    t1.setStyle(TableStyle([
        ("FONTNAME",  (0, 6), (-1, 6), "Helvetica-Bold"),
        ("BACKGROUND",(0, 6), (-1, 6), BRAND_LIGHT),
    ]))
    # Status colours
    status_map = {2: colors.HexColor("#C0392B"),   # HIGH — red
                  3: colors.HexColor("#E67E22"),   # ELEVATED — orange
                  4: colors.HexColor("#27AE60"),   # MODERATE — green
                  5: colors.HexColor("#E67E22")}   # ELEVATED
    for row_idx, c in status_map.items():
        t1.setStyle(TableStyle([("TEXTCOLOR", (4, row_idx), (4, row_idx), c),
                                ("FONTNAME",  (4, row_idx), (4, row_idx), "Helvetica-Bold")]))
    elements.append(KeepTogether([
        t1,
        Paragraph(
            f"Source: {BANK_NAME} Risk Management Division. Figures may not sum due to rounding.",
            s["caption"]),
    ]))

    # Chart 1 — RWA by category
    elements.append(Spacer(1, 6))
    elements.append(chart_rwa_by_category())
    elements.append(Paragraph(
        "Figure 1 — Risk-weighted assets by category, Q2 vs Q3 2026 (USD millions)",
        s["caption"]))

    elements.append(Paragraph("2.2 Credit Risk — Sub-portfolio Detail", s["h2"]))
    elements.append(Paragraph(
        f'Credit risk remains the dominant risk driver for the Bank. The increase in Q3 reflects '
        f'net loan growth of USD 312 million in the commercial real estate (CRE) sub-portfolio '
        f'and USD 94 million in leveraged lending. The non-performing loan (NPL) ratio increased '
        f'from 1.8% to 2.1%, remaining below the peer-group median of 2.4%.',
        s["body"]))
    elements.append(Paragraph(
        f'The two sub-portfolios of greatest concern are <b>Commercial Real Estate</b>, where '
        f'rising office vacancies in Chicago and New York are depressing collateral valuations, '
        f'and <b>Leveraged Loans</b>, where two media-sector borrowers were reclassified as '
        f'non-performing during Q3, contributing USD 18 million to the NPL stock.',
        s["body"]))

    # Table 2
    t2_data = [
        ["Sub-portfolio", "Exposure (USD M)", "NPL Ratio (%)", "Provision Coverage (%)"],
        ["Residential Mortgage",   "841",  "0.9%", "145%"],
        ["Commercial Real Estate", "712",  "3.2%", "112%"],
        ["Corporate Lending",      "634",  "2.6%", "118%"],
        ["Leveraged Loans",        "321",  "4.1%",  "98%"],
        ["Consumer Credit",        "240",  "1.4%", "132%"],
        ["Total Credit Risk",    "2,748",  "2.1%", "119%"],
    ]
    col_w2 = [52*mm, 36*mm, 32*mm, 36*mm]
    t2 = Table(t2_data, colWidths=col_w2)
    t2.setStyle(hdr_table_style())
    t2.setStyle(TableStyle([
        ("FONTNAME", (0, 6), (-1, 6), "Helvetica-Bold"),
        ("BACKGROUND",(0, 6), (-1, 6), BRAND_LIGHT),
    ]))
    elements.append(t2)

    # Chart 2 — credit sub-portfolios
    elements.append(Spacer(1, 8))
    elements.append(chart_credit_sub_portfolio())
    elements.append(Paragraph(
        "Figure 2 — Credit sub-portfolio exposure (left) and NPL ratios (right). "
        "Dashed line indicates peer-group median NPL of 2.4%.",
        s["caption"]))

    elements.append(Paragraph("2.3 Market Risk — Methodology and VaR Estimates", s["h2"]))
    elements.append(Paragraph(
        f'The Bank measures market risk using a <b>parametric Value-at-Risk (VaR)</b> model '
        f'calibrated to a <b>99% confidence level</b> over a <b>10-business-day holding period</b>, '
        f'consistent with Basel III internal models approach requirements. The model is applied '
        f'to the trading book, covering interest rate risk, foreign exchange risk, equity risk, '
        f'and commodity risk sub-portfolios.',
        s["body"]))
    elements.append(Paragraph(
        f'For Q3 2026, the 10-day 99% VaR across the consolidated trading book stood at '
        f'<b>USD 38.4 million</b> (Q2 2026: USD 35.1 million), reflecting the increase in '
        f'interest rate volatility observed during August 2026. The interest rate sub-portfolio '
        f'contributed the largest share at 61% of total VaR, driven by duration exposure in '
        f'the sovereign bond inventory.',
        s["body"]))
    elements.append(Paragraph(
        f'Back-testing conducted over the 250 most recent trading days produced <b>2 exceptions</b> '
        f'(days on which actual trading losses exceeded the VaR estimate), within the Basel III '
        f'green zone threshold of 4 exceptions. The Bank has not breached the amber zone '
        f'(5–9 exceptions) during the current regulatory assessment cycle.',
        s["body"]))
    elements.append(Paragraph(
        f'The Q3 2026 consolidated 10-day 99% VaR of <b>USD 38.4 million</b> (Q2 2026: '
        f'USD 35.1 million, +9.4%) reflects broad-based volatility increases across all '
        f'sub-portfolios. The breakdown by risk type and key drivers is as follows:',
        s["body"]))

    # Nested bullet list.
    # Gap control: bullet drawn at x = leftIndent - bulletDedent.
    # bulletDedent='auto' sets it equal to leftIndent, making gap = leftIndent.
    # Fix: bulletDedent = leftIndent - GAP gives the desired constant gap.
    #
    # To suppress the spurious outer bullet on nested ListFlowable children,
    # we use a NestedListFlowable subclass that nulls out the bullet on any
    # LIIndenter whose wrapped flowable is itself a ListFlowable.
    GAP      = 9    # bullet-char → text gap (points), same at both depths
    L1       = 14   # depth-1 text start (from page margin)
    L2       = 28   # depth-2 text start (absolute)
    L1_BSIZE = 10   # larger • for depth-1
    L2_BSIZE = 9    # en-dash for depth-2

    class NestedListFlowable(ListFlowable):
        """ListFlowable that suppresses bullet markers on direct ListFlowable children."""
        def _getContent(self):
            content = super()._getContent()
            for item in content:
                if isinstance(item, LIIndenter) and isinstance(item._flowable, ListFlowable):
                    item._bullet = None
            return content

    li  = ParagraphStyle("li",  parent=s["body"], spaceAfter=2, spaceBefore=1)
    li2 = ParagraphStyle("li2", parent=s["body"], fontSize=8.5, leading=12,
                         spaceAfter=1, spaceBefore=0, textColor=GREY)

    def sublist(items):
        return ListFlowable(
            [ListItem(Paragraph(t, li2), bulletColor=BRAND_MID)
             for t in items],
            bulletType="bullet", start="\u2013",   # – en-dash
            bulletFontSize=L2_BSIZE,
            leftIndent=L2, bulletDedent=L2 - GAP,
            spaceBefore=0, spaceAfter=2,
        )

    var_list = NestedListFlowable([
        # ── Interest Rate Risk ──
        ListItem(Paragraph(
            '<b>Interest Rate Risk — USD 23.4 M</b> (+8.3% QoQ, 61% of total VaR).', li)),
        sublist([
            "Duration exposure in the sovereign bond inventory (+USD 1.5 M).",
            "Widening of swap spreads in the 5–10 year tenor bucket (+USD 0.3 M).",
        ]),
        # ── Foreign Exchange Risk ──
        ListItem(Paragraph(
            '<b>Foreign Exchange Risk — USD 7.8 M</b> (+8.3% QoQ, 20% of total VaR).', li)),
        sublist([
            "Elevated EUR/USD and GBP/USD implied volatility through August 2026.",
            "Short sterling position in the UK retail banking book contributed USD 0.4 M.",
        ]),
        # ── Equity Risk (no sub-items) ──
        ListItem(Paragraph(
            '<b>Equity Risk — USD 5.1 M</b> (+4.1% QoQ). Variance driven by '
            'sector-rotation in the North American equity derivatives book.', li)),
        # ── Commodity Risk ──
        ListItem(Paragraph(
            '<b>Commodity Risk — USD 2.1 M</b> (+50.0% QoQ). Largest relative '
            'increase across sub-portfolios.', li)),
        sublist([
            "New energy-sector structured product (notional USD 18 M) added in July 2026.",
            "Crude oil price volatility index (OVX) averaged 42 in Q3 vs 31 in Q2 2026.",
        ]),
    ], bulletType="bullet", start="\u2022",
       bulletFontSize=L1_BSIZE,
       leftIndent=L1, bulletDedent=L1 - GAP,
       spaceBefore=4, spaceAfter=6)

    elements.append(var_list)

    return elements


# ── Section 3 — Capital Adequacy ───────────────────────────────────────────────

def section_capital(styles):
    s = styles
    elements = []
    elements.append(Paragraph("3. Capital Adequacy", s["h1"]))
    elements.append(Paragraph("3.1 Capital Ratios", s["h2"]))
    elements.append(Paragraph(
        f'The Bank maintains capital ratios in excess of all regulatory minimum requirements. '
        f'The CET1 ratio of 12.4% provides a buffer of 440 basis points above the 8.0% minimum '
        f'requirement and 220 basis points above the 10.2% Supervisory Capital Assessment '
        f'threshold communicated by the Federal Reserve Board on 14 March 2026 '
        f'(Reference: {REF_NO}).',
        s["body"]))

    t3_data = [
        ["Capital Measure", "Q3 2026", "Q2 2026", "Regulatory Minimum", "Buffer (bps)"],
        ["Common Equity Tier 1 (CET1)", "12.4%", "12.1%", "8.0%",  "+440"],
        ["Tier 1 Capital Ratio",        "13.8%", "13.5%", "9.5%",  "+430"],
        ["Total Capital Ratio",         "15.2%", "14.9%", "11.5%", "+370"],
        ["Leverage Ratio (Basel III)",   "5.8%",  "5.7%",  "3.0%", "+280"],
    ]
    col_w3 = [62*mm, 22*mm, 22*mm, 28*mm, 22*mm]
    t3 = Table(t3_data, colWidths=col_w3)
    t3.setStyle(hdr_table_style())
    elements.append(t3)

    elements.append(Paragraph("3.2 Stress Testing and Capital Planning", s["h2"]))
    elements.append(Paragraph(
        f'In accordance with the Dodd–Frank Act stress testing requirements, the Bank completed '
        f'its annual Comprehensive Capital Analysis and Review (CCAR) submission in June 2026 '
        f'(FRB approval received June 2026). Stress scenarios are defined by the Federal Reserve '
        f'Board and cover a <b>severely adverse</b> macroeconomic scenario and a <b>moderately '
        f'adverse</b> scenario over a nine-quarter projection horizon.',
        s["body"]))
    elements.append(Paragraph(
        f'The table below summarises projected capital ratios under each stress scenario at the '
        f'end of the nine-quarter horizon, compared with the Bank\'s starting capital position '
        f'as at 30 September 2026.',
        s["body"]))

    ts_data = [
        ["Capital Measure",               "Actual\nQ3 2026",
         "Moderately\nAdverse (min)",     "Severely\nAdverse (min)",
         "Regulatory\nMinimum"],
        ["Common Equity Tier 1 (CET1)",   "12.4%",  "10.1%",  "8.6%",  "8.0%"],
        ["Tier 1 Capital Ratio",          "13.8%",  "11.4%",  "9.9%",  "9.5%"],
        ["Total Capital Ratio",           "15.2%",  "12.8%", "11.2%", "11.5%"],
        ["Leverage Ratio (Basel III)",     "5.8%",   "4.9%",  "4.2%",  "3.0%"],
    ]
    col_ws = [52*mm, 22*mm, 28*mm, 28*mm, 24*mm]
    ts = Table(ts_data, colWidths=col_ws)
    ts.setStyle(hdr_table_style())
    # Flag Total Capital Ratio severely adverse — below minimum
    ts.setStyle(TableStyle([
        ("TEXTCOLOR", (3, 3), (3, 3), colors.HexColor("#C0392B")),
        ("FONTNAME",  (3, 3), (3, 3), "Helvetica-Bold"),
    ]))
    elements.append(ts)
    elements.append(Paragraph(
        "Note: The Total Capital Ratio under the severely adverse scenario (11.2%) falls "
        "below the 11.5% regulatory minimum. The Bank has submitted a capital plan to the "
        "Federal Reserve Board detailing remediation actions, including targeted de-risking "
        "of the leveraged loan portfolio and suspension of discretionary distributions.",
        s["caption"]))
    elements.append(Paragraph(
        f'Management is confident that the planned remediation measures are sufficient to '
        f'restore compliance with the Total Capital Ratio minimum within two quarters of the '
        f'stress scenario onset. The CET1 and Tier 1 ratios remain above their respective '
        f'regulatory minimums under all stress scenarios.',
        s["body"]))

    return elements


# ── Section 4 — Regulatory Compliance ─────────────────────────────────────────

def section_compliance(styles):
    s = styles
    elements = []
    elements.append(Paragraph("4. Regulatory Compliance Status", s["h1"]))
    elements.append(Paragraph(
        f'The Bank is subject to oversight by the Federal Reserve Board, the Office of the '
        f'Comptroller of the Currency (OCC), and the Financial Industry Regulatory Authority '
        f'(FINRA). The following table summarises compliance status across applicable regulatory '
        f'frameworks as at 30 September 2026.',
        s["body"]))

    t4_data = [
        ["Framework / Regulation",        "Applicability",    "Status",       "Notes"],
        ["Basel III Capital Framework",   "Group-wide",       "Compliant",    "CET1 12.4% vs 8.0% min"],
        ["Dodd-Frank Act (Title I/II)",   "Group-wide",       "Compliant",    "Annual stress test submitted"],
        ["Regulation Q (Capital Rules)",  "Group-wide",       "Compliant",    f"Ref: {REF_NO}"],
        ["Volcker Rule (Regulation VV)",  "Trading book",     "Compliant",    "Quarterly attestation filed"],
        ["CRA (12 CFR Part 25)",          "Retail banking",   "Satisfactory", "Last exam: Feb 2026"],
        ["AML / BSA (31 CFR Part 103)",   "All entities",     "Compliant",    "Enhanced monitoring active"],
        ["CCAR (Comprehensive Capital)",  "Holding company",  "Compliant",    "FRB approval received Jun 2026"],
    ]
    col_w4 = [55*mm, 30*mm, 25*mm, 46*mm]
    t4 = Table(t4_data, colWidths=col_w4)
    t4.setStyle(hdr_table_style())
    # Colour status column
    for row in range(1, len(t4_data)):
        status = t4_data[row][2]
        c = colors.HexColor("#27AE60") if status in ("Compliant", "Satisfactory") \
            else colors.HexColor("#C0392B")
        t4.setStyle(TableStyle([
            ("TEXTCOLOR", (2, row), (2, row), c),
            ("FONTNAME",  (2, row), (2, row), "Helvetica-Bold"),
        ]))
    elements.append(t4)
    return elements


# ── Section 6 — Concentration Risk Analysis ──────────────────────────────────

def section_concentration(styles):
    s = styles
    elements = []
    elements.append(Paragraph("5. Concentration Risk Analysis", s["h1"]))
    elements.append(Paragraph(
        f'Concentration risk arises when exposures to a single counterparty, industry sector, '
        f'or geographic region represent a disproportionate share of the Bank\'s total portfolio. '
        f'The Bank monitors concentration risk through a combination of single-name limits, '
        f'sector caps, and geographic diversification targets, all governed by the Group '
        f'Credit Risk Policy (last reviewed: April 2026).',
        s["body"]))

    elements.append(Paragraph("5.1 Sector Concentration", s["h2"]))
    elements.append(Paragraph(
        f'The table below presents the top industry sector exposures as a percentage of '
        f'total credit risk-weighted assets (RWA) as at 30 September 2026, alongside the '
        f'Bank\'s internal sector cap and the headroom remaining before the cap is breached.',
        s["body"]))

    tsc_data = [
        ["Industry Sector",        "Exposure\n(USD M)",
         "% of Credit RWA",        "Internal Sector\nCap (%)",
         "Headroom\n(pp)"],
        ["Commercial Real Estate",  "712",  "25.9%",  "30%",  "4.1"],
        ["Corporate & Leveraged",   "955",  "34.8%",  "35%",  "0.2"],
        ["Residential Mortgage",    "841",  "30.6%",  "35%",  "4.4"],
        ["Consumer & Retail",       "240",   "8.7%",  "15%",  "6.3"],
        ["Other Sectors",            "—",    "—",      "—",    "—"],
    ]
    col_wsc = [52*mm, 26*mm, 26*mm, 30*mm, 22*mm]
    tsc = Table(tsc_data, colWidths=col_wsc)
    tsc.setStyle(hdr_table_style())
    # Highlight near-cap row (Corporate & Leveraged — headroom 0.2 pp)
    tsc.setStyle(TableStyle([
        ("TEXTCOLOR", (4, 2), (4, 2), colors.HexColor("#E67E22")),
        ("FONTNAME",  (4, 2), (4, 2), "Helvetica-Bold"),
    ]))
    elements.append(tsc)
    elements.append(Paragraph(
        f"Source: {BANK_NAME} Credit Risk Division. pp = percentage points of headroom "
        f"to internal sector cap. Corporate & Leveraged headroom is flagged amber (< 1 pp).",
        s["caption"]))
    elements.append(Paragraph(
        f'The Corporate & Leveraged sector exposure at 34.8% of credit RWA is approaching '
        f'the internal cap of 35%. The Bank has implemented a <b>temporary origination pause</b> '
        f'on new leveraged lending commitments above USD 50 million, effective 1 October 2026, '
        f'pending a formal cap review by the Group Risk Committee in November 2026.',
        s["body"]))

    elements.append(Paragraph("5.2 Geographic Concentration", s["h2"]))
    elements.append(Paragraph(
        f'The Bank\'s credit portfolio spans 14 jurisdictions. The five largest geographic '
        f'exposures account for 87% of total credit RWA. The table below details these '
        f'exposures, including the sovereign credit rating of each jurisdiction as assessed '
        f'by Moody\'s as at the reporting date.',
        s["body"]))

    tgc_data = [
        ["Jurisdiction",      "Exposure\n(USD M)", "% of Credit RWA",
         "Sovereign Rating\n(Moody's)", "Change vs Q2 2026"],
        ["United States",     "1,624",  "59.1%",  "Aaa",      "—"],
        ["United Kingdom",      "342",  "12.5%",  "Aa3",  "+0.8 pp"],
        ["Germany",             "218",   "7.9%",  "Aaa",  "−0.3 pp"],
        ["Canada",              "176",   "6.4%",  "Aaa",  "+0.2 pp"],
        ["France",              "131",   "4.8%",  "Aa2",  "−0.1 pp"],
        ["Other (9 countries)", "257",   "9.3%",  "Various",  "—"],
        ["Total",             "2,748", "100.0%",  "—",    "+6.1%"],
    ]
    col_wgc = [38*mm, 26*mm, 28*mm, 34*mm, 34*mm]   # total = 160 mm; last col widened for header
    tgc = Table(tgc_data, colWidths=col_wgc)
    tgc.setStyle(hdr_table_style())
    tgc.setStyle(TableStyle([
        ("FONTNAME",   (0, 7), (-1, 7), "Helvetica-Bold"),
        ("BACKGROUND", (0, 7), (-1, 7), BRAND_LIGHT),
    ]))
    elements.append(tgc)
    elements.append(Paragraph(
        f"Source: {BANK_NAME} Credit Risk Division. pp = percentage points. "
        f"Sovereign ratings as at 30 September 2026 per Moody's Investors Service.",
        s["caption"]))
    elements.append(Paragraph(
        f'Geographic diversification remains a core risk management objective. The Bank\'s '
        f'single-country limit is set at 65% of credit RWA; the United States exposure at '
        f'59.1% provides 5.9 percentage points of headroom. No jurisdiction outside the '
        f'United States exceeds the 15% individual country sub-limit.',
        s["body"]))

    return elements


# ── Section 5 — Forward-Looking Statements ────────────────────────────────────

def section_outlook(styles):
    s = styles
    elements = []
    elements.append(Paragraph("6. Forward-Looking Statements and Outlook", s["h1"]))
    elements.append(Paragraph(
        f'The Bank anticipates continued moderate growth in risk-weighted assets through Q4 2026, '
        f'driven by seasonal increases in consumer credit and corporate lending activity. '
        f'Management expects the CET1 ratio to remain above 12.0% through year-end, supported '
        f'by disciplined capital allocation and the suspension of share buybacks announced on '
        f'18 September 2026.',
        s["body"]))
    elements.append(Paragraph(
        f'The elevated counterparty risk exposure (+31.4% QoQ) reflects increased derivative '
        f'activity in Q3 and is expected to normalise in Q4 following the maturity of a USD 27 '
        f'million cross-currency swap portfolio on 15 October 2026. The Bank has no material '
        f'direct exposure to the commercial property markets in the Asia-Pacific region.',
        s["body"]))
    elements.append(Paragraph(
        f'This disclosure contains forward-looking statements within the meaning of the Private '
        f'Securities Litigation Reform Act of 1995. Actual results may differ materially from '
        f'those projected.',
        s["body"]))
    return elements


# ── Page template (header/footer) ─────────────────────────────────────────────

PAGE_W, PAGE_H = A4

def on_page(canvas, doc):
    canvas.saveState()
    # Header rule
    canvas.setStrokeColor(BRAND_DARK)
    canvas.setLineWidth(1.5)
    canvas.line(20*mm, PAGE_H - 14*mm, PAGE_W - 20*mm, PAGE_H - 14*mm)
    # Header text (skip page 1 — cover block handles it)
    if doc.page > 1:
        canvas.setFont("Helvetica-Bold", 7.5)
        canvas.setFillColor(BRAND_DARK)
        canvas.drawString(20*mm, PAGE_H - 11*mm, BANK_NAME)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(GREY)
        canvas.drawRightString(PAGE_W - 20*mm, PAGE_H - 11*mm,
                               "Quarterly Risk Disclosure — Q3 2026  |  CONFIDENTIAL: Public Disclosure")
    # Footer rule
    canvas.setStrokeColor(LIGHT_GREY)
    canvas.setLineWidth(0.5)
    canvas.line(20*mm, 14*mm, PAGE_W - 20*mm, 14*mm)
    # Footer text
    canvas.setFont("Helvetica", 6.5)
    canvas.setFillColor(GREY)
    footer = (f"{BANK_NAME}  ·  {BANK_ADDR}  ·  {BANK_WEB}  ·  {BANK_PHONE}  "
              f"·  Doc ID: {DOC_ID}  ·  Page {doc.page}")
    canvas.drawCentredString(PAGE_W / 2, 9*mm, footer)
    canvas.restoreState()


# ── Main ──────────────────────────────────────────────────────────────────────

def build_pdf():
    doc = SimpleDocTemplate(
        OUT_FILE,
        pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=20*mm, bottomMargin=22*mm,
        title=f"{BANK_NAME} — Quarterly Risk Disclosure Q3 2026",
        author=BANK_NAME,
        subject="Regulatory Risk Disclosure",
    )

    styles = make_styles()
    story  = []

    story += cover_block(styles)
    story += section_executive_summary(styles)
    story.append(Spacer(1, 4))
    story += section_risk_summary(styles)
    story.append(Spacer(1, 4))
    story += section_capital(styles)
    story.append(Spacer(1, 4))
    story += section_compliance(styles)
    story.append(Spacer(1, 4))
    story += section_concentration(styles)
    story.append(Spacer(1, 4))
    story += section_outlook(styles)

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    print(f"✓ PDF written to: {OUT_FILE}")


if __name__ == "__main__":
    build_pdf()
