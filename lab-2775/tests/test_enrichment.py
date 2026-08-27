"""Unit tests for pipeline.logic.enrichment — normalization and the derived fields."""

from pipeline.logic.chunk_record import build
from pipeline.logic.enrichment import detect_script, enrich, fingerprint, normalize_text, quality_flags

BASE = build(
    doc_id="report.pdf",
    chunk_index=3,
    text="Revenue grew by 12% in 2024.\nThe board approved the dividend.",
    origin={"filename": "report.pdf", "mimetype": "application/pdf", "binary_hash": 7, "uri": None},
    headings=["Annual report", "Financials"],
    page_numbers=[4, 5],
    extra={"token_count": 17},
)


def test_enrich_counts_and_structure():
    out = enrich(BASE)
    assert out["char_count"] == len(BASE["text"])
    assert out["word_count"] == 11
    assert out["heading_path"] == "Annual report > Financials"
    assert out["section_depth"] == 2
    assert (out["page_start"], out["page_end"]) == (4, 5)
    assert out["token_count"] == 17  # producer value kept, not re-estimated
    assert "token_count_estimated" not in out
    assert out["keep"] is True and out["quality_flags"] == []


def test_enrich_does_not_mutate_input():
    original = dict(BASE)
    enrich(BASE)
    assert BASE == original


def test_token_count_is_estimated_when_missing():
    out = enrich({**BASE, "token_count": None})
    assert out["token_count_estimated"] is True
    assert out["token_count"] > 0


def test_fingerprint_ignores_case_and_whitespace():
    assert fingerprint("Hello   world\n") == fingerprint("hello world")
    assert fingerprint("hello world") != fingerprint("hello worlds")


def test_the_producers_chunk_id_survives_enrichment():
    """It is the index's document id, and the producer owns it."""
    assert enrich(BASE)["chunk_id"] == BASE["chunk_id"]


def test_enrich_detects_content_types():
    table = enrich({**BASE, "text": "| a | b |\n|---|---|\n| 1 | 2 |"})
    assert table["has_table"] is True
    assert enrich({**BASE, "text": "- one\n- two\n- three"})["has_list"] is True
    assert enrich({**BASE, "text": "The mass is $E = mc^2$ exactly."})["has_formula"] is True


def test_quality_flags_catch_junk():
    assert "too_short" in quality_flags(10, 2, {"alpha_ratio": 0.9, "symbol_ratio": 0.0})
    assert "low_alpha" in quality_flags(200, 40, {"alpha_ratio": 0.1, "symbol_ratio": 0.0})
    assert "high_symbol" in quality_flags(200, 40, {"alpha_ratio": 0.5, "symbol_ratio": 0.5})
    assert quality_flags(200, 40, {"alpha_ratio": 0.8, "symbol_ratio": 0.1}) == []


def test_short_chunk_is_not_kept():
    out = enrich({**BASE, "text": "Page 7"})
    assert out["keep"] is False and "too_short" in out["quality_flags"]


def test_detect_script():
    assert detect_script("Hello world") == "latin"
    assert detect_script("こんにちは世界") in ("hiragana", "cjk")
    assert detect_script("1234 ...") == "unknown"


# --- normalization ---------------------------------------------------------
# It runs before the fingerprint, so what it folds together is what dedup sees
# as the same chunk.


def test_dehyphenation_joins_words_split_across_lines():
    assert normalize_text("inter-\nnational") == "international"
    # A soft hyphen is the same case.
    assert normalize_text("inter\u00ad\nnational") == "international"


def test_a_hyphen_that_is_not_a_line_break_is_untouched():
    assert normalize_text("well-known") == "well-known"


def test_bare_number_line_is_treated_as_furniture():
    # Known trade-off: a standalone numeric line is almost always a page
    # number in PDF-extracted text, so it goes. A number that is part of a
    # sentence or a table row (which has other content on the line) survives.
    assert normalize_text("body\n20\nmore") == "body\nmore"
    assert normalize_text("total 20 units") == "total 20 units"
    assert normalize_text("| 20 | 30 |") == "| 20 | 30 |"


def test_ligatures_and_widths_fold_to_plain_ascii():
    assert normalize_text("\ufb01nance") == "finance"
    assert normalize_text("\uff21\uff22\uff23") == "ABC"


def test_invisible_and_control_characters_are_removed():
    assert normalize_text("real\u200btext") == "realtext"
    assert normalize_text("a\x00b") == "a b"


def test_page_furniture_lines_are_dropped():
    text = "Real content here.\n12\nPage 3\n3 of 12\n-----\nMore content."
    out = normalize_text(text)
    assert "Real content here." in out
    assert "More content." in out
    for furniture in ("12", "Page 3", "3 of 12", "-----"):
        assert f"\n{furniture}\n" not in out


def test_whitespace_collapses_but_paragraphs_survive():
    assert normalize_text("a   b c") == "a b c"
    assert normalize_text("para one\n\n\n\npara two") == "para one\n\npara two"


def test_normalize_is_idempotent():
    text = "inter-\nnational  \ufb01nance\n\n\n7\ntail"
    once = normalize_text(text)
    assert normalize_text(once) == once


def test_normalizing_empty_text_is_empty_text():
    assert normalize_text("") == ""


def test_the_fingerprint_ignores_case_and_spacing():
    """Which is why normalize_text runs first: it repairs, this only folds."""
    assert fingerprint("The  Quick brown fox") == fingerprint("the quick BROWN fox")
