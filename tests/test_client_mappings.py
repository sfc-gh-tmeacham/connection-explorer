"""Tests for components.client_mappings — abbreviation, hue, and icon generation."""

from components.client_mappings import (
    _abbreviation,
    _name_to_hue,
    generate_client_icon_uri,
    CLIENT_ICON_ABBREVS,
)


class TestAbbreviation:
    """Tests for _abbreviation()."""

    def test_known_mapping(self):
        """CLIENT_ICON_ABBREVS entries are returned verbatim."""
        for name, expected in list(CLIENT_ICON_ABBREVS.items())[:5]:
            assert _abbreviation(name) == expected

    def test_auto_generates_from_uppercase(self):
        """Names with internal capitals produce first + next uppercase."""
        assert _abbreviation("FooBar") == "FB"

    def test_auto_generates_with_digit(self):
        """Digits count as split points."""
        assert _abbreviation("Abc3xyz") == "A3"

    def test_short_name_in_dict(self):
        """'Go' is in CLIENT_ICON_ABBREVS and returned verbatim."""
        assert _abbreviation("Go") == "Go"

    def test_short_name_not_in_dict(self):
        """Names with <= 2 cleaned chars (not in dict) return uppercased."""
        assert _abbreviation("xy") == "XY"

    def test_no_uppercase_fallback(self):
        """All-lowercase names use first char uppercased + second char lowered."""
        assert _abbreviation("foobar") == "Fo"

    def test_slash_stripped(self):
        """Slashes are removed before abbreviation."""
        result = _abbreviation("A/B")
        assert result == "AB"


class TestNameToHue:
    """Tests for _name_to_hue()."""

    def test_returns_int_in_range(self):
        assert 0 <= _name_to_hue("Tableau") < 360

    def test_deterministic(self):
        """Same name always produces the same hue."""
        assert _name_to_hue("Power BI") == _name_to_hue("Power BI")

    def test_different_names_differ(self):
        """Different names should (almost certainly) produce different hues."""
        assert _name_to_hue("Tableau") != _name_to_hue("dbt")


class TestGenerateClientIconUri:
    """Tests for generate_client_icon_uri()."""

    def test_returns_data_uri(self):
        uri = generate_client_icon_uri("Tableau")
        assert uri.startswith("data:image/svg+xml;base64,")

    def test_fallback_icon_for_unknown(self):
        """Unknown names still produce a valid data URI (letter fallback)."""
        uri = generate_client_icon_uri("SomeUnknownTool12345")
        assert uri.startswith("data:image/svg+xml;base64,")

    def test_deterministic(self):
        """Same name produces the same URI."""
        a = generate_client_icon_uri("dbt")
        generate_client_icon_uri.cache_clear()
        b = generate_client_icon_uri("dbt")
        assert a == b
