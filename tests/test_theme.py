"""Tests for components.theme — hex-to-RGB and relative luminance."""

from components.theme import _hex_to_rgb, _relative_luminance


class TestHexToRgb:
    def test_valid_hex(self):
        assert _hex_to_rgb("#29B5E8") == (0x29, 0xB5, 0xE8)

    def test_no_hash(self):
        assert _hex_to_rgb("29B5E8") == (0x29, 0xB5, 0xE8)

    def test_black(self):
        assert _hex_to_rgb("#000000") == (0, 0, 0)

    def test_white(self):
        assert _hex_to_rgb("#FFFFFF") == (255, 255, 255)

    def test_short_hex_returns_none(self):
        assert _hex_to_rgb("#FFF") is None

    def test_invalid_chars_returns_none(self):
        assert _hex_to_rgb("#ZZZZZZ") is None

    def test_empty_returns_none(self):
        assert _hex_to_rgb("") is None

    def test_none_returns_none(self):
        assert _hex_to_rgb(None) is None


class TestRelativeLuminance:
    def test_white(self):
        assert _relative_luminance((255, 255, 255)) == pytest.approx(1.0, abs=0.01)

    def test_black(self):
        assert _relative_luminance((0, 0, 0)) == 0.0

    def test_mid_gray(self):
        lum = _relative_luminance((128, 128, 128))
        assert 0.4 < lum < 0.6

    def test_pure_red(self):
        lum = _relative_luminance((255, 0, 0))
        assert lum == pytest.approx(0.299, abs=0.01)

    def test_pure_green(self):
        lum = _relative_luminance((0, 255, 0))
        assert lum == pytest.approx(0.587, abs=0.01)

    def test_pure_blue(self):
        lum = _relative_luminance((0, 0, 255))
        assert lum == pytest.approx(0.114, abs=0.01)


# pytest.approx needs the import
import pytest
