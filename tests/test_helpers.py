from unittest import TestCase

import mock

from pg_view.helpers import UnitConverter


class UnitConverterTest(TestCase):
    def test_kb_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(3, UnitConverter.kb_to_mbytes(3072))

    def test_kb_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.kb_to_mbytes(None))

    def test_sectors_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(10, UnitConverter.sectors_to_mbytes(20480))

    def test_sectors_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.sectors_to_mbytes(None))

    def test_bytes_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(2, UnitConverter.bytes_to_mbytes(2097152))

    def test_bytes_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.bytes_to_mbytes(None))

    @mock.patch('pg_view.helpers.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_convert_when_ok(self):
        self.assertEqual(5, UnitConverter.ticks_to_seconds(500))

    @mock.patch('pg_view.helpers.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.ticks_to_seconds(None))

    def test_time_diff_to_percent_should_convert_when_ok(self):
        self.assertEqual(1000.0, UnitConverter.time_diff_to_percent(10))

    def test_time_diff_to_percent_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.time_diff_to_percent(None))
