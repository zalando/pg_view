from unittest import TestCase

import mock

from pg_view.factories import get_displayer_by_class
from pg_view.models.displayers import OUTPUT_METHOD


class GetDisplayerByClassTest(TestCase):
    def test_get_displayer_by_class_should_raise_exception_when_unknown_method(self):
        with self.assertRaises(Exception):
            get_displayer_by_class('unknown', {}, True, True, True)

    @mock.patch('pg_view.factories.JsonDisplayer.from_collector')
    def test_get_displayer_by_class_should_return_json_displayer_when_json(self, mocked_from_collector):
        collector = mock.Mock()
        get_displayer_by_class(OUTPUT_METHOD.json, collector, True, True, True)
        mocked_from_collector.assert_called_with(collector, True, True, True)

    @mock.patch('pg_view.factories.ConsoleDisplayer.from_collector')
    def test_get_displayer_by_class_should_return_console_displayer_when_console(self, mocked_from_collector):
        collector = mock.Mock()
        get_displayer_by_class(OUTPUT_METHOD.console, collector, True, True, True)
        mocked_from_collector.assert_called_with(collector, True, True, True)

    @mock.patch('pg_view.factories.CursesDisplayer.from_collector')
    def test_get_displayer_by_class_should_return_curses_displayer_when_curses(self, mocked_from_collector):
        collector = mock.Mock()
        get_displayer_by_class(OUTPUT_METHOD.curses, collector, True, True, True)
        mocked_from_collector.assert_called_with(collector, True, True, True)
