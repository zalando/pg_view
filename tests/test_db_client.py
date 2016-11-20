from unittest import TestCase

from pg_view.models.proc_reader import get_dbname_from_path


class DBClientTest(TestCase):
    def test_get_dbname_from_path_should_return_last_when_name(self):
        self.assertEqual('foo', get_dbname_from_path('foo'))

    def test_get_dbname_from_path_should_return_last_when_path(self):
        self.assertEqual('bar', get_dbname_from_path('/pgsql_bar/9.4/data'))
