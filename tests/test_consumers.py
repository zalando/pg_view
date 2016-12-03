from unittest import TestCase

import mock

from pg_view.models.consumers import DiskCollectorConsumer


class DiskCollectorConsumerTest(TestCase):
    def test_consume_should_not_get_new_data_from_queue_when_old_not_consumed(self):
        queue = mock.Mock()
        first_data = {'/var/lib/postgresql/9.3/main': [{
            'xlog': ('16388', '/var/lib/postgresql/9.3/main/pg_xlog'),
            'data': ('35620', '/var/lib/postgresql/9.3/main')
        }, {
            'xlog': ('/dev/sda1', 41251136, 37716376),
            'data': ('/dev/sda1', 41251136, 37716376)}
        ]}

        queue.get_nowait.return_value = first_data

        consumer = DiskCollectorConsumer(queue)
        consumer.consume()
        self.assertEqual(first_data, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

        self.assertIsNone(consumer.consume())
        self.assertEqual(first_data, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

    def test_consume_should_consume_new_data_when_old_fetched(self):
        queue = mock.Mock()
        first_data = {'/var/lib/postgresql/9.3/main': [{
            'xlog': ('16388', '/var/lib/postgresql/9.3/main/pg_xlog'),
            'data': ('35620', '/var/lib/postgresql/9.3/main')
        }, {
            'xlog': ('/dev/sda1', 41251136, 37716376),
            'data': ('/dev/sda1', 41251136, 37716376)}
        ]}

        second_data = {'/var/lib/postgresql/9.3/main': [{
            'xlog': ('16389', '/var/lib/postgresql/9.3/main/pg_xlog'),
            'data': ('35621', '/var/lib/postgresql/9.3/main')
        }, {
            'xlog': ('/dev/sda1', 41251137, 37716377),
            'data': ('/dev/sda1', 41251137, 37716377)}
        ]}

        queue.get_nowait.side_effect = [first_data.copy(), second_data.copy()]

        consumer = DiskCollectorConsumer(queue)
        consumer.consume()
        self.assertEqual(first_data, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

        self.assertEqual(first_data['/var/lib/postgresql/9.3/main'], consumer.fetch('/var/lib/postgresql/9.3/main'))
        self.assertEqual({}, consumer.result)
        self.assertEqual(first_data, consumer.cached_result)

        consumer.consume()
        self.assertEqual(second_data, consumer.result)
        self.assertEqual(second_data, consumer.cached_result)

        self.assertEqual(second_data['/var/lib/postgresql/9.3/main'], consumer.fetch('/var/lib/postgresql/9.3/main'))
        self.assertEqual({}, consumer.result)
        self.assertEqual(second_data, consumer.cached_result)
