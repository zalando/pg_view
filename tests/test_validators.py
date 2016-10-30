import sys
from unittest import TestCase

import os

path = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, path)

from pg_view.validators import output_method_is_valid


class ValidatorTest(TestCase):
    def test_output_method_is_valid_should_return_true_when_valid(self):
        ALLOWED_OUTPUTS = ['console', 'json', 'curses']
        for output in ALLOWED_OUTPUTS:
            self.assertTrue(output_method_is_valid(output))

    def test_output_method_is_valid_should_return_false_when_invalid(self):
        ALLOWED_OUTPUTS = ['test', 'foo', 1]
        for output in ALLOWED_OUTPUTS:
            self.assertFalse(output_method_is_valid(output))
