#!/usr/bin/env python

"""
Test for the minimal wordcount example
"""

from __future__ import absolute_import

import logging
import tempfile
import unittest

import hello_beam


class WordCountMinimalTest(unittest.TestCase):
    """Unit test for wordcount_minimal example with direct runner."""

    SAMPLE_TEXT = 'This is a nice little piece of text I have here.'

    @staticmethod
    def create_temp_file(contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents)
            return f.name

    def test_basics(self):
        temp_path = self.create_temp_file(self.SAMPLE_TEXT)
        hello_beam.run([
            '--input=%s*' % temp_path,
            '--output=%s.result' % temp_path])
        self.assertIsNotNone(temp_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
