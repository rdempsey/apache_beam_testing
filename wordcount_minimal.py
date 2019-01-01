#!/usr/bin/env python

"""
A minimalist word-counting workflow that counts words in a file.

Usage:
python wordcount_minimal.py
--input PATH_TO_INPUT_FILE
--output PATH_TO_OUTPUT_FILE
"""

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
    """Define and run the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='data/input/scalable_sentiment_classification_for_big_data.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='data/output/word_count/word_count',
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DirectRunner',
        '--job_name=my-wordcount-job',
    ])

    # Create the pipeline options
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection
        lines = p | ReadFromText(known_args.input)

        # Count the occurrences of each word
        counts = (
            lines
            | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                          .with_output_types(unicode))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)

        output = counts | 'Format' >> beam.Map(format_result)

        # Write the output string using a "Write" transform that has side effects.
        output | WriteToText(known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
