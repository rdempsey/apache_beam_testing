#!/usr/bin/env python

"""
hello_beam.py

Sample file for running a very simple pipeline in Apache Beam locally.
Takes a file as input and saves a file as output.

Usage:
python hello_beam.py
--input PATH_TO_INPUT_FILE
--output PATH_TO_OUTPUT_FILE
"""

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='data/input/us-500.csv',
        help='Input file to process')
    parser.add_argument(
        '--output',
        dest='output',
        default='data/output/hello_beam/hello_beam',
        help='Output local filename')

    args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = False

    p = beam.Pipeline(options=options)
    (p | 'Read from Input File' >> beam.io.ReadFromText(args.input)
       | 'Write to file' >> beam.io.WriteToText(args.output)
     )
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
