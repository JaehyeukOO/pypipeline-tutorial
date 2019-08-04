#
# Copyright (c) 2019, Jaehyeuk Oh, Hyperconnect
#
# All rights reserved.
#

from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.io import ReadFromText

from apache_beam.options.pipeline_options import PipelineOptions


def run():
    pipeline_options = PipelineOptions(["--runner=DirectRunner"])

    p = beam.Pipeline(options=pipeline_options)

    # read file
    text_filename = "resources/in.txt"
    lines = p | 'read' >> ReadFromText(text_filename)

    # out
    lines | 'out' >> beam.Map(lambda x: logging.info(x))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
