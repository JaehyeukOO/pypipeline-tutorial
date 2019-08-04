#
# Copyright (c) 2019, Jaehyeuk Oh, Hyperconnect
#
# All rights reserved.
#

from __future__ import absolute_import

import logging
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    pipeline_options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
    p = beam.Pipeline(options=pipeline_options)

    # read
    topic_path = "projects/hpcnt-practice/topics/hpcnt-tutorial"
    lines = (p | 'read' >> beam.io.ReadFromPubSub(topic=topic_path))

    # format message
    def format_message(message, timestamp=beam.DoFn.TimestampParam):
        message = json.loads(message)
        formatted_message = {
            'text': message.get('text'),
            'created_at': message.get('created_at'),
            'timestamp': float(timestamp)
        }
        return formatted_message

    formatted = lines | beam.Map(format_message)

    filtered = (formatted
                | 'windowed' >> beam.WindowInto(beam.window.FixedWindows(5))
                | 'filter' >> (beam.Filter(lambda data: data.get('text').lower().find('bit') >= 0)))

    filtered | 'out' >> beam.Map(lambda out: logging.info(out))

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
