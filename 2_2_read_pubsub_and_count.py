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


def run():
    pipeline_options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
    p = beam.Pipeline(options=pipeline_options)

    # read from pubsub
    topic_path = "projects/hpcnt-practice/topics/hpcnt-tutorial-file"
    lines = (p | 'read' >> beam.io.ReadFromPubSub(topic=topic_path)
               | 'Load into JSON' >> beam.Map(json.loads))

    # out
    lines | 'out' >> beam.Map(lambda x: logging.info(x))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
