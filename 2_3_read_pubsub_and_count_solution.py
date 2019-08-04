#
# Copyright (c) 2019, Jaehyeuk Oh, Hyperconnect
#
# All rights reserved.
#

from __future__ import absolute_import

import logging
import json

import apache_beam as beam
from apache_beam.transforms.core import _ReiterableChain
from apache_beam.options.pipeline_options import PipelineOptions


def run():
    pipeline_options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
    p = beam.Pipeline(options=pipeline_options)

    # read
    topic_path = "projects/hpcnt-practice/topics/hpcnt-tutorial-file"
    lines = (p | 'read' >> beam.io.ReadFromPubSub(topic=topic_path, with_attributes=True))

    # format message
    def format_message(message, timestamp=beam.DoFn.TimestampParam):
        message = json.loads(message.data)
        formatted_message = {
            'data': message.get('data'),
            'timestamp': float(message.get('event_time'))
        }
        return formatted_message

    formatted = lines | beam.Map(format_message)
    windowed = formatted | beam.WindowInto(beam.window.FixedWindows(5))

    # split words
    def find_words(element):
        import re
        return re.findall(r'[A-Za-z\']+', element.get('data'))

    words = (windowed | 'split' >> (beam.FlatMap(find_words)))

    # count words
    def count_ones(word_ones):
        (word, ones) = word_ones
        return word, sum(ones)

    counts = (words
                | 'pair' >> beam.Map(lambda x: (x, 1))
                | 'group' >> beam.GroupByKey()
                | 'count' >> beam.Map(count_ones))

    # aggr to list
    def aggr_to_list(values):
        try:
            if not values:
                return values
            elif isinstance(values, _ReiterableChain):
                return [x for x in values]
            elif len(values) == 1:
                return values[0]
            else:
                if isinstance(values[0], list):
                    return values[0] + [values[1]]
                else:
                    return [x for x in values]
        except Exception:
            print(values)
            pass

    aggred_list = counts | 'sort' >> beam.CombineGlobally(aggr_to_list).without_defaults()

    # out
    def sorted_out(values):
        out = sorted(sum(values, []), key=lambda x: x[1], reverse=True)
        logging.info(out)
    aggred_list | 'out' >> beam.Map(sorted_out)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()