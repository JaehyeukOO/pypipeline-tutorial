#
# Copyright (c) 2019, Jaehyeuk Oh, Hyperconnect
#
# All rights reserved.
#

from __future__ import absolute_import

import logging
import json

import apache_beam as beam
from apache_beam.transforms.core import DoFn
from apache_beam.transforms import userstate
from apache_beam.coders import StrUtf8Coder, VarIntCoder
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.core import _ReiterableChain
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    pipeline_options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
    p = beam.Pipeline(options=pipeline_options)

    # read
    topic_path = "projects/hpcnt-practice/topics/hpcnt-tutorial"
    lines = p | 'read' >> beam.io.ReadFromPubSub(topic=topic_path)

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

    # split words
    def find_words(element):
        import re
        return re.findall(r'[#@]+[A-Za-z\']+', element.get('text'))

    words = (formatted | 'split' >> (beam.FlatMap(find_words)))

    class StatefulBufferingFn(DoFn):
        BUFFER_STATE = BagStateSpec('buffer', StrUtf8Coder())
        COUNT_STATE = userstate.CombiningValueStateSpec('count', VarIntCoder(), CountCombineFn())

        def process(self, element,
                    buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
                    count_state=beam.DoFn.StateParam(COUNT_STATE)):

            key, value = element
            try:
                index_value = list(buffer_state.read()).index(value)
            except:
                index_value = -1
            if index_value < 0:
                buffer_state.add(value)
                index_value = count_state.read()
                count_state.add(1)

            # print(value, list(buffer_state.read()).index(value), list(buffer_state.read()))
            yield ('{}_{}'.format(value, index_value), 1)


    indexed = (words | 'convert to KV' >> beam.Map(lambda x: ('common key', x))
                     | 'set index' >> (beam.ParDo(StatefulBufferingFn())))

    # count words
    def count_ones(word_ones):
        (word, ones) = word_ones
        return word, sum(ones)

    counts = (indexed
                | 'windowed' >> beam.WindowInto(beam.window.FixedWindows(5))
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
        try:
            out = sorted(sum(values, []), key=lambda x: int(x[0].split('_')[1]), reverse=False)
            logging.info(out)
        except Exception:
            logging.info(values)
    aggred_list | 'out' >> beam.Map(sorted_out)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
