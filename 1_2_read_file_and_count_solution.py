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

    # split words
    def find_words(element):
        import re
        return re.findall(r'[A-Za-z\']+', element)

    words = (lines | 'split' >> (beam.FlatMap(find_words)))

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
        if not values:
            return values
        elif len(values) == 1:
            return values[0]
        else:
            if isinstance(values[0], list):
                return values[0] + [values[1]]
            else:
                return [values[0]] + [values[1]]

    aggred_list = counts | 'sort' >> beam.CombineGlobally(aggr_to_list).without_defaults()

    # out
    aggred_list | 'out' >> beam.Map(lambda x: logging.info(sorted(x, key=lambda x: x[1], reverse=True)))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
