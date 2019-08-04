#
# Copyright (c) 2019, Jaehyeuk Oh, Hyperconnect
#
# All rights reserved.
#

import json
import sys
import argparse

from configs.pypipeline_config import TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
from tweepy import StreamListener, OAuthHandler, Stream

from google.cloud.pubsub import types
from google.cloud import pubsub


PROJECT_ID = 'hpcnt-practice'
PUBSUB_TOPIC = 'hpcnt-tutorial'


class PubSubListener(StreamListener):

    def __init__(self):
        super(StreamListener, self).__init__()

        import json
        from google.auth import jwt

        service_account_info = json.load(open("configs/hpcnt-practice.json"))
        publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        credentials = jwt.Credentials.from_service_account_info(
            service_account_info, audience=publisher_audience
        )
        self.publisher = pubsub.PublisherClient(
            batch_settings=types.BatchSettings(max_messages=500),
            credentials = credentials
        )

    def on_data(self, data):
        tweet = json.loads(data)
        # sys.stdout.write(str(tweet))

        # Only publish original tweets
        if 'extended_tweet' in tweet:
            sys.stdout.write('+')
            self.publisher.publish('projects/{}/topics/{}'.format(
                PROJECT_ID,
                PUBSUB_TOPIC
                ),
                data.encode('utf-8')
            )
        else:
            sys.stdout.write('-')
            self.publisher.publish('projects/{}/topics/{}'.format(
                PROJECT_ID,
                PUBSUB_TOPIC
                ),
                data.encode('utf-8')
            )

        sys.stdout.flush()
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--twitter-topics',
                      dest='twitter_topics',
                      default='bitcoin',
                      help='A comma separated list of topics to track')

    known_args, _ = parser.parse_known_args(sys.argv)

    #This handles Twitter authetification and the connection to Twitter Streaming API
    auth = OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
    stream = Stream(auth=auth, listener=PubSubListener(), tweet_mode='extended')

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=known_args.twitter_topics.split(','), languages=['en'])
