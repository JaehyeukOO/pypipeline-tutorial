#
# Copyright (c) 2019, Jaehyeuk Oh, Hyperconnect
#
# All rights reserved.
#

import json

from google.auth import jwt
from google.cloud.pubsub import types
from google.cloud import pubsub

from time import sleep
from threading import Timer

import calendar
import time


PROJECT_ID = 'hpcnt-practice'
PUBSUB_TOPIC = 'hpcnt-tutorial-file'


service_account_info = json.load(open("configs/hpcnt-practice.json"))
publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=publisher_audience
)
publisher = pubsub.PublisherClient(
    batch_settings=types.BatchSettings(max_messages=500),
    credentials=credentials
)


class CircularReadFile:
    def __init__(self):
        self.open()

    def open(self):
        self.in_file = open("resources/in.txt", "r")

    def read(self):
        line = self.in_file.readline()
        if not line:
            self.open()
            return self.read()
        elif line.strip() == '':
            return self.read()
        return line.strip()


rf = CircularReadFile()


def publish_one_line():
    Timer(1.0, publish_one_line).start()
    # read file line and send to pubsub
    line = rf.read()
    data = {"event_time": calendar.timegm(time.gmtime()), "data": line}
    print(data)
    publisher.publish('projects/{}/topics/{}'.format(PROJECT_ID, PUBSUB_TOPIC), json.dumps(data).encode('utf-8'))


# set timer
publish_one_line()

while True:
    sleep(60.0)
    continue
