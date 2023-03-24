import tweepy
import time
from time import sleep
import requests
import json
from json import dumps,loads
from kafka import KafkaProducer,KafkaConsumer

consumer=KafkaConsumer('demo1',bootstrap_servers=['MAYTINH-UHGO2M2:9092'],value_deserializer =lambda x:loads(x.decode('utf-8')))
for v in consumer:
    print(v.value)