"""
Author: Anshul Pardhi
Scrapper (Kafka Producer) which continuously extracts tweets with a given hashtag
"""
from kafka import KafkaProducer
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "n6d7hyc6mNR6vcPsHqsLC6gCV"
consumer_secret = "mVve8URjCgeqD29tUnfGHfuovWyZIH26JKF4eIKIb9NkeuzEof"
access_token = "2800332947-jt17YAXNXTmiZjcRMCdtaWbZ5WkHVB4wvZitkkQ"
access_secret = "dr0JnsEcxISr8doTxpUSHd2qv1gVasce4uPp9OpXASCSl"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("twitter_coronavirus", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has trump and coronavirus hashtag (Tweets)
twitter_stream.filter(track=['#coronavirus'])
