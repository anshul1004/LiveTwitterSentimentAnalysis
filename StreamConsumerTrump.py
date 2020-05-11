"""
Author: Anshul Pardhi
Spark streaming using Kafka consumer which continuously collects filtered tweets,
performs sentiment analysis and sends the results to elastic search
"""
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer

es = Elasticsearch()


def main():
    """
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    """
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter_trump")
    for msg in consumer:

        dict_data = json.loads(msg.value)

        # Create a blob
        tweet_blob = TextBlob(dict_data["text"])
        tweet = dict_data["text"]

        print(tweet_blob)  # Print the tweet text

        # Calculate blob sentiment
        print(format(tweet_blob.sentiment))
        polarity = tweet_blob.sentiment.polarity
        if polarity > 0:
            blob_sentiment = "Positive"
        elif polarity < 0:
            blob_sentiment = "Negative"
        else:
            blob_sentiment = "Neutral"

        # Calculate vader sentiment
        sid = SentimentIntensityAnalyzer()
        ss = sid.polarity_scores(tweet)
        for k in sorted(ss):
            print('{0}: {1}, '.format(k, ss[k]), end='')
            if k == 'compound':
                if ss[k] >= 0.05:
                    vader_sentiment = "Positive"
                elif ss[k] <= -0.05:
                    vader_sentiment = "Negative"
                else:
                    vader_sentiment = "Neutral"

        print()
        print("Blob Sentiment: ", blob_sentiment)
        print("Vader Sentiment: ", vader_sentiment)
        print()

        # add text and sentiment info to elasticsearch
        es.index(index="tweet_trump",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "blob_sentiment": blob_sentiment,
                       "vader_sentiment": vader_sentiment})
        print('\n')


if __name__ == "__main__":
    main()
