from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import twitter_credentials
import numpy as np
import pandas as pd
import re
import os

# # # # TWITTER CLIENT # # # #
class TwitterClient():

    def __init__(self, fetched_tweets_filename, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)
        self.twitter_user = twitter_user
        self.fetched_tweets_filename = fetched_tweets_filename

    def get_twitter_client_api(self):
        return self.twitter_client 

    def get_user_timeline_tweets(self, num_tweets=None):
        with open(os.path.join('./Desktop',self.fetched_tweets_filename), 'a') as tf:
            for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user, tweet_mode='extended', include_rts = False,).items(10):
                tf.write(f'{tweet.full_text}\n')

# # # # TWITTER AUTHENTICATOR # # # #
class TwitterAuthenticator():
    """
    Twitter authenticator
    """
    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth

# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Streams and processes live tweets
    """
    def __init__(self):
            self.twitter_authenticator = TwitterAuthenticator()

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # handles twitter auth and connection to twitter streaming API
        listener = TwitterListener(fetched_tweets_filename)
        auth = self.twitter_authenticator.authenticate_twitter_app()
        stream = Stream(auth, listener)

        stream.filter(track=hash_tag_list)

# # # # TWITTER LISTENER # # # #
class TwitterListener(StreamListener):
    """
    Listener that prints received tweets to stdout
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data + '\n')
            return True
        except BaseException as e:
            print("Error on data: %s" % str(e))
        return True

    def on_error(self, status):
        if status == 420:
            # returning false in event rates limit exceeded
            return False
        print(status)

# # # # TWEET ANALYZER # # # #
class TweetAnalyzer():
    """
    Functionality for analyzing and categorizing content from tweets
    """
    
    def tweets_to_data_frame(self, tweets):
        df = pd.DataFrame(data=[tweet.text for tweet in tweets], columns=['Tweets'])
        return df

if __name__ == "__main__":
    twitter_client_trump = TwitterClient("donald_trump.txt", "realDonaldTrump")
    twitter_client_trump.get_user_timeline_tweets()

    twitter_client_biden = TwitterClient("joe_biden.txt", "JoeBiden")
    twitter_client_biden.get_user_timeline_tweets()

    # df = tweet_analyzer.tweets_to_data_frame(tweets)
    # print(df.head(10))