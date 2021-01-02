from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler

import twitter_credentials
import numpy as np
import pandas as pd
import re
import os

# # # # TWITTER CLIENT # # # #
class TwitterClient():

    def __init__(self, fetched_tweets_filename, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.twitter_user = twitter_user
        self.fetched_tweets_filename = fetched_tweets_filename

    def get_twitter_client_api(self):
        return self.twitter_client 

    def get_user_timeline_tweets(self, num_tweets=None, last_id=None):
        tweet_query = []

        with open(os.path.join('./Desktop',self.fetched_tweets_filename), 'a') as tf:
            while len(tweet_query) <= num_tweets:
                # max_tweets = num_tweets - len(tweet_query)

                for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user, tweet_mode='extended', include_rts = False, max_id=last_id).items():
                    tweet_query.append(tweet)
                    tf.write(f'{tweet.full_text}\n')

                if len(tweet_query) != 0:
                    last_id = tweet_query[-1].id - 1
                    print (last_id)
                print(len(tweet_query))

# # # # TWITTER AUTHENTICATOR # # # #
class TwitterAuthenticator():
    """
    Twitter authenticator
    """
    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth

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
    twitter_client_trump.get_user_timeline_tweets(30000, 1305960428120551423)

    # twitter_client_biden = TwitterClient("joe_biden.txt", "JoeBiden")
    # twitter_client_biden.get_user_timeline_tweets()

    # df = tweet_analyzer.tweets_to_data_frame(tweets)
    # print(df.head(10))