from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import os
import sys
access_token = "1018106408116473857-YxLM8YvFaNzIoiekGNc0FomkVGSkxA"
access_token_secret = "REB7NJlDZTVSWVwySNEDBLmuPi3tggXuKAcEfPoMtU7y7"
consumer_key = "xkJf0eu3i1sU4Wn1MziAdCmWM"
consumer_secret = "vFEMYcEfgJKlItZxEp0WtM4h77qa0ULuMwXg2dEmzvPgXTYus6"

i = 0

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        global i
        
        i = i + 1
        print(data)

        print(i)
        if i == 100000:
            sys.exit()
            
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StdOutListener())

    #This line filter Twitter Streams to capture data by the keywords: 'gadgets', 'phones'
    stream.filter(track=['apple', 'samsung','googlepixel','redmi','oneplus','gadgets'])
