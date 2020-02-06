from tweepy import Stream
from tweepy import OAuthHandler
import json
import sys
import socket
import signal
import requests
import requests_oauthlib
from requests_oauthlib import OAuth1Session
from tweepy.streaming import StreamListener


consumer_key = 'S9mscvjwxdzHmmiFZmbFDWW4O'
consumer_secret = 'sExiybIvG9tfcvLeOKmre7t4qUMZqOyOJ0QycsMIYNHGGlwf2K'
access_token_key = '1212621264461824001-0VT9WpXEnoENyoGIZLQhszhAMBt6ek'
access_token_secret = 'DPnwrlpJ1ITJLLOXnM2UUpmH6An1T1MMr7Bv2BeKZEuiy'


class listener(StreamListener):

    def on_data(self, data):
        raw_data = json.loads(data)
        #print(raw_data)
        return True

    def on_error(self, status_code):
        print(status_code)

# if __name__ == '__main__':
#     auth = OAuthHandler(consumer_key, consumer_secret)
#     auth.set_access_token(access_token_key,access_token_secret)
#     twitterStream = Stream(auth, listener())
#     twitterStream.filter(track=["#BTS"])

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('track', 'BTS')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    twitter = OAuth1Session(consumer_key, client_secret=consumer_secret,
                            resource_owner_key=access_token_key,
                            resource_owner_secret=access_token_secret)
    response = twitter.get(query_url,stream=True)
    print(query_url, response)
    for line in response.iter_lines():
        try:
            tweet = json.loads(line)
            tweet_text = tweet['text']
            # print('tweet ' + tweet_text)
            # print("--------------------")
            client_socket.sendall(tweet_text.encode() + '\n'.encode('ascii'))
            data = client_socket.recv(1024)
            print(data.decode())

        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)



TCP_IP = 'localhost'
TCP_PORT = 9999
conn = None
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((TCP_IP,TCP_PORT))
get_tweets()




